package supervisor

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"sync"
	"time"

	"encoding/json"
	"github.com/golang/glog"
	"github.com/hyperhq/runv/factory"
	"github.com/opencontainers/runtime-spec/specs-go"
	"io/ioutil"
	"path/filepath"
)

type Supervisor struct {
	StateDir string
	Factory  factory.Factory
	// Default CPU and memory amounts to use when not specified by container
	defaultCpus   int
	defaultMemory int

	Events SvEvents

	sync.RWMutex // Protects Supervisor.Containers, HyperPod.Containers, HyperPod.Processes, Container.Processes
	Containers   map[string]*Container
}

func New(stateDir, eventLogDir string, f factory.Factory, defaultCpus int, defaultMemory int) (*Supervisor, error) {
	if err := os.MkdirAll(stateDir, 0755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(eventLogDir, 0755); err != nil {
		return nil, err
	}
	if defaultCpus <= 0 {
		return nil, fmt.Errorf("defaultCpu must be greater than 0.")
	}
	if defaultMemory <= 0 {
		return nil, fmt.Errorf("defaultMemory must be greater than 0.")
	}
	sv := &Supervisor{
		StateDir:      stateDir,
		Factory:       f,
		defaultCpus:   defaultCpus,
		defaultMemory: defaultMemory,
		Containers:    make(map[string]*Container),
	}

	dirEntries, err := ioutil.ReadDir(stateDir)
	if err != nil {
		return nil, fmt.Errorf("could not open stateDir: %v %v", stateDir, err)
	}

	// Reload stale containers which still have configuration. These will be
	// marked as "stopped". This allows recovery and clean up after an
	// unexpected shutdown.
	for _, e := range dirEntries {
		if !e.IsDir() {
			continue
		}
		stateFile := filepath.Join(stateDir, e.Name(), "state.json")
		if _, err := os.Stat(stateFile); os.IsNotExist(err) {
			glog.Warningf("stateDir exists but state.json does not: %v", e.Name())
			continue
		}

		data, err := ioutil.ReadFile(stateFile)
		if err != nil {
			glog.Errorf("error reading stateFile: %v %v", stateFile, err)
			continue
		}

		state := &specs.State{}
		if err := json.Unmarshal(data, &state); err != nil {
			glog.Errorf("error unmarshalling stateFile: %v %v", stateFile, err)
			continue
		}

		// TODO: it's not clear we can always recover specs this way? Should
		// they be saved somewhere else?
		spec := new(specs.Spec)
		ocfData, err := ioutil.ReadFile(filepath.Join(state.BundlePath, "config.json"))
		if err != nil {
			glog.Errorf("could not read config.json from container BundlePath: %v", err)
			// TODO: should a missing file lead to cleaning up runv-containerd state?
			continue
		}
		if err := json.Unmarshal(ocfData, spec); err != nil {
			glog.Errorf("could not unmarshal config.json from container BundlePath: %v", err)
			continue
		}

		// State is loaded. Add a stopped container (no owner pod).
		// TODO: it should be possible to read the pid from state.json and
		// find a running VM that the container might still be active in and
		// reconnect to it.
		sv.Containers[state.ID] = &Container{
			Id:         state.ID,
			BundlePath: state.BundlePath,
			Spec:       spec,
			Processes:  make(map[string]*Process),
			ownerPod:   nil,
		}
		glog.Infof("Recovered stopped container for supervisor: %v", state.ID)
	}

	sv.Events.subscribers = make(map[chan Event]struct{})
	go sv.reaper()

	// TODO: read stateDir and setup pod-less containers so we can recover
	// mountpoints on a hard kill.

	return sv, sv.Events.setupEventLog(eventLogDir)
}

func (sv *Supervisor) CreateContainer(container, bundlePath, stdin, stdout, stderr string, spec *specs.Spec) (*Container, *Process, error) {
	sv.Lock()
	defer sv.Unlock()
	hp, err := sv.getHyperPod(container, spec)
	if err != nil {
		return nil, nil, err
	}
	c, err := hp.createContainer(container, bundlePath, stdin, stdout, stderr, spec)
	if err != nil {
		return nil, nil, err
	}
	sv.Containers[container] = c
	glog.Infof("Supervisor.CreateContainer() return: c:%v p:%v", c, c.Processes["init"])
	return c, c.Processes["init"], nil
}

func (sv *Supervisor) AddProcess(container, processId, stdin, stdout, stderr string, spec *specs.Process) (*Process, error) {
	sv.Lock()
	defer sv.Unlock()
	if c, ok := sv.Containers[container]; ok {
		return c.addProcess(processId, stdin, stdout, stderr, spec)
	}
	return nil, fmt.Errorf("container %s is not found for AddProcess()", container)
}

func (sv *Supervisor) TtyResize(container, processId string, width, height int) error {
	sv.RLock()
	defer sv.RUnlock()
	p := sv.getProcess(container, processId)
	if p != nil {
		return p.ttyResize(container, width, height)
	}
	return fmt.Errorf("The container %s or the process %s is not found", container, processId)
}

func (sv *Supervisor) CloseStdin(container, processId string) error {
	sv.RLock()
	defer sv.RUnlock()
	p := sv.getProcess(container, processId)
	if p != nil {
		return p.closeStdin()
	}
	return fmt.Errorf("The container %s or the process %s is not found", container, processId)
}

func (sv *Supervisor) Signal(container, processId string, sig int) error {
	sv.RLock()
	defer sv.RUnlock()
	p := sv.getProcess(container, processId)
	if p != nil {
		return p.signal(sig)
	}
	return fmt.Errorf("The container %s or the process %s is not found", container, processId)
}

func (sv *Supervisor) getProcess(container, processId string) *Process {
	if c, ok := sv.Containers[container]; ok {
		if p, ok := c.Processes[processId]; ok {
			return p
		}
	}
	return nil
}

func (sv *Supervisor) reaper() {
	events := sv.Events.Events(time.Time{})
	for e := range events {
		if e.Type == EventExit {
			go sv.reap(e.ID, e.PID)
		}
	}
}

func (sv *Supervisor) reap(container, processId string) {
	glog.Infof("reap container %s processId %s", container, processId)
	sv.Lock()
	defer sv.Unlock()
	if c, ok := sv.Containers[container]; ok {
		if p, ok := c.Processes[processId]; ok {
			go p.reap()
			delete(c.ownerPod.Processes, processId)
			delete(c.Processes, processId)
			if p.init {
				// TODO: kill all the other existing processes in the same container
			}
			if len(c.Processes) == 0 {
				go c.reap()
				delete(c.ownerPod.Containers, container)
				delete(sv.Containers, container)
			}
			if len(c.ownerPod.Containers) == 0 {
				go c.ownerPod.reap()
			}
		}
	}
}

// find shared pod or create a new one
func (sv *Supervisor) getHyperPod(container string, spec *specs.Spec) (hp *HyperPod, err error) {
	if _, ok := sv.Containers[container]; ok {
		return nil, fmt.Errorf("The container %s is already existing", container)
	}
	if spec.Linux == nil {
		return nil, fmt.Errorf("it is not linux container config")
	}
	for _, ns := range spec.Linux.Namespaces {
		if len(ns.Path) > 0 {
			if ns.Type == "mount" {
				// TODO support it!
				return nil, fmt.Errorf("Runv doesn't support shared mount namespace currently")
			}

			pidexp := regexp.MustCompile(`/proc/(\d+)/ns/*`)
			matches := pidexp.FindStringSubmatch(ns.Path)
			if len(matches) != 2 {
				return nil, fmt.Errorf("Can't find shared container with network ns path %s", ns.Path)
			}
			pid, _ := strconv.Atoi(matches[1])

			for _, c := range sv.Containers {
				if c.ownerPod != nil && pid == c.ownerPod.getNsPid() {
					if hp != nil && hp != c.ownerPod {
						return nil, fmt.Errorf("Conflict share")
					}
					hp = c.ownerPod
					break
				}
			}
			if hp == nil {
				return nil, fmt.Errorf("Can't find shared container with network ns path %s", ns.Path)
			}
		}
	}
	if hp == nil {
		sv.Unlock()
		hp, err = createHyperPod(sv.Factory, spec, sv.defaultCpus, sv.defaultMemory)
		sv.Lock()
		glog.Infof("createHyperPod() returns")
		if err != nil {
			return nil, err
		}
		hp.sv = sv
		// recheck existed
		if _, ok := sv.Containers[container]; ok {
			go hp.reap()
			return nil, fmt.Errorf("The container %s is already existing", container)
		}
	}
	return hp, nil
}
