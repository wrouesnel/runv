package supervisor

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/hyperhq/runv/factory"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/constabulary/gb/testdata/src/c"
)

type Supervisor struct {
	StateDir string
	Factory  factory.Factory
	// Default CPU and memory amounts to use when not specified by container
	defaultCpus   int
	defaultMemory int

	Events SvEvents

	sync.RWMutex // Protects Supervisor.Containers, // HyperPod.Containers, HyperPod.Processes, Container.Processes
	containers   map[string]*Container
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
		containers:    make(map[string]*Container),
	}
	sv.Events.subscribers = make(map[chan Event]struct{})
	go sv.reaper()
	return sv, sv.Events.setupEventLog(eventLogDir)
}

// getContainer safely searches for a container in the supervisor. Returns nil if
// the container is not found. Does not guarantee the container will not be removed
// immediately after.
func (sv *Supervisor) getContainer(container string) *Container {
	sv.RLock()
	defer sv.RUnlock()

	if c, ok := sv.containers[container]; ok {
		return c
	}

	return nil
}

// getProcess returns a process from a given container/processId if it exists. Returns nil if not found.
func (sv *Supervisor) getProcess(container, processId string) *Process {
	if c := sv.getContainer(container); c != nil {
		if p := c.getProcess(processId); p != nil {
			return p
		}
	}
	return nil
}

func (sv *Supervisor) CreateContainer(container, bundlePath, stdin, stdout, stderr string, spec *specs.Spec) (*Container, *Process, error) {
	hp, err := sv.getHyperPod(container, spec)
	if err != nil {
		return nil, nil, err
	}
	c, err := hp.createContainer(container, bundlePath, stdin, stdout, stderr, spec)
	if err != nil {
		return nil, nil, err
	}

	// Safely add the container
	sv.Lock()
	sv.containers[container] = c
	sv.Unlock()

	glog.Infof("Supervisor.CreateContainer() return: c:%v p:%v", c, c.Processes["init"])
	return c, c.Processes["init"], nil
}

func (sv *Supervisor) AddProcess(container, processId, stdin, stdout, stderr string, spec *specs.Process) (*Process, error) {
	if c := sv.getContainer(container); c != nil {
		return c.addProcess(processId, stdin, stdout, stderr, spec)
	}
	return nil, fmt.Errorf("container %s is not found for AddProcess()", container)
}

func (sv *Supervisor) TtyResize(container, processId string, width, height int) error {
	p := sv.getProcess(container, processId)
	if p != nil {
		return p.ttyResize(container, width, height)
	}
	return fmt.Errorf("The container %s or the process %s is not found", container, processId)
}

func (sv *Supervisor) CloseStdin(container, processId string) error {
	p := sv.getProcess(container, processId)
	if p != nil {
		return p.closeStdin()
	}
	return fmt.Errorf("The container %s or the process %s is not found", container, processId)
}

func (sv *Supervisor) Signal(container, processId string, sig int) error {
	p := sv.getProcess(container, processId)
	if p != nil {
		return p.signal(sig)
	}
	return fmt.Errorf("The container %s or the process %s is not found", container, processId)
}

func (sv *Supervisor) reaper() {
	events := sv.Events.Events(time.Time{})
	for e := range events {
		if e.Type == EventExit {
			go sv.reap(e.ID, e.PID)
		}
	}
}

// TODO
func (sv *Supervisor) reap(container, processId string) {
	glog.Infof("reap container %s processId %s", container, processId)
	//sv.Lock()
	//defer sv.Unlock()

	// TODO: containers should self-reap if they run out of processes (or init)
	// TODO: pod's should self-reap if they run out of containers.
	if c := sv.getContainer(container); c != nil {
		c.reapProcess(processId)
	}

	//if c, ok := sv.Containers[container]; ok {
	//	if p, ok := c.Processes[processId]; ok {
	//		go p.reap()
	//		delete(c.ownerPod.Processes, processId)
	//		delete(c.Processes, processId)
	//		if p.init {
	//			// TODO: kill all the other existing processes in the same container
	//		}
	//		if len(c.Processes) == 0 {
	//			go c.reap()
	//			delete(c.ownerPod.Containers, container)
	//			delete(sv.Containers, container)
	//		}
	//		if len(c.ownerPod.Containers) == 0 {
	//			go c.ownerPod.reap()
	//		}
	//	}
	//}
}

// find shared pod or create a new one
func (sv *Supervisor) getHyperPod(container string, spec *specs.Spec) (hp *HyperPod, err error) {
	// Lock the containers struct while we're searching it
	if c := sv.getContainer(container); c != nil {
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

			// Maybe there should be a container iterator?
			sv.Lock()
			for _, c := range sv.containers {
				if c.ownerPod != nil && pid == c.ownerPod.getNsPid() {
					if hp != nil && hp != c.ownerPod {
						sv.Unlock()
						return nil, fmt.Errorf("Conflict share")
					}
					hp = c.ownerPod
					break
				}
			}
			sv.Unlock()

			if hp == nil {
				return nil, fmt.Errorf("Can't find shared container with network ns path %s", ns.Path)
			}
		}
	}

	if err != nil {
		return hp, err
	}

	if hp == nil {
		// use 'func() + defer' to ensure we regain the lock when createHyperPod() panic.
		func() {
			sv.Unlock()
			defer sv.Lock()
			hp, err = createHyperPod(sv.Factory, spec, sv.defaultCpus, sv.defaultMemory)
		}()
		glog.Infof("createHyperPod() returns")
		if err != nil {
			return nil, err
		}
		hp.sv = sv
		// recheck existed
		if c := sv.getContainer(container); c != nil {
			if _, ok := sv.containers[container]; ok {
				go hp.reap()
			}
		}
		return hp, nil
	}
}
