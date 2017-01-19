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
)

type Supervisor struct {
	StateDir string
	Factory  factory.Factory
	// Default CPU and memory amounts to use when not specified by container
	defaultCpus   int
	defaultMemory int

	Events SvEvents

	// pods tracks the pods being managed by the supervisor
	pods    []*HyperPod
	podsMtx sync.RWMutex // Protects Supervisor.pods

	// containerQueue prevents multiple containerId requests from causing
	// multiple pod startups without blocking the Supervisor
	containerQueue    map[string]struct{}
	containerQueueMtx sync.RWMutex // Protects Supervisor.containerQueue
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
		StateDir:       stateDir,
		Factory:        f,
		defaultCpus:    defaultCpus,
		defaultMemory:  defaultMemory,
		pods:           make([]*HyperPod, 0),
		containerQueue: make(map[string]struct{}),
	}
	sv.Events.subscribers = make(map[chan Event]struct{})
	go sv.reaper()
	return sv, sv.Events.setupEventLog(eventLogDir)
}

// getPod searches for the pod which contains a particular container.
func (sv *Supervisor) getPod(container string) *HyperPod {
	sv.podsMtx.RLock()
	defer sv.podsMtx.RUnlock()

	// TODO: this is a possible bottleneck, we may want to do some indexing
	for _, pod := range sv.pods {
		if c := pod.getContainer(container); c != nil {
			return pod
		}
	}

	return nil
}

func (sv *Supervisor) ListContainers() []*Container {
	sv.podsMtx.RLock()
	defer sv.podsMtx.RUnlock()

	returnedList := []*Container{}

	for _, pod := range sv.pods {
		for _, c := range pod.ListContainers() {
			returnedList = append(returnedList, c)
		}
	}

	return returnedList
}

func (sv *Supervisor) CreateContainer(container, bundlePath, stdin, stdout, stderr string, spec *specs.Spec) (*Container, *Process, error) {
	// This locking is needed to resolve the race in creating hyperpods.
	// It is made an error to send multiple CreateContainer requests with the
	// same containerId when one is already in progress.
	// TODO: this should actually block, but just not hold the lock
	sv.containerQueueMtx.Lock()
	if _, found := sv.containerQueue[container]; found {
		sv.containerQueueMtx.Unlock()
		return nil, nil, fmt.Errorf("container %s is already being created by another request.", container)
	} else {
		// Add the container to the queue
		sv.containerQueue[container] = struct{}{}
		sv.containerQueueMtx.Unlock()
	}

	hp, err := sv.getHyperPod(container, spec)
	if err != nil {
		return nil, nil, err
	}
	c, err := hp.CreateContainer(container, bundlePath, stdin, stdout, stderr, spec)
	// Unlock containerId as soon as container creation has finished, regardless
	// of success or failure.
	sv.containerQueueMtx.Lock()
	delete(sv.containerQueue, container)
	sv.containerQueueMtx.Unlock()

	if err != nil {
		return nil, nil, err
	}

	glog.Infof("Supervisor.CreateContainer() return: c:%v p:%v", c, c.getProcess("init"))

	return c, c.getProcess("init"), nil
}

func (sv *Supervisor) AddProcess(container, processId, stdin, stdout, stderr string, spec *specs.Process) (*Process, error) {
	if pod := sv.getPod(container); pod != nil {
		return pod.AddProcess(container, processId, stdin, stdout, stderr, spec)
	}
	return nil, fmt.Errorf("container %s is not found for AddProcess()", container)
}

func (sv *Supervisor) TtyResize(container, processId string, width, height int) error {
	if pod := sv.getPod(container); pod != nil {
		if c := pod.getContainer(container); c != nil {
			if p := c.getProcess(processId); p != nil {
				return p.TtyResize(container, width, height)
			}
			return fmt.Errorf("The process %s is not found in the container %s", processId, container)
		}
	}
	return fmt.Errorf("The container %s is not found", container)
}

func (sv *Supervisor) CloseStdin(container, processId string) error {
	if pod := sv.getPod(container); pod != nil {
		if c := pod.getContainer(container); c != nil {
			if p := c.getProcess(processId); p != nil {
				return p.closeStdin()
			}
			return fmt.Errorf("The process %s is not found in the container %s", processId, container)
		}
	}
	return fmt.Errorf("The container %s is not found", container)
}

func (sv *Supervisor) Signal(container, processId string, sig int) error {
	if pod := sv.getPod(container); pod != nil {
		if c := pod.getContainer(container); c != nil {
			if p := c.getProcess(processId); p != nil {
				return p.Signal(sig)
			}
			return fmt.Errorf("The process %s is not found in the container %s", processId, container)
		}
	}
	return fmt.Errorf("The container %s is not found", container)
}

func (sv *Supervisor) reaper() {
	events := sv.Events.Events(time.Time{})
	for e := range events {
		if e.Type == EventExit {
			sv.reapProcess(e.ID, e.PID)
		}
	}
}

// reapProcess reaps the given process from the given container
func (sv *Supervisor) reapProcess(container, processId string) {
	glog.Infof("reap container %s processId %s", container, processId)
	// Acquire write-lock to stop other tasks from grabbing the pod we want.
	sv.podsMtx.Lock()
	defer sv.podsMtx.Unlock()

	var targetPod *HyperPod
	podIdx := -1

	for idx, pod := range sv.pods {
		if c := pod.getContainer(container); c != nil {
			podIdx = idx
			targetPod = pod
			break
		}
	}

	if targetPod == nil {
		return
	}

	targetPod.ReapProcess(container, processId)
	// Reap the pod if it's become reapable
	if targetPod.ShouldReap() {
		targetPod.reap()
		sv.pods = append(sv.pods[:podIdx], sv.pods[podIdx+1:]...)

	}
}

// find shared pod or create a new one
func (sv *Supervisor) getHyperPod(container string, spec *specs.Spec) (*HyperPod, error) {
	// Check if any existing pod has the container
	if pod := sv.getPod(container); pod != nil {
		return nil, fmt.Errorf("The container %s is already existing", container)
	}
	if spec.Linux == nil {
		return nil, fmt.Errorf("it is not linux container config")
	}

	// This is the hyperpod which will be returned
	var hp *HyperPod

	// If asking for a namespace join, find a pod which matches all the
	// namespaces that have been asked for
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

			sv.podsMtx.RLock()
			for _, pod := range sv.pods {
				if pid == pod.getNsPid() {
					if hp != nil && hp != pod {
						sv.podsMtx.RUnlock()
						return nil, fmt.Errorf("Conflict share (cannot add a container to namespace across multiple pods)")
					}
					hp = pod
					break
				}
			}
			sv.podsMtx.RUnlock()

			if hp == nil {
				return nil, fmt.Errorf("Can't find shared container with network ns path %s", ns.Path)
			}
		}
	}

	// TODO: prevent containerIds being able to race while starting up a pod.

	// If no pod found by now, create a new pod
	if hp == nil {
		var err error
		glog.V(3).Infof("CreateHyperPod() entered")
		hp, err = CreateHyperPod(
			sv.Factory,
			spec,
			sv.defaultCpus,
			sv.defaultMemory,
			sv.StateDir,
			sv.Events.notifySubscribers,
		)
		glog.Infof("CreateHyperPod() returns")

		if err != nil {
			return nil, err
		}
	}

	if hp != nil {
		sv.podsMtx.Lock()
		sv.pods = append(sv.pods, hp)
		sv.podsMtx.Unlock()
	}

	return hp, nil
}
