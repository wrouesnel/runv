package supervisor

import (
	"fmt"
	"io"
	"os"
	"syscall"

	"github.com/golang/glog"
	"github.com/hyperhq/runv/hypervisor"
	"github.com/opencontainers/runtime-spec/specs-go"
	"sync"
)

type Process struct {
	Id     string
	Stdin  string
	Stdout string
	Stderr string
	Spec   *specs.Process
	ProcId int

	// vm holds the vm backing process that this process is talking to.
	// This is a slightly leaky abstraction, but there's no scenario where a
	// a pod's VM could be changed and container ops continue to make sense
	// without some serious clean up process-side.
	vm *hypervisor.Vm

	// innerId is Id or container id + "-init"
	// pass to hypervisor package and HyperPod.Processes
	innerId string

	init        bool
	stdio       *hypervisor.TtyIO
	stdinCloser io.Closer

	// flag channel if container is shutting down/has shutdown. channel is
	// closed when it is true.
	reaping chan bool

	sync.RWMutex
}

func (p *Process) setupIO() error {
	glog.Infof("process setupIO: stdin %s, stdout %s, stderr %s", p.Stdin, p.Stdout, p.Stderr)

	// use a new go routine to avoid deadlock when stdin is fifo
	go func() {
		if stdinCloser, err := os.OpenFile(p.Stdin, syscall.O_WRONLY, 0); err == nil {
			p.stdinCloser = stdinCloser
		}
	}()

	stdin, err := os.OpenFile(p.Stdin, syscall.O_RDONLY, 0)
	if err != nil {
		return err
	}
	stdout, err := os.OpenFile(p.Stdout, syscall.O_RDWR, 0)
	if err != nil {
		return err
	}
	stderr, err := os.OpenFile(p.Stderr, syscall.O_RDWR, 0)
	if err != nil {
		return err
	}

	p.stdio = &hypervisor.TtyIO{
		Stdin:  stdin,
		Stdout: stdout,
		Stderr: stderr,
	}
	glog.Infof("process setupIO() success")

	return nil
}

// TtyResize requests the process resize it's TTY.
func (p *Process) TtyResize(container string, width, height int) error {
	p.Lock()
	p.Unlock()
	if p.ShouldReap() {
		return fmt.Errorf("Process (%s) has already exited.", p.Id)
	}

	// If working on the primary process, do not pass execId (it won't be recognized)
	if p.innerId == fmt.Sprintf("%s-init", container) {
		return p.vm.Tty(container, "", height, width)
	}
	return p.vm.Tty(container, p.innerId, height, width)
}

func (p *Process) closeStdin() error {
	var err error
	if p.stdinCloser != nil {
		err = p.stdinCloser.Close()
		p.stdinCloser = nil
	}
	return err
}

func (p *Process) Signal(sig int) error {
	// TODO: support killing non-init in the VM
	if p.init {
		// TODO: change vm.KillContainer()
		return p.vm.KillContainer(p.Id, syscall.Signal(sig))
	}
	return fmt.Errorf("Kill to non-init process of container is unsupported")
}

func (p *Process) ShouldReap() bool {
	select {
	case <-p.reaping:
		return true
	default:
		return false
	}
}

func (p *Process) reap() {
	p.Lock()
	p.Unlock()
	close(p.reaping)

	p.closeStdin()
}
