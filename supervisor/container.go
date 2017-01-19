package supervisor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/docker/docker/pkg/mount"
	"github.com/docker/docker/pkg/symlink"
	"github.com/golang/glog"
	"github.com/hyperhq/runv/api"
	"github.com/hyperhq/runv/hypervisor"
	"github.com/hyperhq/runv/lib/utils"
	"github.com/opencontainers/runtime-spec/specs-go"
	"sync"
)

const (
	ContainerInitProcessId string = "init"
)

type Container struct {
	Id          string
	BundlePath  string
	Spec        *specs.Spec
	NamespaceId int

	// vm holds the vm backing process that this container is talking to.
	// This is a slightly leaky abstraction, but there's no scenario where a
	// a pod's VM could be changed and container ops continue to make sense
	// without some serious clean up container-side.
	vm *hypervisor.Vm

	// Subprocesses (including init) managed by this container.
	processes map[string]*Process

	// Container state directory
	stateDir string

	// notifyFn is a creator-provided function to dispatch container events
	notifyFn func(e Event)

	// flag channel if container is shutting down/has shutdown. channel is
	// closed when it is true.
	reaping chan bool

	// Protects Container.Processes, Container.reap
	sync.RWMutex
}

// NewContainer creates and initializes a new container and it's init process.
func NewContainer(containerId, bundlePath, stdin, stdout, stderr string, spec *specs.Spec, namespaceId int, vm *hypervisor.Vm, stateDir string, notifyFn func(e Event)) *Container {
	// Create the container
	c := &Container{
		Id:          containerId,
		BundlePath:  bundlePath,
		Spec:        spec,
		NamespaceId: namespaceId,
		vm:			 vm,
		processes:   make(map[string]*Process),
		stateDir:    stateDir,
		notifyFn:    notifyFn,
		reaping:     make(chan bool),
	}
	// Create the init process
	innerProcessId := containerId + "-init"
	p := &Process{
		Id:     ContainerInitProcessId,
		Stdin:  stdin,
		Stdout: stdout,
		Stderr: stderr,
		Spec:   &spec.Process,
		ProcId: namespaceId,

		vm:		 vm,
		innerId: innerProcessId,
		init:    true,
		reaping: make(chan bool),
	}
	// Setup the init process.
	if err := p.setupIO(); err != nil {
		return nil
	}

	// Add the process to the map (no locking needed because no one knows
	// about this container yet.
	c.processes[p.Id] = p

	return c
}

// notify calls notifyFn if it is set to dispatch a container event if an
// event handler is set.
func (c *Container) notify(e Event) {
	if c.notifyFn != nil {
		c.notifyFn(e)
	}
}

// getProcess locks and retrieves a process struct from the given container.
// It *does not* guarantee that the process will not be subsequently removed
// from the container.
func (c *Container) getProcess(processId string) *Process {
	c.RLock()
	defer c.RUnlock()
	// If the container is being reaped, there can be no processes.
	if c.ShouldReap() {
		return nil
	}

	if p, ok := c.processes[processId]; ok {
		return p
	}
	return nil
}

// ListProcesses returns a list of all processes in this container.
func (c *Container) ListProcesses() []*Process {
	c.RLock()
	defer c.RUnlock()

	returnedList := []*Process{}

	if c.ShouldReap() {
		return returnedList
	}

	for _, p := range c.processes {
		returnedList = append(returnedList, p)
	}

	return returnedList
}

// run launches this container. It is non-blocking once the
// pod has acknowledged the process has been successfully started (the process
// may still exit immediately via normal POSIX mechanisms).
func (c *Container) run() error {
	p := c.getProcess(ContainerInitProcessId)
	if p == nil {
		return fmt.Errorf("Attempting to run container (%s) with reaped init process", c.Id)
	}

	// Try and create the process
	err := c.create(p)
	if err != nil {
		return err
	}
	// Wait for the VM to acknowledge the process
	res := c.vm.WaitProcess(true, []string{c.Id}, -1)
	if res == nil {
		return err
	}
	// Start the process. If the startup fails, return the error now.
	err = c.start(p)
	if err != nil {
		return err
	}
	// Notify subscribers of the new process.
	e := Event{
		ID:        c.Id,
		Type:      EventContainerStart,
		Timestamp: time.Now(),
	}
	c.notify(e)
	// It is now safe to return control to the caller (this prevents docker-likes
	// from blocking when trying to send multiple commands before startup
	// finishes.

	// The rest of this function runs asynchronously.
	go func() {
		exit, err := c.wait(p, res)
		e = Event{
			ID:        c.Id,
			Type:      EventExit,
			Timestamp: time.Now(),
			PID:       p.Id,
			Status:    -1,
		}
		if err == nil && exit != nil {
			e.Timestamp = exit.FinishedAt
			e.Status = exit.Code
		}
		c.notify(e)
		// Reap the finished init process.
		c.ReapProcess(ContainerInitProcessId)
	}()

	return nil
}

func (c *Container) create(p *Process) error {
	// save the state

	glog.V(3).Infof("prepare hypervisor info")
	config := api.ContainerDescriptionFromOCF(c.Id, c.Spec)

	rootPath := c.Spec.Root.Path
	if !filepath.IsAbs(rootPath) {
		rootPath = filepath.Join(c.BundlePath, rootPath)
	}
	vmRootfs := filepath.Join(hypervisor.BaseDir, c.vm.Id, hypervisor.ShareDirTag, c.Id, "rootfs")
	os.MkdirAll(vmRootfs, 0755)

	// Mount rootfs
	err := utils.Mount(rootPath, vmRootfs)
	if err != nil {
		glog.Errorf("mount %s to %s failed: %s\n", rootPath, vmRootfs, err.Error())
		return err
	}

	// Pre-create dirs necessary for hyperstart before setting rootfs readonly
	// TODO: a better way to support readonly rootfs
	if err = preCreateDirs(rootPath); err != nil {
		return err
	}

	// Mount necessary files and directories from spec
	for _, m := range c.Spec.Mounts {
		if err := mountToRootfs(&m, vmRootfs, ""); err != nil {
			return fmt.Errorf("mounting %q to rootfs %q at %q failed: %v", m.Source, m.Destination, vmRootfs, err)
		}
	}

	// set rootfs readonly
	if c.Spec.Root.Readonly {
		err = utils.SetReadonly(vmRootfs)
		if err != nil {
			glog.Errorf("set rootfs %s readonly failed: %s\n", vmRootfs, err.Error())
			return err
		}
	}

	r := c.vm.AddContainer(config)
	if !r.IsSuccess() {
		return fmt.Errorf("add container %s failed: %s", c.Id, r.Message())
	}

	return nil
}

func (c *Container) start(p *Process) error {

	glog.V(3).Infof("save state id %s, bundle %s", c.Id, c.BundlePath)
	stateDir := filepath.Join(c.stateDir, c.Id)
	_, err := os.Stat(stateDir)
	if err == nil {
		glog.V(1).Infof("Container %s exists\n", c.Id)
		return fmt.Errorf("Container %s exists\n", c.Id)
	}
	err = os.MkdirAll(stateDir, 0644)
	if err != nil {
		glog.V(1).Infof("%s\n", err.Error())
		return err
	}

	state := &specs.State{
		Version:    c.Spec.Version,
		ID:         c.Id,
		Pid:        c.NamespaceId,
		BundlePath: c.BundlePath,
	}
	stateData, err := json.MarshalIndent(state, "", "\t")
	if err != nil {
		glog.V(1).Infof("%s\n", err.Error())
		return err
	}
	stateFile := filepath.Join(stateDir, "state.json")
	err = ioutil.WriteFile(stateFile, stateData, 0644)
	if err != nil {
		glog.V(1).Infof("%s\n", err.Error())
		return err
	}

	err = c.vm.Attach(p.stdio, c.Id, nil)
	if err != nil {
		glog.V(1).Infof("StartPod fail: fail to set up tty connection.\n")
		return err
	}

	err = execPrestartHooks(c.Spec, state)
	if err != nil {
		glog.V(1).Infof("execute Prestart hooks failed, %s\n", err.Error())
		return err
	}

	//Todo: vm.AddContainer here
	return c.vm.StartContainer(c.Id)
}

func (c *Container) wait(p *Process, result <-chan *api.ProcessExit) (*api.ProcessExit, error) {
	state := &specs.State{
		Version:    c.Spec.Version,
		ID:         c.Id,
		Pid:        -1,
		BundlePath: c.BundlePath,
	}

	err := execPoststartHooks(c.Spec, state)
	if err != nil {
		glog.V(1).Infof("execute Poststart hooks failed %s\n", err.Error())
	}

	exit, ok := <-result
	if !ok {
		exit = nil
		glog.V(1).Infof("get exit code failed %s\n", err.Error())
	}

	err = execPoststopHooks(c.Spec, state)
	if err != nil {
		glog.V(1).Infof("execute Poststop hooks failed %s\n", err.Error())
		return exit, err
	}
	return exit, nil
}

// AddProcess adds a process to the current container. It is called by
// HyperPod.AddProcess, which validates processId uniqueness at the pod-level.
func (c *Container) AddProcess(processId, stdin, stdout, stderr string, spec *specs.Process) (*Process, error) {
	// Lock the container while doing a processID check - if failed, we
	// unlock, but if success we need to ensure no other process with that ID
	// is created before we finish.
	c.Lock()
	defer c.Unlock()
	if c.ShouldReap() {
		return nil, fmt.Errorf("init process of the container %s had already exited", c.Id)
	}

	if _, ok := c.processes[processId]; ok {
		return nil, fmt.Errorf("conflict process ID (%s)", processId)
	}
	// check init process has not exited
	if _, ok := c.processes["init"]; !ok {
		return nil, fmt.Errorf("init process of the container %s had already exited", c.Id)
	}
	// check we're not trying to add another init process
	if processId == "init" { // test in case the init process is being reaped
		return nil, fmt.Errorf("conflict process ID (%s)", processId)
	}

	p := &Process{
		Id:     processId,
		Stdin:  stdin,
		Stdout: stdout,
		Stderr: stderr,
		Spec:   spec,
		ProcId: -1,

		vm:		 c.vm,
		innerId: processId,
	}
	if err := p.setupIO(); err != nil {
		return nil, err
	}
	// Start a waiter in the VM for the results.
	resultCh := c.vm.WaitProcess(false, []string{processId}, -1)
	// Add the process to the container.
	err := c.vm.AddProcess(c.Id, processId, spec.Terminal, spec.Args, spec.Env, spec.Cwd, p.stdio)
	if err != nil {
		glog.V(1).Infof("add process to container failed: %v\n", err)
		return nil, err
	}
	// Container was added successfully, so add to container state
	c.processes[processId] = p
	// And notify event watchers
	e := Event{
		ID:        c.Id,
		Type:      EventProcessStart,
		Timestamp: time.Now(),
		PID:       processId,
	}
	c.notify(e)

	// The rest of this function runs asynchronously to wait for the exit
	// (note how it does absolutely no manipulation of the processes map)
	go func() {
		// Get the exit code
		exit := <-resultCh
		// Prepare the event
		e := Event{
			ID:        c.Id,
			Type:      EventExit,
			Timestamp: time.Now(),
			PID:       processId,
			Status:    -1,
		}
		// If the exit code was received, update the event
		if exit != nil {
			e.Status = int(exit.Code)
			e.Timestamp = exit.FinishedAt
		}
		// Notify event watchers of exit
		c.notify(e)
		// Reap the process
		c.ReapProcess(processId)
	}()
	return p, nil
}

func execHook(hook specs.Hook, state *specs.State) error {
	b, err := json.Marshal(state)
	if err != nil {
		return err
	}
	cmd := exec.Cmd{
		Path:  hook.Path,
		Args:  hook.Args,
		Env:   hook.Env,
		Stdin: bytes.NewReader(b),
	}
	return cmd.Run()
}

func execPrestartHooks(rt *specs.Spec, state *specs.State) error {
	for _, hook := range rt.Hooks.Prestart {
		err := execHook(hook, state)
		if err != nil {
			return err
		}
	}

	return nil
}

func execPoststartHooks(rt *specs.Spec, state *specs.State) error {
	for _, hook := range rt.Hooks.Poststart {
		err := execHook(hook, state)
		if err != nil {
			glog.V(1).Infof("exec Poststart hook %s failed %s", hook.Path, err.Error())
		}
	}

	return nil
}

func execPoststopHooks(rt *specs.Spec, state *specs.State) error {
	for _, hook := range rt.Hooks.Poststop {
		err := execHook(hook, state)
		if err != nil {
			glog.V(1).Infof("exec Poststop hook %s failed %s", hook.Path, err.Error())
		}
	}

	return nil
}

// Signal forwards a signal to a process in this container
func (c *Container) Signal(processId string, sig int) error {
	p := c.getProcess(processId)
	if p == nil {
		return fmt.Errorf("reapProcess: container %s does not have a process %s", c.Id, processId)
	}
	return p.Signal(sig)
}

// reapProcess reaps a given process from the container
func (c *Container) ReapProcess(process string) error {
	p := c.getProcess(process)
	if p == nil {
		return fmt.Errorf("reapProcess: container %s does not have a process %s", c.Id, process)
	}

	go p.reap()
	delete(c.processes, process)

	// TODO: kill all other processes if init process is killed.
	// Note: depends on support for killing non-init processes in containers in
	// Process
	if p.init {
		// TODO: need support for killing container processes.
	}

	// If no more processes, mark the container as reapable.
	if len(c.processes) == 0 {
		close(c.reaping)
		go c.reap()
	}

	return nil
}

// Returns true if the container has reaped/is about to be reaped.
// Returns false is the container is healthy.
func (c *Container) ShouldReap() bool {
	select {
	case <-c.reaping:
		return true
	default:
		return false
	}
}

func (c *Container) reap() {
	// Run these operations asynchronously to not block clean up.
	containerSharedDir := filepath.Join(hypervisor.BaseDir, c.vm.Id, hypervisor.ShareDirTag, c.Id)
	utils.Umount(filepath.Join(containerSharedDir, "rootfs"))
	os.RemoveAll(containerSharedDir)
	os.RemoveAll(filepath.Join(c.stateDir, c.Id))
}

func mountToRootfs(m *specs.Mount, rootfs, mountLabel string) error {
	// TODO: we don't use mountLabel here because it looks like mountLabel is
	// only significant when SELinux is enabled.
	var (
		dest = m.Destination
	)
	if !strings.HasPrefix(dest, rootfs) {
		dest = filepath.Join(rootfs, dest)
	}

	switch m.Type {
	case "proc", "sysfs", "mqueue", "tmpfs", "cgroup", "devpts":
		glog.V(3).Infof("Skip mount point %q of type %s", m.Destination, m.Type)
		return nil
	case "bind":
		stat, err := os.Stat(m.Source)
		if err != nil {
			// error out if the source of a bind mount does not exist as we will be
			// unable to bind anything to it.
			return err
		}
		// ensure that the destination of the bind mount is resolved of symlinks at mount time because
		// any previous mounts can invalidate the next mount's destination.
		// this can happen when a user specifies mounts within other mounts to cause breakouts or other
		// evil stuff to try to escape the container's rootfs.
		if dest, err = symlink.FollowSymlinkInScope(filepath.Join(rootfs, m.Destination), rootfs); err != nil {
			return err
		}
		if err := checkMountDestination(rootfs, dest); err != nil {
			return err
		}
		// update the mount with the correct dest after symlinks are resolved.
		m.Destination = dest
		if err := createIfNotExists(dest, stat.IsDir()); err != nil {
			return err
		}
		if err := mount.Mount(m.Source, dest, m.Type, strings.Join(m.Options, ",")); err != nil {
			return err
		}
	default:
		if err := os.MkdirAll(dest, 0755); err != nil {
			return err
		}
		return mount.Mount(m.Source, dest, m.Type, strings.Join(m.Options, ","))
	}
	return nil
}

// checkMountDestination checks to ensure that the mount destination is not over the top of /proc.
// dest is required to be an abs path and have any symlinks resolved before calling this function.
func checkMountDestination(rootfs, dest string) error {
	invalidDestinations := []string{
		"/proc",
	}
	// White list, it should be sub directories of invalid destinations
	validDestinations := []string{
		// These entries can be bind mounted by files emulated by fuse,
		// so commands like top, free displays stats in container.
		"/proc/cpuinfo",
		"/proc/diskstats",
		"/proc/meminfo",
		"/proc/stat",
		"/proc/net/dev",
	}
	for _, valid := range validDestinations {
		path, err := filepath.Rel(filepath.Join(rootfs, valid), dest)
		if err != nil {
			return err
		}
		if path == "." {
			return nil
		}
	}
	for _, invalid := range invalidDestinations {
		path, err := filepath.Rel(filepath.Join(rootfs, invalid), dest)
		if err != nil {
			return err
		}
		if path == "." || !strings.HasPrefix(path, "..") {
			return fmt.Errorf("%q cannot be mounted because it is located inside %q", dest, invalid)
		}

	}
	return nil
}

// preCreateDirs creates necessary dirs for hyperstart
func preCreateDirs(rootfs string) error {
	dirs := []string{
		"proc",
		"sys",
		"dev",
		"lib/modules",
	}
	for _, dir := range dirs {
		err := createIfNotExists(filepath.Join(rootfs, dir), true)
		if err != nil {
			return err
		}
	}
	return nil
}

// createIfNotExists creates a file or a directory only if it does not already exist.
func createIfNotExists(path string, isDir bool) error {
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			if isDir {
				return os.MkdirAll(path, 0755)
			}
			if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
				return err
			}
			f, err := os.OpenFile(path, os.O_CREATE, 0755)
			if err != nil {
				return err
			}
			f.Close()
		}
	}
	return nil
}
