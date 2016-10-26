package hypervisor

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/golang/glog"
	hyperstartapi "github.com/hyperhq/runv/hyperstart/api/json"
	"github.com/hyperhq/runv/hypervisor/types"
	"github.com/hyperhq/runv/lib/term"
	"github.com/hyperhq/runv/lib/utils"
)

type WindowSize struct {
	Row    uint16 `json:"row"`
	Column uint16 `json:"column"`
}

type TtyIO struct {
	Stdin     io.ReadCloser
	Stdout    io.Writer
	Stderr    io.Writer
	OutCloser io.Closer
	Callback  chan *types.VmResponse
}

func (tty *TtyIO) WaitForFinish() error {
	if tty.Callback == nil {
		return fmt.Errorf("cannot wait on this tty")
	}

	<-tty.Callback

	glog.V(1).Info("tty is closed")
	if tty.Stdin != nil {
		tty.Stdin.Close()
	}
	if tty.OutCloser != nil {
		tty.OutCloser.Close()
	} else {
		cf := func(w io.Writer) {
			if w == nil {
				return
			}
			if c, ok := w.(io.WriteCloser); ok {
				c.Close()
			}
		}
		cf(tty.Stdout)
		cf(tty.Stderr)
	}

	return nil
}

// Concurrency-safe map type for ttySessions
type ttySessionMap struct {
	sessMap map[uint64]*ttyAttachments
	sync.RWMutex
}

func newTtySessionMap() *ttySessionMap {
	return &ttySessionMap{
		sessMap: make(map[uint64]*ttyAttachments),
	}
}

func (this *ttySessionMap) HasKey(session uint64) bool {
	_, found := this.Get(session)
	return found
}

func (this *ttySessionMap) Get(session uint64) (*ttyAttachments, bool) {
	this.RLock()
	defer this.RUnlock()
	value, found := this.sessMap[session]
	return value, found
}

func (this *ttySessionMap) Set(session uint64, value *ttyAttachments) {
	this.Lock()
	this.Unlock()
	this.sessMap[session] = value
}

func (this *ttySessionMap) Delete(session uint64) {
	this.Lock()
	this.Unlock()
	delete(this.sessMap, session)
}

// Thread-safe queue of attach commands
type pendingTtyQueue struct {
	queue []*AttachCommand
	sync.Mutex
}

func newPendingTtyQueue() *pendingTtyQueue {
	return &pendingTtyQueue{
		queue: make([]*AttachCommand, 0),
	}
}

func (this *pendingTtyQueue) Append(item *AttachCommand) {
	this.Lock()
	defer this.Unlock()
	this.queue = append(this.queue, item)
}

func (this *pendingTtyQueue) Pop() *AttachCommand {
	this.Lock()
	defer this.Unlock()
	if len(this.queue) == 0 {
		return nil
	}
	value := this.queue[0]

	newQueue := []*AttachCommand{}
	copy(newQueue, this.queue[1:])

	this.queue = newQueue
	return value
}

// Do an atomic swap of the entire queue.
func (this *pendingTtyQueue) Swap(newqueue []*AttachCommand) []*AttachCommand {
	this.Lock()
	defer this.Unlock()
	oldqueue := this.queue
	this.queue = newqueue
	return oldqueue
}

type ttyAttachments struct {
	persistent  bool
	started     bool
	closed      bool
	tty         bool
	stdioSeq    uint64
	stderrSeq   uint64
	attachments []*TtyIO
}

type pseudoTtys struct {
	attachId   uint64
	attachLock sync.RWMutex
	channel    chan *hyperstartapi.TtyMessage
	ttys       *ttySessionMap

	pendingTtys     *pendingTtyQueue
	pendingTtysLock sync.Mutex // Protect the attach queue

}

func newPts() *pseudoTtys {
	return &pseudoTtys{
		attachId:    1,
		channel:     make(chan *hyperstartapi.TtyMessage, 256),
		ttys:        newTtySessionMap(),
		pendingTtys: newPendingTtyQueue(),
	}
}

func readTtyMessage(conn *net.UnixConn) (*hyperstartapi.TtyMessage, error) {
	needRead := 12
	length := 0
	read := 0
	buf := make([]byte, 512)
	res := []byte{}
	for read < needRead {
		want := needRead - read
		if want > 512 {
			want = 512
		}
		glog.V(1).Infof("tty: trying to read %d bytes", want)
		nr, err := conn.Read(buf[:want])
		if err != nil {
			glog.Error("read tty data failed")
			return nil, err
		}

		res = append(res, buf[:nr]...)
		read = read + nr

		glog.V(1).Infof("tty: read %d/%d [length = %d]", read, needRead, length)

		if length == 0 && read >= 12 {
			length = int(binary.BigEndian.Uint32(res[8:12]))
			glog.V(1).Infof("data length is %d", length)
			if length > 12 {
				needRead = length
			}
		}
	}

	return &hyperstartapi.TtyMessage{
		Session: binary.BigEndian.Uint64(res[:8]),
		Message: res[12:],
	}, nil
}

func waitTtyMessage(ctx *VmContext, conn *net.UnixConn) {
	for {
		msg, ok := <-ctx.ptys.channel
		if !ok {
			glog.V(1).Info("tty chan closed, quit sent goroutine")
			break
		}

		glog.V(3).Infof("trying to write to session %d", msg.Session)

		if ctx.ptys.ttys.HasKey(msg.Session) {
			_, err := conn.Write(msg.ToBuffer())
			if err != nil {
				glog.V(1).Info("Cannot write to tty socket: ", err.Error())
				return
			}
		}
	}
}

func waitPts(ctx *VmContext) {
	conn, err := utils.UnixSocketConnect(ctx.TtySockName)
	if err != nil {
		glog.Error("Cannot connect to tty socket ", err.Error())
		ctx.Hub <- &InitFailedEvent{
			Reason: "Cannot connect to tty socket " + err.Error(),
		}
		return
	}

	glog.V(1).Info("tty socket connected")

	go waitTtyMessage(ctx, conn.(*net.UnixConn))

	for {
		res, err := readTtyMessage(conn.(*net.UnixConn))
		if err != nil {
			glog.V(1).Info("tty socket closed, quit the reading goroutine ", err.Error())
			ctx.Hub <- &Interrupted{Reason: "tty socket failed " + err.Error()}
			close(ctx.ptys.channel)
			return
		}
		if ta, ok := ctx.ptys.ttys.Get(res.Session); ok {
			if len(res.Message) == 0 {
				glog.V(1).Infof("session %d closed by peer, close pty", res.Session)
				ta.closed = true
			} else if ta.closed {
				var code uint8 = 255
				if len(res.Message) == 1 {
					code = uint8(res.Message[0])
				}
				glog.V(1).Infof("session %d, exit code %d", res.Session, code)
				ctx.ptys.Close(ctx, res.Session, code)
			} else {
				for _, tty := range ta.attachments {
					if tty.Stdout != nil && res.Session == ta.stdioSeq {
						_, err := tty.Stdout.Write(res.Message)
						if err != nil {
							glog.V(1).Infof("fail to write session %d, close pty attachment", res.Session)
							ctx.ptys.DetachBySessionId(res.Session, tty)
						}
					}
					if tty.Stderr != nil && res.Session == ta.stderrSeq {
						_, err := tty.Stderr.Write(res.Message)
						if err != nil {
							glog.V(1).Infof("fail to write session %d, close pty attachment", res.Session)
							ctx.ptys.DetachBySessionId(res.Session, tty)
						}
					}
				}
			}
		}
	}
}

func newAttachmentsWithTty(persist, isTty bool, tty *TtyIO) *ttyAttachments {
	ta := &ttyAttachments{
		persistent: persist,
		tty:        isTty,
	}

	if tty != nil {
		ta.attach(tty)
	}

	return ta
}

func (ta *ttyAttachments) attach(tty *TtyIO) {
	ta.attachments = append(ta.attachments, tty)
}

func (ta *ttyAttachments) detach(tty *TtyIO) {
	at := []*TtyIO{}
	detached := false
	for _, t := range ta.attachments {
		if tty != t {
			at = append(at, t)
		} else {
			detached = true
		}
	}
	if detached {
		ta.attachments = at
	}
}

func (ta *ttyAttachments) close() {
	for _, t := range ta.attachments {
		t.Close()
	}
	ta.attachments = []*TtyIO{}
}

func (ta *ttyAttachments) empty() bool {
	return len(ta.attachments) == 0
}

func (ta *ttyAttachments) isTty() bool {
	return ta.tty
}

func (tty *TtyIO) Close() {
	glog.V(1).Info("Close tty ")

	if tty.Callback != nil {
		close(tty.Callback)
	} else {
		if tty.Stdin != nil {
			tty.Stdin.Close()
		}
		if tty.OutCloser != nil {
			tty.OutCloser.Close()
		} else {
			cf := func(w io.Writer) {
				if w == nil {
					return
				}
				if c, ok := w.(io.WriteCloser); ok {
					c.Close()
				}
			}
			cf(tty.Stdout)
			cf(tty.Stderr)
		}
	}
}

// Set the current attachID (used when restoring hardware state)
// TODO: deprecate and only allow at initialization time
func (pts *pseudoTtys) SetAttachId(id uint64) {
	pts.attachLock.Lock()
	defer pts.attachLock.Unlock()
	pts.attachId = id
}

// Get the current attachID from the generator. Used when dumping hardware state.
func (pts *pseudoTtys) CurrentAttachId() uint64 {
	pts.attachLock.RLock()
	pts.attachLock.RUnlock()
	return pts.attachId
}

// Get the next AttachID and increment
func (pts *pseudoTtys) NextAttachId() uint64 {
	pts.attachLock.Lock()
	pts.attachLock.Unlock()
	id := pts.attachId
	pts.attachId++
	return id
}

func (pts *pseudoTtys) IsTty(session uint64) bool {
	if ta, ok := pts.ttys.Get(session); ok {
		return ta.isTty()
	}
	return false
}

// Detaches a TTY used on the given session ID
func (pts *pseudoTtys) DetachBySessionId(session uint64, tty *TtyIO) {
	// Make sure we close the tty even if we do nothing else
	defer tty.Close()

	ta, found := pts.ttys.Get(session)
	if !found {
		// Nothing to do.
		return
	}
	ta.detach(tty)

	if !ta.persistent && ta.empty() {
		// Remove the stdio reference
		pts.ttys.Delete(ta.stdioSeq)
		// Do we have an stderr somewhere else?
		if ta.stderrSeq > 0 {
			pts.ttys.Delete(ta.stderrSeq)
		}
	}
}

func (pts *pseudoTtys) Close(ctx *VmContext, session uint64, code uint8) {
	if ta, ok := pts.ttys.Get(session); ok {
		ack := make(chan bool, 1)
		kind := types.E_CONTAINER_FINISHED
		id := ctx.LookupBySession(session)

		if id == "" {
			if id = ctx.LookupExecBySession(session); id != "" {
				kind = types.E_EXEC_FINISHED
				//remove exec automatically
				ctx.DeleteExec(id)
			}
		}

		if id != "" {
			ctx.reportProcessFinished(kind, &types.ProcessFinished{
				Id: id, Code: code, Ack: ack,
			})
			// wait for pod handler setting up exitcode for container
			<-ack
		}

		ta.close()
		pts.ttys.Delete(ta.stdioSeq)
		if ta.stderrSeq > 0 {
			pts.ttys.Delete(ta.stderrSeq)
		}
	}
}

func (pts *pseudoTtys) ptyConnect(persist, isTty bool, stdioSeq, stderrSeq uint64, tty *TtyIO) {
	ta, ok := pts.ttys.Get(stdioSeq)
	if ok {
		ta.attach(tty)
	} else {
		ta := newAttachmentsWithTty(persist, isTty, tty)
		ta.stdioSeq = stdioSeq
		ta.stderrSeq = stderrSeq

		pts.ttys.Set(stdioSeq, ta)
		if stderrSeq > 0 {
			pts.ttys.Set(stderrSeq, ta)
		}
	}

	pts.connectStdin(stdioSeq, tty)
}

func (pts *pseudoTtys) startStdin(session uint64, isTty bool) {
	ta, ok := pts.ttys.Get(session)
	if ok {
		if !ta.started {
			ta.started = true
			for _, tty := range ta.attachments {
				pts.connectStdin(session, tty)
			}
		}
	}
}

// we close the stdin of the container when the last attached
// stdin closed. we should move this decision to hyper and use
// the same policy as docker(stdinOnce)
func (pts *pseudoTtys) isLastStdin(session uint64) bool {
	var count int

	if ta, ok := pts.ttys.Get(session); ok {
		for _, tty := range ta.attachments {
			if tty.Stdin != nil {
				count++
			}
		}
	}

	return count == 1
}

func (pts *pseudoTtys) connectStdin(session uint64, tty *TtyIO) {
	// TODO: replace with accessor usage here
	if ta, ok := pts.ttys.Get(session); !ok || !ta.started {
		return
	}

	if tty.Stdin != nil {
		go func() {
			buf := make([]byte, 32)
			keys, _ := term.ToBytes(DetachKeys)
			isTty := pts.IsTty(session)

			defer func() { recover() }()
			for {
				nr, err := tty.Stdin.Read(buf)
				if nr == 1 && isTty {
					for i, key := range keys {
						if nr != 1 || buf[0] != key {
							break
						}
						if i == len(keys)-1 {
							glog.Info("got stdin detach keys, exit term")
							pts.DetachBySessionId(session, tty)
							return
						}
						nr, err = tty.Stdin.Read(buf)
					}
				}
				if err != nil {
					glog.Info("a stdin closed, ", err.Error())
					if err == io.EOF && !isTty && pts.isLastStdin(session) {
						// send eof to hyperstart
						glog.V(1).Infof("session %d send eof to hyperstart", session)
						pts.channel <- &hyperstartapi.TtyMessage{
							Session: session,
							Message: make([]byte, 0),
						}
						// don't detach, we need the last output of the container
					} else {
						pts.DetachBySessionId(session, tty)
					}
					return
				}

				glog.V(3).Infof("trying to input char: %d and %d chars", buf[0], nr)

				mbuf := make([]byte, nr)
				copy(mbuf, buf[:nr])
				pts.channel <- &hyperstartapi.TtyMessage{
					Session: session,
					Message: mbuf[:nr],
				}
			}
		}()
	}

	return
}

func (pts *pseudoTtys) closePendingTtys() {
	for {
		tty := pts.pendingTtys.Pop()
		if tty == nil {
			break
		}
		tty.Streams.Close()
	}
}

func TtyLiner(conn io.Reader, output chan string) {
	buf := make([]byte, 1)
	line := []byte{}
	cr := false
	emit := false
	for {

		nr, err := conn.Read(buf)
		if err != nil || nr < 1 {
			glog.V(1).Info("Input byte chan closed, close the output string chan")
			close(output)
			return
		}
		switch buf[0] {
		case '\n':
			emit = !cr
			cr = false
		case '\r':
			emit = true
			cr = true
		default:
			cr = false
			line = append(line, buf[0])
		}
		if emit {
			output <- string(line)
			line = []byte{}
			emit = false
		}
	}
}

func (vm *Vm) Attach(tty *TtyIO, container string, size *WindowSize) error {
	cmd := &AttachCommand{
		Streams:   tty,
		Size:      size,
		Container: container,
	}

	return vm.GenericOperation("Attach", func(ctx *VmContext, result chan<- error) {
		ctx.attachCmd(cmd, result)
	}, StateInit, StateStarting, StateRunning)
}

func (vm *Vm) GetLogOutput(container string, callback chan *types.VmResponse) (io.ReadCloser, io.ReadCloser, error) {
	stdout, stdoutStub := io.Pipe()
	stderr, stderrStub := io.Pipe()
	outIO := &TtyIO{
		Stdin:    nil,
		Stdout:   stdoutStub,
		Stderr:   stderrStub,
		Callback: callback,
	}

	cmd := &AttachCommand{
		Streams:   outIO,
		Container: container,
	}

	vm.GenericOperation("Attach", func(ctx *VmContext, result chan<- error) {
		ctx.attachCmd(cmd, result)
	}, StateInit, StateStarting, StateRunning)

	return stdout, stderr, nil
}
