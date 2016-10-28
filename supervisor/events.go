package supervisor

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/golang/glog"
)

const (
	EventExit           = "exit"
	EventContainerStart = "start-container"
	EventProcessStart   = "start-process"
)

var (
	defaultEventsBufferSize = 128
)

type Event struct {
	ID        string    `json:"id"`
	Type      string    `json:"type"`
	Timestamp time.Time `json:"timestamp"`
	PID       string    `json:"pid,omitempty"`
	Status    int       `json:"status,omitempty"`
}

// Represents a subscriber and acts a mini-queue for subscribers which are
// catching up.
type svEventSubscriber struct {
	// Holds events which occurred while the subscriber is catching up.
	localEventLog []Event
	storedOnly    bool             // Subscriber does not receive new events at the moment.
	closing       chan interface{} // If subscriber has closed, this channel is closed
	// Protects the eventSubscriber.
	*sync.Mutex
}

type SvEvents struct {
	subscriberLock sync.RWMutex
	// Subscribers need 2 channels - one is the channel we emit events to them
	// on, the other is used to notify internal routines if the subscriber
	// unsubscribes early.
	subscribers map[chan Event]svEventSubscriber

	eventLog  []Event
	eventLock sync.Mutex
}

func NewSvEvents() *SvEvents {
	return &SvEvents{
		subscribers: make(map[chan Event]svEventSubscriber),
	}
}

func (se *SvEvents) setupEventLog(logDir string) error {
	if err := se.readEventLog(logDir); err != nil {
		return err
	}
	events := se.Events(time.Time{}, false, "")
	f, err := os.OpenFile(filepath.Join(logDir, "events.log"), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0755)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	go func() {
		for e := range events {
			glog.Infof("write event log: %v", e)
			se.eventLock.Lock()
			se.eventLog = append(se.eventLog, e)
			se.eventLock.Unlock()
			if err := enc.Encode(e); err != nil {
				glog.Infof("containerd: fail to write event to journal")
			}
		}
	}()
	return nil
}

// Note: no locking - don't call after initialization
func (se *SvEvents) readEventLog(logDir string) error {
	f, err := os.Open(filepath.Join(logDir, "events.log"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	for {
		var e Event
		if err := dec.Decode(&e); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		se.eventLock.Lock()
		se.eventLog = append(se.eventLog, e)
		se.eventLock.Unlock()
	}
	return nil
}

// Events returns an event channel that external consumers can use to receive updates
// on container events
func (se *SvEvents) Events(from time.Time, storedOnly bool, id string) chan Event {
	// Make event channel
	c := make(chan Event, defaultEventsBufferSize)
	// Make the channel available as a subscriber
	subscriber := svEventSubscriber{
		localEventLog: []Event{},
		storedOnly:    storedOnly,
		closing:       make(chan interface{}),
		Mutex:         new(sync.Mutex),
	}

	se.subscriberLock.Lock()
	se.subscribers[c] = subscriber
	se.subscriberLock.Unlock()

	if !from.IsZero() {
		// snapshot history at this point in time.
		se.eventLock.Lock()
		subscriber.Lock()
		subscriber.localEventLog = se.eventLog[:]
		subscriber.Unlock()
		se.eventLock.Unlock()

		// Start dequeuing events from history
		go func() {
			for {
				subscriber.Lock()
				if len(subscriber.localEventLog) == 0 {
					if storedOnly {
						// Close the channel and finish.
						se.Unsubscribe(c)
					} else {
						// Lock the subscriber, send the live event notification.
						subscriber.Lock()
						subscriber.storedOnly = false
						// Notify the client that from now on it's live events
						c <- Event{
							Type:      "live",
							Timestamp: time.Now(),
						}
					}
					subscriber.Unlock()
					break
				}
				// Dequeue an item
				e := subscriber.localEventLog[0]
				subscriber.localEventLog = subscriber.localEventLog[1:]

				if e.Timestamp.After(from) {
					if id == "" || e.ID == id {
						select {
						case _ = <-subscriber.closing:
							// Subscriber exited early
							return
						default:
							c <- e
						}
					}
				}
				subscriber.Unlock()
			}
		}()
	}
	return c
}

// Unsubscribe removes the provided channel from receiving any more events
func (se *SvEvents) Unsubscribe(sub chan Event) {
	se.subscriberLock.Lock()
	defer se.subscriberLock.Unlock()
	if subinfo, ok := se.subscribers[sub]; ok {
		delete(se.subscribers, sub)
		close(sub)
		close(subinfo.closing)
	}
}

// notifySubscribers will send the provided event to the external subscribers
// of the events channel
func (se *SvEvents) notifySubscribers(e Event) {
	glog.Infof("notifySubscribers: %v", e)
	se.subscriberLock.RLock()
	defer se.subscriberLock.RUnlock()
	for c := range se.subscribers {
		sub := se.subscribers[c]
		sub.Lock()
		if sub.storedOnly {
			// Queue while sub empties
			sub.localEventLog = append(sub.localEventLog, e)
		} else {
			// do a non-blocking send for the channel
			select {
			case c <- e:
			default:
				glog.Warningf("containerd: event not sent to subscriber")
			}
		}
		sub.Unlock()
	}
}
