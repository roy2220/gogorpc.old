package channel

import (
	"fmt"
	"sync"
)

const (
	EventNone EventType = iota
	EventAccepting
	EventAccepted
	EventAcceptFailed
	EventConnecting
	EventConnected
	EventConnectFailed
	EventProcessFailed
)

const (
	None State = iota
	Initial
	Establishing
	Established
	Closed
)

type Listener struct {
	events chan Event
}

func (self *Listener) Init(normalNumberOfEvents int) *Listener {
	if normalNumberOfEvents < minNormalNumberOfEvents {
		normalNumberOfEvents = minNormalNumberOfEvents
	}

	self.events = make(chan Event, normalNumberOfEvents)
	return self
}

func (self *Listener) Close() {
	close(self.events)
}

func (self *Listener) FireEvent(eventType EventType, oldState, newState State) {
	self.events <- Event{eventType, oldState, newState}
}

func (self *Listener) Events() <-chan Event {
	return self.events
}

type Event struct {
	Type     EventType
	OldState State
	NewState State
}

type EventType int

func (self EventType) GoString() string {
	switch self {
	case EventNone:
		return "<EventNone>"
	case EventAccepting:
		return "<EventAccepting>"
	case EventAccepted:
		return "<EventAccepted>"
	case EventAcceptFailed:
		return "<EventAcceptFailed>"
	case EventConnecting:
		return "<EventConnecting>"
	case EventConnected:
		return "<EventConnected>"
	case EventConnectFailed:
		return "<EventConnectFailed>"
	case EventProcessFailed:
		return "<EventProcessFailed>"
	default:
		return fmt.Sprintf("<State:%d>", self)
	}
}

type State int

func (self State) GoString() string {
	switch self {
	case None:
		return "<None>"
	case Initial:
		return "<Initial>"
	case Establishing:
		return "<Establishing>"
	case Established:
		return "<Established>"
	case Closed:
		return "<Closed>"
	default:
		return fmt.Sprintf("<State:%d>", self)
	}
}

const minNormalNumberOfEvents = 4

type listenerManager struct {
	lock      sync.Mutex
	listeners map[*Listener]struct{}
}

func (self *listenerManager) AddListener(doubleChecker func() error, normalNumberOfEvents int) (*Listener, error) {
	if err := doubleChecker(); err != nil {
		return nil, err
	}

	listener := new(Listener).Init(normalNumberOfEvents)
	self.lock.Lock()

	if err := doubleChecker(); err != nil {
		return nil, err
	}

	listeners := self.listeners

	if listeners == nil {
		listeners = map[*Listener]struct{}{}
		self.listeners = listeners
	}

	listeners[listener] = struct{}{}
	self.lock.Unlock()
	return listener, nil
}

func (self *listenerManager) RemoveListener(doubleChecker func() error, listener *Listener) error {
	if err := doubleChecker(); err != nil {
		return err
	}

	self.lock.Lock()

	if err := doubleChecker(); err != nil {
		return err
	}

	delete(self.listeners, listener)
	self.lock.Unlock()
	listener.Close()
	return nil
}

func (self *listenerManager) FireEvent(eventType EventType, oldState, newState State) {
	self.lock.Lock()

	for listener := range self.listeners {
		listener.FireEvent(eventType, oldState, newState)
	}

	self.lock.Unlock()
}

func (self *listenerManager) Close() {
	self.lock.Lock()

	for listener := range self.listeners {
		listener.Close()
	}

	self.lock.Unlock()
}
