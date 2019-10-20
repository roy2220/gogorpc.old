package stream

import (
	"fmt"
	"sync"
	"time"

	"github.com/let-z-go/toolkit/utils"
	"github.com/rs/zerolog"

	"github.com/let-z-go/gogorpc/internal/transport"
)

type Options struct {
	Transport                 *transport.Options
	Logger                    *zerolog.Logger
	EventFilters              [1 + NumberOfEventDirections + NumberOfEventDirections*NumberOfEventTypes][]EventFilter
	ActiveHangupTimeout       time.Duration
	IncomingKeepaliveInterval time.Duration
	OutgoingKeepaliveInterval time.Duration
	IncomingConcurrencyLimit  int
	OutgoingConcurrencyLimit  int

	normalizeOnce sync.Once
}

func (self *Options) Normalize() *Options {
	self.normalizeOnce.Do(func() {
		if self.Transport == nil {
			self.Transport = &defaultTransportOptions
		}

		self.Transport.Normalize()

		if self.Logger == nil {
			self.Logger = self.Transport.Logger
		}

		normalizeDurValue(&self.ActiveHangupTimeout, defaultActiveHangupTimeout, minActiveHangupTimeout, maxActiveHangupTimeout)
		normalizeDurValue(&self.IncomingKeepaliveInterval, defaultKeepaliveInterval, minKeepaliveInterval, maxKeepaliveInterval)
		normalizeDurValue(&self.OutgoingKeepaliveInterval, defaultKeepaliveInterval, minKeepaliveInterval, maxKeepaliveInterval)
		normalizeIntValue(&self.IncomingConcurrencyLimit, defaultConcurrencyLimit, minConcurrencyLimit, maxConcurrencyLimit)
		normalizeIntValue(&self.OutgoingConcurrencyLimit, defaultConcurrencyLimit, minConcurrencyLimit, maxConcurrencyLimit)
	})

	return self
}

func (self *Options) AddEventFilter(eventDirection EventDirection, eventType EventType, eventFilter EventFilter) *Options {
	if eventDirection < 0 {
		utils.Assert(eventType < 0, func() string {
			return fmt.Sprintf("gogorpc/channel: invalid argument: eventType=%#v, eventDirection=%#v", eventType, eventDirection)
		})

		eventFilters := &self.EventFilters[0]
		i := len(*eventFilters)
		insertEventFilter(eventFilter, eventFilters, i)

		for ed, j := 0, 1; ed < NumberOfEventDirections; ed, j = ed+1, j+1 {
			insertEventFilter(eventFilter, &self.EventFilters[j], i)

			for et, k := 0, 1+NumberOfEventDirections+ed*NumberOfEventTypes; et < NumberOfEventTypes; et, k = et+1, k+1 {
				insertEventFilter(eventFilter, &self.EventFilters[k], i)
			}
		}
	} else {
		ed := int(eventDirection)

		if eventType < 0 {
			eventFilters := &self.EventFilters[1+ed]
			i := len(*eventFilters)
			insertEventFilter(eventFilter, eventFilters, i)

			for et, j := 0, 1+NumberOfEventDirections+ed*NumberOfEventTypes; et < NumberOfEventTypes; et, j = et+1, j+1 {
				insertEventFilter(eventFilter, &self.EventFilters[j], i)
			}
		} else {
			et := int(eventType)
			eventFilters := &self.EventFilters[1+NumberOfEventDirections+ed*NumberOfEventTypes+et]
			insertEventFilter(eventFilter, eventFilters, len(*eventFilters))
		}
	}

	return self
}

func (self *Options) GetEventFilters(eventDirection EventDirection, eventType EventType) []EventFilter {
	if eventDirection < 0 {
		utils.Assert(eventType < 0, func() string {
			return fmt.Sprintf("gogorpc/channel: invalid argument: eventType=%#v, eventDirection=%#v", eventType, eventDirection)
		})

		return self.EventFilters[0]
	}

	if eventType < 0 {
		return self.EventFilters[1+int(eventDirection)]
	}

	return self.DoGetEventFilters(eventDirection, eventType)
}

func (self *Options) DoGetEventFilters(eventDirection EventDirection, eventType EventType) []EventFilter {
	return self.EventFilters[1+NumberOfEventDirections+int(eventDirection)*NumberOfEventTypes+int(eventType)]
}

const (
	defaultActiveHangupTimeout = 3 * time.Second
	minActiveHangupTimeout     = 1 * time.Second
	maxActiveHangupTimeout     = 5 * time.Second
)

const (
	defaultKeepaliveInterval = 15 * time.Second
	minKeepaliveInterval     = 5 * time.Second
	maxKeepaliveInterval     = 60 * time.Second
)

const (
	defaultConcurrencyLimit = 1 << 17
	minConcurrencyLimit     = 1
	maxConcurrencyLimit     = 1 << 20
)

var defaultTransportOptions transport.Options

func insertEventFilter(eventFilter EventFilter, eventFilters *[]EventFilter, i int) {
	newEventFilters := make([]EventFilter, len(*eventFilters)+1)
	copy(newEventFilters[:i], (*eventFilters)[:i])
	newEventFilters[i] = eventFilter
	copy(newEventFilters[i+1:], (*eventFilters)[i:])
	*eventFilters = newEventFilters
}

func normalizeDurValue(value *time.Duration, defaultValue, minValue, maxValue time.Duration) {
	if *value == 0 {
		*value = defaultValue
		return
	}

	if *value < minValue {
		*value = minValue
		return
	}

	if *value > maxValue {
		*value = maxValue
		return
	}
}

func normalizeIntValue(value *int, defaultValue, minValue, maxValue int) {
	if *value == 0 {
		*value = defaultValue
		return
	}

	if *value < minValue {
		*value = minValue
		return
	}

	if *value > maxValue {
		*value = maxValue
		return
	}
}
