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

func (o *Options) Normalize() *Options {
	o.normalizeOnce.Do(func() {
		if o.Transport == nil {
			o.Transport = &defaultTransportOptions
		}

		o.Transport.Normalize()

		if o.Logger == nil {
			o.Logger = o.Transport.Logger
		}

		normalizeDurValue(&o.ActiveHangupTimeout, defaultActiveHangupTimeout, minActiveHangupTimeout, maxActiveHangupTimeout)
		normalizeDurValue(&o.IncomingKeepaliveInterval, defaultKeepaliveInterval, minKeepaliveInterval, maxKeepaliveInterval)
		normalizeDurValue(&o.OutgoingKeepaliveInterval, defaultKeepaliveInterval, minKeepaliveInterval, maxKeepaliveInterval)
		normalizeIntValue(&o.IncomingConcurrencyLimit, defaultConcurrencyLimit, minConcurrencyLimit, maxConcurrencyLimit)
		normalizeIntValue(&o.OutgoingConcurrencyLimit, defaultConcurrencyLimit, minConcurrencyLimit, maxConcurrencyLimit)
	})

	return o
}

func (o *Options) AddEventFilter(eventDirection EventDirection, eventType EventType, eventFilter EventFilter) *Options {
	if eventDirection < 0 {
		utils.Assert(eventType < 0, func() string {
			return fmt.Sprintf("gogorpc/channel: invalid argument: eventType=%#v, eventDirection=%#v", eventType, eventDirection)
		})

		eventFilters := &o.EventFilters[0]
		i := len(*eventFilters)
		insertEventFilter(eventFilter, eventFilters, i)

		for ed, j := 0, 1; ed < NumberOfEventDirections; ed, j = ed+1, j+1 {
			insertEventFilter(eventFilter, &o.EventFilters[j], i)

			for et, k := 0, 1+NumberOfEventDirections+ed*NumberOfEventTypes; et < NumberOfEventTypes; et, k = et+1, k+1 {
				insertEventFilter(eventFilter, &o.EventFilters[k], i)
			}
		}
	} else {
		ed := int(eventDirection)

		if eventType < 0 {
			eventFilters := &o.EventFilters[1+ed]
			i := len(*eventFilters)
			insertEventFilter(eventFilter, eventFilters, i)

			for et, j := 0, 1+NumberOfEventDirections+ed*NumberOfEventTypes; et < NumberOfEventTypes; et, j = et+1, j+1 {
				insertEventFilter(eventFilter, &o.EventFilters[j], i)
			}
		} else {
			et := int(eventType)
			eventFilters := &o.EventFilters[1+NumberOfEventDirections+ed*NumberOfEventTypes+et]
			insertEventFilter(eventFilter, eventFilters, len(*eventFilters))
		}
	}

	return o
}

func (o *Options) GetEventFilters(eventDirection EventDirection, eventType EventType) []EventFilter {
	if eventDirection < 0 {
		utils.Assert(eventType < 0, func() string {
			return fmt.Sprintf("gogorpc/channel: invalid argument: eventType=%#v, eventDirection=%#v", eventType, eventDirection)
		})

		return o.EventFilters[0]
	}

	if eventType < 0 {
		return o.EventFilters[1+int(eventDirection)]
	}

	return o.DoGetEventFilters(eventDirection, eventType)
}

func (o *Options) DoGetEventFilters(eventDirection EventDirection, eventType EventType) []EventFilter {
	return o.EventFilters[1+NumberOfEventDirections+int(eventDirection)*NumberOfEventTypes+int(eventType)]
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
	*eventFilters = append(*eventFilters, nil)

	for j := len(*eventFilters) - 1; j > i; j-- {
		(*eventFilters)[j] = (*eventFilters)[j-1]
	}

	(*eventFilters)[i] = eventFilter
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
