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
	PacketFilters             [1 + NumberOfDirections + NumberOfDirections*NumberOfMessageTypes][]PacketFilter
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

func (self *Options) AddPacketFilter(direction Direction, messageType MessageType, packetFilter PacketFilter) *Options {
	if direction < 0 {
		utils.Assert(messageType < 0, func() string {
			return fmt.Sprintf("gogorpc/channel: invalid argument: messageType=%#v, direction=%#v", messageType, direction)
		})

		packetFilters := &self.PacketFilters[0]
		i := len(*packetFilters)
		insertPacketFilter(packetFilter, packetFilters, i)

		for d, j := 0, 1; d < NumberOfDirections; d, j = d+1, j+1 {
			insertPacketFilter(packetFilter, &self.PacketFilters[j], i)

			for mt, k := 0, 1+NumberOfDirections+d*NumberOfMessageTypes; mt < NumberOfMessageTypes; mt, k = mt+1, k+1 {
				insertPacketFilter(packetFilter, &self.PacketFilters[k], i)
			}
		}
	} else {
		d := int(direction)

		if messageType < 0 {
			packetFilters := &self.PacketFilters[1+d]
			i := len(*packetFilters)
			insertPacketFilter(packetFilter, packetFilters, i)

			for mt, j := 0, 1+NumberOfDirections+d*NumberOfMessageTypes; mt < NumberOfMessageTypes; mt, j = mt+1, j+1 {
				insertPacketFilter(packetFilter, &self.PacketFilters[j], i)
			}
		} else {
			mt := int(messageType)
			packetFilters := &self.PacketFilters[1+NumberOfDirections+d*NumberOfMessageTypes+mt]
			insertPacketFilter(packetFilter, packetFilters, len(*packetFilters))
		}
	}

	return self
}

func (self *Options) GetPacketFilters(direction Direction, messageType MessageType) []PacketFilter {
	if direction < 0 {
		utils.Assert(messageType < 0, func() string {
			return fmt.Sprintf("gogorpc/channel: invalid argument: messageType=%#v, direction=%#v", messageType, direction)
		})

		return self.PacketFilters[0]
	}

	if messageType < 0 {
		return self.PacketFilters[1+int(direction)]
	}

	return self.DoGetPacketFilters(direction, messageType)
}

func (self *Options) DoGetPacketFilters(direction Direction, messageType MessageType) []PacketFilter {
	return self.PacketFilters[1+NumberOfDirections+int(direction)*NumberOfMessageTypes+int(messageType)]
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

func insertPacketFilter(packetFilter PacketFilter, packetFilters *[]PacketFilter, i int) {
	newPacketFlters := make([]PacketFilter, len(*packetFilters)+1)
	copy(newPacketFlters[:i], (*packetFilters)[:i])
	newPacketFlters[i] = packetFilter
	copy(newPacketFlters[i+1:], (*packetFilters)[i:])
	*packetFilters = newPacketFlters
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
