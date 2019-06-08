package pbrpc

import (
	"context"
	"net"
	"time"

	"golang.org/x/net/websocket"
)

type connection struct {
	raw     net.Conn
	preRWCs chan *connectionPreRWC
}

func (self *connection) establish(raw net.Conn) {
	self.raw = raw
	self.preRWCs = make(chan *connectionPreRWC, 2)

	go func() {
		readCancellation := noCancellation
		readDeadline := time.Time{}
		writeCancellation := noCancellation
		writeDeadline := time.Time{}

		for {
			select {
			case preRWC := <-self.preRWCs:
				switch preRWC.type_ {
				case 'R':
					if preRWC.deadline != readDeadline {
						readDeadline = preRWC.deadline
						self.raw.SetReadDeadline(readDeadline)
					}

					readCancellation = preRWC.cancellation
					preRWC.completion <- struct{}{}
				case 'W':
					if preRWC.deadline != writeDeadline {
						writeDeadline = preRWC.deadline
						self.raw.SetWriteDeadline(writeDeadline)
					}

					writeCancellation = preRWC.cancellation
					preRWC.completion <- struct{}{}
				default: // case 'C':
					preRWC.completion <- struct{}{}
					return
				}
			case <-readCancellation:
				readDeadline = time.Now()
				self.raw.SetReadDeadline(readDeadline)
				readCancellation = noCancellation
			case <-writeCancellation:
				writeDeadline = time.Now()
				self.raw.SetWriteDeadline(writeDeadline)
				writeCancellation = noCancellation
			}
		}
	}()
}

func (self *connection) read(context_ context.Context, deadline time.Time, buffer []byte) (int, error) {
	completion := make(chan struct{}, 1)
	self.preRWCs <- &connectionPreRWC{'R', context_.Done(), deadline, completion}
	<-completion
	n, e := self.raw.Read(buffer)

	if e != nil {
		if e2 := context_.Err(); e2 != nil {
			e = e2
		}
	}

	return n, e
}

func (self *connection) write(context_ context.Context, deadline time.Time, data []byte) (int, error) {
	completion := make(chan struct{}, 1)
	self.preRWCs <- &connectionPreRWC{'W', context_.Done(), deadline, completion}
	<-completion
	var nn int
	var e error

	switch raw := self.raw.(type) {
	case *websocket.Conn:
		i := 0
		nn = 0

		for {
			j := i + maxWebSocketFramePayloadSize
			var n int

			if j >= len(data) {
				n, e = raw.Write(data[i:])
				nn += n
				break
			}

			n, e = raw.Write(data[i:j])
			nn += n

			if e != nil {
				break
			}

			i = j
		}
	default:
		nn, e = self.raw.Write(data)
	}

	if e != nil {
		if e2 := context_.Err(); e2 != nil {
			e = e2
		}
	}

	return nn, e
}

func (self *connection) close(force bool) error {
	completion := make(chan struct{}, 1)
	self.preRWCs <- &connectionPreRWC{'C', nil, time.Time{}, completion}
	<-completion

	if force {
		if raw, ok := self.raw.(*net.TCPConn); ok {
			raw.SetLinger(0)
		}
	}

	e := self.raw.Close()
	self.raw = nil
	self.preRWCs = nil
	return e
}

type connectionPreRWC struct {
	type_        byte
	cancellation <-chan struct{}
	deadline     time.Time
	completion   chan<- struct{}
}

var noCancellation = make(<-chan struct{})
