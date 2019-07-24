package pbrpc

import (
	"context"
	"strconv"
	"time"
)

const normalNumberOfMethodInterceptors = 8
const defaultServerAddress = "127.0.0.1:8888"

type markingList struct {
	items           []string
	itemIsMarked    []bool
	markedItemCount int
}

func (self *markingList) AddItem(item string) {
	self.items = append(self.items, item)
	self.itemIsMarked = append(self.itemIsMarked, false)
}

func (self *markingList) MarkItem(item string) bool {
	for i, item2 := range self.items {
		if item2 == item {
			if !self.itemIsMarked[i] {
				self.itemIsMarked[i] = true
				self.markedItemCount++
			}

			return true
		}
	}

	return false
}

func (self *markingList) UnmarkItems() {
	for i := range self.items {
		self.itemIsMarked[i] = false
	}

	self.markedItemCount = 0
}

func (self *markingList) GetNumberOfMarkedItems() int {
	return self.markedItemCount
}

func makeDeadline(context_ context.Context, timeout time.Duration) time.Time {
	deadline1, ok := context_.Deadline()

	if timeout < 1 {
		if ok {
			return deadline1
		} else {
			return time.Time{}
		}
	} else {
		deadline2 := time.Now().Add(timeout)

		if ok && deadline1.Before(deadline2) {
			return deadline1
		} else {
			return deadline2
		}
	}
}

func makeMethodInterceptorLocator(serviceName string, methodIndex int32) string {
	serviceNameLength := len(serviceName)
	rawMethodInterceptorLocator := make([]byte, serviceNameLength+1, serviceNameLength+5)
	copy(rawMethodInterceptorLocator, []byte(serviceName))
	rawMethodInterceptorLocator[serviceNameLength] = ':'
	rawMethodInterceptorLocator = strconv.AppendInt(rawMethodInterceptorLocator[serviceNameLength+1:], int64(methodIndex), 10)
	return string(rawMethodInterceptorLocator)
}
