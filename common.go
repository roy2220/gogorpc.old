package pbrpc

import (
	"context"
	"fmt"
	"time"
)

type markingList struct {
	items           []string
	itemIsMarked    []bool
	markedItemCount int
}

func (self *markingList) addItem(item string) {
	self.items = append(self.items, item)
	self.itemIsMarked = append(self.itemIsMarked, false)
}

func (self *markingList) markItem(item string) bool {
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

func (self *markingList) unmarkItems() {
	for i, _ := range self.items {
		self.itemIsMarked[i] = false
	}

	self.markedItemCount = 0
}

func (self *markingList) getNumberOfMarkedItems() int {
	return self.markedItemCount
}

var defaultServerAddress = "127.0.0.1:8888"

func makeDeadline(context_ context.Context, timeout time.Duration) (time.Time, error) {
	if e := context_.Err(); e != nil {
		return time.Time{}, e
	}

	deadline1, ok := context_.Deadline()
	deadline2 := time.Now().Add(timeout)

	if ok && deadline1.Before(deadline2) {
		return deadline1, nil
	} else {
		return deadline2, nil
	}
}

func representMethodID(serviceName string, methodName string) string {
	return fmt.Sprintf("<%v.%v>", serviceName, methodName)
}
