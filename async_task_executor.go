package pbrpc

import (
	"context"
	"math"
	"time"
	"unsafe"

	"github.com/let-z-go/intrusive_containers/list"
	"github.com/let-z-go/toolkit/deque"
	"github.com/let-z-go/toolkit/lazy_map"
	"github.com/let-z-go/toolkit/semaphore"
)

type asyncTaskExecutor struct {
	key2DequeOfAsyncTaskItems lazy_map.LazyMap
}

func (self *asyncTaskExecutor) executeAsyncTask(context_ context.Context, key string, asyncTask func()) error {
	retryDelay := time.Duration(0)

	for {
		value, valueClearer, _ := self.key2DequeOfAsyncTaskItems.GetOrSetValue(key, func() (interface{}, error) {
			dequeOfAsyncTaskItems := (&deque.Deque{}).Initialize(math.MaxInt32)
			return dequeOfAsyncTaskItems, nil
		})

		dequeOfAsyncTaskItems := value.(*deque.Deque)
		asyncTaskItem_ := asyncTaskItem{data: asyncTask}
		e := dequeOfAsyncTaskItems.AppendNode(context_, &asyncTaskItem_.listNode)

		if valueClearer == nil {
			if e != nil {
				if e != semaphore.SemaphoreClosedError {
					return e
				}

				const minRetryDelay = 5 * time.Millisecond
				const maxRetryDelay = 1 * time.Second

				if retryDelay == 0 {
					retryDelay = minRetryDelay
				} else {
					retryDelay *= 2

					if retryDelay > maxRetryDelay {
						retryDelay = maxRetryDelay
					}
				}

				select {
				case <-time.After(retryDelay):
				case <-context_.Done():
					return context_.Err()
				}

				continue
			}
		} else {
			go processAsyncTaskItems(dequeOfAsyncTaskItems, valueClearer)
		}

		return nil
	}
}

type asyncTaskItem struct {
	listNode list.ListNode
	data     func()
}

func processAsyncTaskItems(dequeOfAsyncTaskItems *deque.Deque, valueClearer func()) {
	for {
		context_, _ := context.WithTimeout(context.Background(), 3*time.Second)
		listOfAsyncTaskItems := (&list.List{}).Initialize()
		_, e := dequeOfAsyncTaskItems.RemoveAllNodes(context_, true, listOfAsyncTaskItems)

		if e != nil {
			dequeOfAsyncTaskItems.Close(listOfAsyncTaskItems)
		}

		getListNode := listOfAsyncTaskItems.GetNodes()

		for listNode := getListNode(); listNode != nil; listNode = getListNode() {
			asyncTaskItem_ := (*asyncTaskItem)(listNode.GetContainer(unsafe.Offsetof(asyncTaskItem{}.listNode)))
			asyncTaskItem_.data()
		}

		if e != nil {
			valueClearer()
			return
		}
	}
}
