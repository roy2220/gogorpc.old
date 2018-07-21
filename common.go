package pbrpc

import (
	"context"
	"fmt"
	"time"
)

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
