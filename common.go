package pbrpc

import (
	"context"
	"time"
)

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
