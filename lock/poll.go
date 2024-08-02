// copy and modify from k8s.io/apimachinery/pkg/util/wait/wait.go
package lock

import (
	"context"
	"errors"
	"time"
)

type (
	ConditionFunc            func() (done bool, err error)
	ConditionWithContextFunc func(context.Context) (done bool, err error)
)

func (cf ConditionFunc) WithContext() ConditionWithContextFunc {
	return func(context.Context) (done bool, err error) {
		return cf()
	}
}

type WaitWithContextFunc func(ctx context.Context) <-chan struct{}

// poller 定时器
func poller(interval, timeout time.Duration) WaitWithContextFunc {
	return func(ctx context.Context) <-chan struct{} {
		ch := make(chan struct{})
		go func() {
			defer close(ch)
			tick := time.NewTicker(interval)
			defer tick.Stop()
			var after <-chan time.Time
			if timeout != 0 {
				timer := time.NewTimer(timeout)
				after = timer.C
				defer timer.Stop()
			}
			for {
				select {
				case <-tick.C:
					select {
					case ch <- struct{}{}:
					default:
					}
				case <-after:
					return
				case <-ctx.Done():
					return
				}
			}
		}()
		return ch
	}
}

// poll 执行condition函数
func poll(ctx context.Context, immediate bool, wait WaitWithContextFunc, condition ConditionWithContextFunc) error {
	if immediate {
		done, err := runCondition(ctx, condition)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
	}

	select {
	case <-ctx.Done():
		return errors.New("timed out waiting for the condition")
	default:
		return WaitForWithContext(ctx, wait, condition)
	}
}

func runCondition(ctx context.Context, condition ConditionWithContextFunc) (bool, error) {
	defer HandleCrash()
	return condition(ctx)
}

// WaitForWithContext
func WaitForWithContext(ctx context.Context, wait WaitWithContextFunc, condition ConditionWithContextFunc) error {
	waitCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := wait(waitCtx)
	for {
		select {
		case _, open := <-c:
			if !open {
				return errors.New("timed out waiting for the condition")
			}
			ok, err := runCondition(ctx, condition)
			if err != nil {
				return err
			}
			if ok {
				return nil
			}
		case <-ctx.Done():
			return errors.New("timed out waiting for the condition")
		}
	}
}

// ContextForChannel
func ContextForChannel(parentCh <-chan struct{}) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		select {
		case <-parentCh:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}

// PollImmediateUntil
func PollImmediateUntil(interval time.Duration, condition ConditionFunc, stopCh <-chan struct{}) error {
	ctx, cancel := ContextForChannel(stopCh)
	defer cancel()
	return poll(ctx, true, poller(interval, 0), condition.WithContext())
}
