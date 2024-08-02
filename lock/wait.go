// copy and modify from k8s.io/apimachinery/pkg/util/wait/wait.go
package lock

import (
	"context"
	"math/rand"
	"runtime/debug"
	"time"

	"k8s.io/klog/v2"
)

const JitterFactor = 1.2

func init() {
	rand.NewSource(time.Now().UnixNano())
}

// JitteredBackoffManager 指数退避计时器,用于获取指数级的时间周期
type JitteredBackoffManager struct {
	backoffTimer *time.Timer
	duration     time.Duration
	jitter       float64
}

// NewJitteredBackoffManager NewJitteredBackoffManager
func NewJitteredBackoffManager(duration time.Duration, jitter float64) *JitteredBackoffManager {
	if jitter <= 0.0 {
		jitter = 1.0
	}
	return &JitteredBackoffManager{
		duration: duration,
		jitter:   jitter,
	}
}

// getNextBackoff 根据jitter计算下一次的等待时间
func (j *JitteredBackoffManager) getNextBackoff() time.Duration {
	jitterPeriod := j.duration
	if j.jitter > 0.0 {
		jitterPeriod = j.duration + time.Duration(rand.Float64()*j.jitter*float64(j.duration))
	}
	return jitterPeriod
}

// Backoff 返回下一次等待时间的timer
func (j *JitteredBackoffManager) Backoff() *time.Timer {
	backoff := j.getNextBackoff()
	if j.backoffTimer == nil {
		j.backoffTimer = time.NewTimer(backoff)
	} else {
		j.backoffTimer.Reset(backoff)
	}
	return j.backoffTimer
}

// BackoffUntil 重试函数f
func BackoffUntil(ctx context.Context, f func(context.Context), backoff *JitteredBackoffManager, sliding bool, stopCh <-chan struct{}) {
	var t *time.Timer
	for {
		select {
		case <-stopCh:
			return
		default:
		}

		if !sliding {
			t = backoff.Backoff()
		}

		func() {
			defer HandleCrash()
			f(ctx)
		}()

		if sliding {
			t = backoff.Backoff()
		}

		select {
		case <-stopCh:
			if !t.Stop() {
				<-t.C
			}
		case <-t.C:
		}
	}
}

// HandleCrash 打印crash堆栈
func HandleCrash() {
	if r := recover(); r != nil {
		klog.InfoS("recovered from panic", "err", r, "stack", string(debug.Stack()))
	}
}
