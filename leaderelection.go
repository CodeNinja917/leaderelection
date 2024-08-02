package leaderelection

import (
	"context"
	"fmt"
	"time"

	"github.com/CodeNinja917/leaderelection/lock"
	"github.com/google/uuid"
	"k8s.io/klog/v2"

	"github.com/redis/go-redis/v9"
)

// LeaderCallbacks 选主时回调配置
type LeaderCallbacks struct {
	// OnStartedLeading 主节点启动是执行
	OnStartedLeading func(context.Context)
	// OnStoppedLeading 主节点停止时执行
	OnStoppedLeading func(context.Context)
	// OnNewLeader 被选为主节点时执行
	OnNewLeader func(string)
}

// LeaderElectionConfig 配置信息
type LeaderElectionConfig struct {
	RedisConfig redis.Options
	// LeaseDuration redis锁ttl时间. 默认15s
	LeaseDuration time.Duration
	// RenewDeadline 重新设置锁的超时时间. 默认10s
	RenewDeadline time.Duration
	// RetryPeriod 获取锁时的最小重试间隔时间. 默认2s
	RetryPeriod time.Duration
	// Callbacks 主节点回调函数
	Callbacks LeaderCallbacks
	// ReleaseOnCancel 取消时是否释放锁
	ReleaseOnCancel bool
	// Key 锁名称
	Key string
	// Identity 节点标识. 默认是uuid
	Identity string
}

func initLeaderElectionConfig(lec *LeaderElectionConfig) {
	if lec.LeaseDuration == 0 {
		lec.LeaseDuration = time.Second * 15
	}
	if lec.RenewDeadline == 0 {
		lec.RenewDeadline = time.Second * 10
	}
	if lec.RetryPeriod == 0 {
		lec.RetryPeriod = time.Second * 2
	}
}

type LeaderElector struct {
	config   LeaderElectionConfig
	lock     *lock.RedisLock
	isLeader bool
}

func NewLeaderElector(ctx context.Context, lec LeaderElectionConfig) (*LeaderElector, error) {
	initLeaderElectionConfig(&lec)
	if lec.LeaseDuration <= lec.RenewDeadline {
		return nil, fmt.Errorf("leaseDuration must be greater than renewDeadline")
	}
	if lec.RenewDeadline <= time.Duration(lock.JitterFactor*float64(lec.RetryPeriod)) {
		return nil, fmt.Errorf("renewDeadline must be greater than retryPeriod*JitterFactor")
	}
	if lec.LeaseDuration < 1 {
		return nil, fmt.Errorf("leaseDuration must be greater than zero")
	}
	if lec.RenewDeadline < 1 {
		return nil, fmt.Errorf("renewDeadline must be greater than zero")
	}
	if lec.RetryPeriod < 1 {
		return nil, fmt.Errorf("retryPeriod must be greater than zero")
	}
	if lec.Callbacks.OnStartedLeading == nil {
		return nil, fmt.Errorf("OnStartedLeading callback must not be nil")
	}
	if lec.Callbacks.OnStoppedLeading == nil {
		return nil, fmt.Errorf("OnStoppedLeading callback must not be nil")
	}
	if lec.Key == "" {
		return nil, fmt.Errorf("key must be not empty")
	}
	if lec.Identity == "" {
		lec.Identity = uuid.New().String()
	}

	lock, err := lock.NewRedisLock(ctx, &lec.RedisConfig, lec.Key, lec.Identity, lec.LeaseDuration)
	if err != nil {
		return nil, err
	}
	return &LeaderElector{
		config: lec,
		lock:   lock,
	}, nil
}

func (le *LeaderElector) Run(ctx context.Context) {
	defer lock.HandleCrash()
	defer func() {
		if le.config.Callbacks.OnStoppedLeading != nil {
			le.config.Callbacks.OnStoppedLeading(ctx)
		}
	}()

	if !le.acquire(ctx) {
		// 取消上下文
		return
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go le.config.Callbacks.OnStartedLeading(ctx)
	le.renew(ctx)
}

func (le *LeaderElector) acquire(ctx context.Context) bool {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		acquire bool
		err     error
	)
	klog.Infof("attempting to acquire leader lease %v...", le.config.Key)

	backoff := lock.NewJitteredBackoffManager(le.config.RetryPeriod, lock.JitterFactor)
	lock.BackoffUntil(ctx, func(ctx context.Context) {
		acquire, err = le.lock.TryAcquire(ctx)
		if err != nil {
			klog.Infof("failed to acquire lease %v: %v, will retry", le.config.Key, err)
			return
		}
		if !acquire {
			klog.V(4).Infof("failed to acquire lease %v, will retry", le.config.Key)
			return
		}
		le.maybeReportTransition()
		klog.Infof("successfully acquired lease %v", le.config.Key)
		cancel()
	}, backoff, true, ctx.Done())

	return acquire
}

// maybeReportTransition 主节点切换,更新相关字段
func (le *LeaderElector) maybeReportTransition() {
	if le.isLeader {
		return
	}
	le.isLeader = true
	if le.config.Callbacks.OnNewLeader != nil {
		go le.config.Callbacks.OnNewLeader(le.config.Identity)
	}
}

// renew 续约
func (le *LeaderElector) renew(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	backoff := lock.NewJitteredBackoffManager(le.config.RetryPeriod, lock.JitterFactor)
	lock.BackoffUntil(ctx, func(ctx context.Context) {
		timeoutCtx, timeoutCancel := context.WithTimeout(ctx, le.config.RenewDeadline)
		defer timeoutCancel()
		err := lock.PollImmediateUntil(le.config.RetryPeriod, func() (bool, error) {
			return le.lock.TryAcquire(timeoutCtx)
		}, timeoutCtx.Done())
		if err == nil {
			klog.V(5).Infof("successfully acquired lease %v", le.config.Key)
			return
		}
		le.maybeReportTransition()
		klog.Infof("failed to renew lease %v: %v\n", le.config.Key, err)
		cancel()
	}, backoff, true, ctx.Done())

	if le.config.ReleaseOnCancel {
		le.release(ctx)
	}
}

func (le *LeaderElector) release(ctx context.Context) {
	if !le.isLeader {
		return
	}

	ok, err := le.lock.Release(ctx)
	if err != nil {
		klog.Infof("failed to release lease %v: %v", le.config.Key, err)
		return
	}
	if !ok {
		klog.Infof("failed to release lease %v", le.config.Key)
		return
	}
	le.isLeader = false
}
