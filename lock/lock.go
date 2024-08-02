package lock

import (
	"context"
	_ "embed"
	"errors"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	//go:embed script/lock.lua
	lockLuaScript string
	//go:embed script/release.lua
	releaseLuaScript string
)

// RedisLock RedisLock
type RedisLock struct {
	key           string
	identify      string
	leaseDuration time.Duration
	client        *redis.Client
}

// NewRedisLock NewRedisLock
func NewRedisLock(ctx context.Context, cfg *redis.Options, key, identify string, leaseDuration time.Duration) (
	*RedisLock, error,
) {
	client := redis.NewClient(cfg)
	return &RedisLock{
		key:           key,
		identify:      identify,
		leaseDuration: leaseDuration,
		client:        client,
	}, client.Ping(ctx).Err()
}

// TryAcquire 获取锁
func (r *RedisLock) TryAcquire(ctx context.Context) (bool, error) {
	args := []string{r.identify, strconv.Itoa(int(r.leaseDuration / time.Millisecond))}
	resp, err := r.client.Eval(ctx, lockLuaScript, []string{r.key}, args).Result()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	if reply, ok := resp.(string); ok && reply == "OK" {
		return true, nil
	}
	return false, errors.New("unknown reply")
}

// Release 释放锁
func (r *RedisLock) Release(ctx context.Context) (bool, error) {
	resp, err := r.client.Eval(ctx, releaseLuaScript, []string{r.key}, []string{r.identify}).Result()
	if err != nil {
		return false, err
	}
	if reply, ok := resp.(int64); ok && reply == 1 {
		return true, nil
	}
	return false, errors.New("unknown reply")
}

// Identity 获取主节点身份
func (r *RedisLock) Identity(ctx context.Context) (string, error) {
	return r.client.Get(ctx, r.key).Result()
}
