# Go Redis使用可重入锁支持选主
> 使用Redis实现多实例选主功能

`leaderelection`参考[K8S选主机制](https://github.com/kubernetes/client-go/tree/master/tools/leaderelection)使用一个有效的分布式锁，实现选主功能

```
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/CodeNinja917/leaderelection"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func main() {
	var (
		redisAddr string
		lockName  string
		id        string
	)
	flag.StringVar(&redisAddr, "redis-addr", "localhost:6379", "redis addr")
	flag.StringVar(&lockName, "lock-name", "", "the lease lock resource name")
	flag.StringVar(&id, "id", uuid.New().String(), "the holder identify name")
	flag.Parse()

	cfg := leaderelection.LeaderElectionConfig{
		RedisConfig: redis.Options{Addr: redisAddr},
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				for {
					select {
					case <-ctx.Done():
						return
					default:
						time.Sleep(10 * time.Second)
						fmt.Println("Controller loop...")
					}
				}
			},
			OnStoppedLeading: func(ctx context.Context) {
				fmt.Printf("leader lost: %s\n", id)
				os.Exit(0)
			},
			OnNewLeader: func(identity string) {
				if identity == id {
					return
				}
				fmt.Printf("new leader elected: %s\n", identity)
			},
		},
		ReleaseOnCancel: true,
		Identity:        id,
		Key:             lockName,
	}
	ctx := context.Background()
	le, err := leaderelection.NewLeaderElector(ctx, cfg)
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
		return
	}
	le.Run(ctx)
}
```