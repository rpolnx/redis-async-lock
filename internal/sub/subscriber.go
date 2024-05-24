package subscriber

import (
	"context"

	"github.com/bsm/redislock"
	"github.com/redis/go-redis/v9"
	"github.com/rpolnx/go-redis-async-lock/internal/repo"
)

type Subscriber struct {
	repo *repo.RedisRepo
	Locker *redislock.Client
}

func NewSubscriber(repo *repo.RedisRepo) *Subscriber {
	locker := redislock.New(repo.Client)
	return &Subscriber{
		repo:   repo,
		Locker: locker,
	}
}

func (p *Subscriber) Subscribe(channel string) *redis.PubSub {
	ps := p.repo.Client.Subscribe(context.Background(), channel)

	return ps
}

