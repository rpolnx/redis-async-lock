package repo

import (
	"context"

	"github.com/redis/go-redis/v9"
)

type RedisRepo struct {
	Client *redis.Client
}

func NewRedisRepo() (*RedisRepo, error) {
	url := "redis://:@localhost:6379/0"
	opts, err := redis.ParseURL(url)
	if err != nil {
		return nil, err
	}

	client := redis.NewClient(opts)

	cmd := client.Ping(context.Background())

	if cmd.Err() != nil {
		return nil, cmd.Err()
	}

	return &RedisRepo{
		Client: client,
	}, err
}
