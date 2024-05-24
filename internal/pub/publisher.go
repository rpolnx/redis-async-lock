package publisher

import (
	"context"

	"github.com/rpolnx/go-redis-async-loc/internal/repo"
)

type Publisher struct {
	repo *repo.RedisRepo
}

func NewPublisher(repo *repo.RedisRepo) *Publisher {
	return &Publisher{
		repo: repo,
	}
}

func (p *Publisher) Publish(channel string, payload []byte) error {
	return p.repo.Client.Publish(context.Background(), channel, payload).Err()
}
