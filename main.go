package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/bsm/redislock"
	publisher "github.com/rpolnx/go-redis-async-lock/internal/pub"
	"github.com/rpolnx/go-redis-async-lock/internal/repo"
	subscriber "github.com/rpolnx/go-redis-async-lock/internal/sub"
)

func main() {
	fmt.Println("Starting program")

	NUM_MESSAGES, _ := strconv.Atoi(os.Getenv("NUM_MESSAGES"))
	TOPIC_NAME := os.Getenv("TOPIC_NAME")
	NUM_SUBSCRIBER, _ := strconv.Atoi(os.Getenv("NUM_SUBSCRIBER"))
	KEY_NAME := os.Getenv("KEY_NAME")

	redisRepo, err := repo.NewRedisRepo()
	if err != nil {
		log.Fatal(err)
	}
	defer redisRepo.Client.Close()

	sub := subscriber.NewSubscriber(redisRepo)
	pub := publisher.NewPublisher(redisRepo)

	initialMap := map[string]int{}

	b, _ := json.Marshal(initialMap)

	r := redisRepo.Client.Set(context.Background(), "MY_MAP", b, 0)

	if r.Err() != nil {
		log.Fatal(r)
	}

	var wg sync.WaitGroup
	for i := 0; i < NUM_SUBSCRIBER; i++ {
		wg.Add(1)
		go func(idx int) {
			ps := sub.Subscribe(TOPIC_NAME)

			defer ps.Close()

			ctx := context.Background()
			for msg := range ps.Channel() {
				fmt.Println(fmt.Sprintf("Sub %d ->", idx+1), msg.Channel, msg.Payload)

				var lock *redislock.Lock
				defer lock.Release(ctx)
				strategy := redislock.LimitRetry(redislock.LinearBackoff(100*time.Millisecond), 300)
				lock, err = sub.Locker.Obtain(ctx, KEY_NAME, time.Duration(30)*time.Second,
					&redislock.Options{
						RetryStrategy: strategy,
					})

				if err == redislock.ErrNotObtained {
					fmt.Println("Could not obtain lock!", msg.Channel, msg.Payload, time.Now().Format(time.RFC3339))
					return
				} else if err != nil {
					fmt.Printf("Unexpected lock err: %v!", err)
				}

				fmt.Println(fmt.Sprintf("got lock %d ->", idx+1), msg.Channel, msg.Payload, time.Now().Format(time.RFC3339))

				redisV, err := redisRepo.Client.Get(context.Background(), "MY_MAP").Result()
				if err != nil {
					log.Fatal(err)
				}
				personalMap := map[string]int{}
				err = json.Unmarshal([]byte(redisV), &personalMap)
				if err != nil {
					log.Fatal(err)
				}

				_, ok := personalMap[fmt.Sprintf("SUB_%d", idx)]
				if !ok {
					personalMap[fmt.Sprintf("SUB_%d", idx)] = 0
				}
				personalMap[fmt.Sprintf("SUB_%d", idx)] += 1

				b, _ = json.Marshal(personalMap)
				r := redisRepo.Client.Set(context.Background(), "MY_MAP", b, 0)

				if r.Err() != nil {
					log.Fatal(r)
				}

				lock.Release(ctx)
				fmt.Println(fmt.Sprintf("release lock %d ->", idx+1), msg.Channel, msg.Payload, time.Now().Format(time.RFC3339))
			}

		}(i)
	}

	time.Sleep(100 * time.Millisecond)

	for i := 0; i < NUM_MESSAGES; i++ {
		err := pub.Publish(TOPIC_NAME, []byte(fmt.Sprintf("Message %d", i+1)))
		if err != nil {
			log.Fatal(err)
		}
	}
	wg.Wait()

	fmt.Println("ending")

}
