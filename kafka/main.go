package main

import (
	"context"
	"encoding/json"
	"fmt"
	"go-asm/kafka/adapter"
	"golang.org/x/sync/errgroup"
	"log"
	"os/signal"
	"syscall"
	"time"
)

const (
	topic = "event-topic"
)

func main() {
	globalCtx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	group, ctx := errgroup.WithContext(globalCtx)

	group.Go(func() error {
		proConf := adapter.ProducerConfig{
			Config: adapter.Config{Address: "localhost:9092"},
			Async:  false,
		}
		pro, err := adapter.NewProducer(ctx, proConf)
		if err != nil {
			log.Printf("Error create producer, err: %v", err)
			return err
		}
		defer pro.Close()

		for i := 0; i <= 100; i++ {
			event := adapter.Event{
				Msg:      fmt.Sprintf("Event: %v", i),
				CreateAt: time.Now().Unix(),
			}

			body, err := json.Marshal(&event)
			if err != nil {
				return err
			}
			if err := pro.Producer(topic, "", body); err != nil {
				log.Printf("Producer msg error, err: %v", err)
				return err
			}
		}

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				continue
			}
		}
	})

	group.Go(func() error {
		csConfig := adapter.ConsumerConfig{
			Config: adapter.Config{Address: "localhost:9092"},
			Topics: topic,
			Group:  "test_1",
		}
		consumer, err := adapter.NewConsumer(context.Background(), csConfig, handleFunc)
		if err != nil {
			log.Printf("Error create consumer, err: %v", err)
			return err
		}
		consumer.Close()
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				continue
			}
		}
	})

	if err := group.Wait(); err != nil {
		log.Printf("One or more groutine error, err: %v", err)
		return
	}
}

func handleFunc(_ string, _ []byte, value []byte) error {
	var event adapter.Event
	if err := json.Unmarshal(value, &event); err != nil {
		return err
	}
	log.Printf("Event: %v", event)
	return nil
}
