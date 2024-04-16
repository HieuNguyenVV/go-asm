package adapter

import (
	"context"
	"github.com/IBM/sarama"
	"golang.org/x/sync/errgroup"
	"log"
	"strings"
	"time"
)

type (
	consumerService struct {
		name    string
		client  sarama.ConsumerGroup
		context context.Context
		group   *errgroup.Group
		cancel  func()
	}
	consumerGroup struct {
		handlerFunc func(string, []byte, []byte) error
	}
)

func NewConsumer(ctx context.Context, conf ConsumerConfig, handlerFunc func(string, []byte, []byte) error) (Consumer, error) {
	consumer := &consumerService{
		name:    "consumer-kafka",
		client:  nil,
		context: nil,
		group:   nil,
		cancel:  nil,
	}
	var err error

	config := sarama.NewConfig()
	config.Metadata.Full = true
	config.ChannelBufferSize = 2560

	config.Consumer.Group.Rebalance.Timeout = time.Minute
	config.Consumer.Group.Rebalance.Retry.Max = 10
	config.Consumer.Group.Rebalance.Retry.Backoff = time.Second * 5
	//config.Consumer.Return.Errors = true
	config.Consumer.Offsets.AutoCommit.Enable = true
	config.Consumer.Offsets.AutoCommit.Interval = time.Second
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	consumer.client, err = sarama.NewConsumerGroup(strings.Split(conf.Config.Address, ","), conf.Group, config)
	if err != nil {
		return nil, err
	}
	global, cancel := context.WithCancel(ctx)
	consumer.cancel = cancel
	consumer.group, consumer.context = errgroup.WithContext(global)
	cfgHandler := &consumerGroup{
		handlerFunc: handlerFunc,
	}

	consumer.group.Go(func() error {
		for {
			if e := consumer.client.Consume(consumer.context, strings.Split(conf.Topics, ","), cfgHandler); e != nil {
				log.Printf("failed to consume message, err: %v", e)
			}
			if e := consumer.context.Err(); e != nil {
				return e
			}
		}
	})

	consumer.group.Go(func() error {
		for {
			select {
			case e := <-consumer.client.Errors():
				log.Printf("consumer get error, err: %v", e)
			case <-consumer.context.Done():
				consumer.cancel = nil
				if err := consumer.client.Close(); err != nil {
					log.Printf("failed to close consumer, err: %v", err)
				}
				return consumer.context.Err()
			}
		}
	})
	return consumer, nil
}

func (cs *consumerService) Close() {
	if cs.cancel != nil {
		if err := cs.group.Wait(); err != nil && err != context.Canceled {
			log.Printf("Failed to wait error group, err: %v", err)
		}
	}
}

func (cf *consumerGroup) Setup(session sarama.ConsumerGroupSession) error {
	log.Printf("consumer (%s) - claim (%v)", session.MemberID(), session.Claims())
	return nil
}

func (cf *consumerGroup) Cleanup(session sarama.ConsumerGroupSession) error {
	log.Printf("consumer (%s) - cleanup", session.MemberID())
	return nil
}

func (cf *consumerGroup) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	for msg := range claim.Messages() {
		if err := cf.handlerFunc(msg.Topic, msg.Key, msg.Value); err != nil {
			log.Printf("Failed to handler message, err: %v", err)
		} else {
			session.MarkMessage(msg, "")
		}

	}
	return nil
}
