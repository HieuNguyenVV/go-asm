package adapter

import (
	"context"
	"github.com/IBM/sarama"
	"log"
	"strings"
	"time"
)

type (
	syncproducerService struct {
		name    string
		context context.Context
		client  sarama.SyncProducer
		cancel  func()
	}
	asyncproducerService struct {
		name    string
		context context.Context
		client  sarama.AsyncProducer
		cancel  func()
	}
)

func NewProducer(ctx context.Context, conf ProducerConfig) (Producer, error) {
	config := sarama.NewConfig()

	config.Metadata.Full = true
	config.ChannelBufferSize = 2560

	config.Producer.Partitioner = sarama.NewHashPartitioner
	config.Producer.Return.Errors = true
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Timeout = time.Minute
	config.Producer.Retry.Max = 10
	config.Producer.Retry.Backoff = time.Second * 5

	if conf.Async {
		pro, err := sarama.NewAsyncProducer(strings.Split(conf.Config.Address, ","), config)
		if err != nil {
			return nil, err
		}

		ps := &asyncproducerService{
			name:    "kafka-asyn",
			context: nil,
			client:  pro,
			cancel:  nil,
		}

		ps.context, ps.cancel = context.WithCancel(ctx)
		go ps.monitor()
		return ps, nil
	} else {
		pro, err := sarama.NewSyncProducer(strings.Split(conf.Config.Address, ","), config)
		if err != nil {
			return nil, err
		}

		ps := &syncproducerService{
			name:    "kafka-sync",
			context: nil,
			client:  pro,
			cancel:  nil,
		}
		ps.context, ps.cancel = context.WithCancel(ctx)
		return ps, nil
	}
}

func (p *syncproducerService) Producer(topic, key string, value []byte) error {
	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Key:       nil,
		Value:     sarama.ByteEncoder(value),
		Headers:   nil,
		Metadata:  nil,
		Offset:    0,
		Partition: 0,
		Timestamp: time.Time{},
	}

	if key != "" {
		msg.Key = sarama.StringEncoder(key)
	}
	if _, _, err := p.client.SendMessage(msg); err != nil {
		return err
	}
	return nil
}

func (p *syncproducerService) Close() {
	if err := p.client.Close(); err != nil {
		log.Printf("Error close sync produce, err: %v", err)
	}
}

func (p *asyncproducerService) Producer(topic, key string, value []byte) error {
	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Key:       nil,
		Value:     sarama.ByteEncoder(value),
		Headers:   nil,
		Metadata:  nil,
		Offset:    0,
		Partition: 0,
		Timestamp: time.Time{},
	}

	if key != "" {
		msg.Key = sarama.StringEncoder(key)
	}

	p.client.Input() <- msg
	return nil
}

func (p *asyncproducerService) Close() {
	if p.cancel != nil {
		p.cancel()
	}
}

func (p *asyncproducerService) monitor() {
	for {
		select {
		case err := <-p.client.Errors():
			if err != nil {
				bs, e := err.Msg.Value.Encode()
				if e != nil {
					log.Printf("Failed to get error message, err: %v", e)
					continue
				}
				log.Printf("Failed to produce message, error: %v, document: %v", err, string(bs))
			}
		case <-p.context.Done():
			log.Printf("Close ansync producer")
			if err := p.client.Close(); err != nil {
				log.Printf("Failed to close async producer, error: %v", err)
			}
			return
		}
	}
}
