package main

import (
	"context"
	"fmt"
	"github.com/panjf2000/ants/v2"
	"golang.org/x/sync/errgroup"
	"log"
	"os/signal"
	"sync"
	"syscall"
)

var dataStore int

func main() {
	contextGlobal, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT, syscall.SIGKILL)
	defer cancel()

	group, ctx := errgroup.WithContext(contextGlobal)

	inputChan := make(chan int, 20)
	outputChan := make(chan string, 20)

	group.Go(func() error {
		for i := 0; i < 100; i++ {
			inputChan <- i
		}

		for {
			select {
			case <-ctx.Done():
				close(inputChan)
				return ctx.Err()
			default:
				continue
			}
		}
	})

	group.Go(func() error {
		var (
			wg           sync.WaitGroup
			mutex        sync.Mutex
			numberWorker int = 10
		)

		pool, _ := ants.NewPoolWithFunc(numberWorker, func(job interface{}) {
			defer wg.Done()
			mutex.Lock()
			dataStore = job.(int) + 1
			outputChan <- fmt.Sprintf("output: %v", dataStore)
			mutex.Unlock()
		})
		defer pool.Release()

		for job := range inputChan {
			wg.Add(1)
			if err := pool.Invoke(job); err != nil {
				return err
			}
		}
		wg.Wait()
		close(outputChan)

		//success
		return nil
	})

	group.Go(func() error {
		for result := range outputChan {
			fmt.Println(result)
		}

		return nil
	})

	if err := group.Wait(); err != nil {
		log.Printf("One or more goroutine returned err, err: %v", err)
		return
	}
}
