package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

type Item struct {
	ProductID int `json:"product_id"`
	Quantity  int `json:"quantity"`
}

type CreateOrderRequest struct {
	UserID int    `json:"user_id"`
	Items  []Item `json:"items"`
}

type Job struct {
	Request CreateOrderRequest
}

const (
	workers   = 50
	jobsCount = 5000
)

func main() {
	log.Println("Запуск генератора нагрузки")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	jobs := make(chan Job, 100)

	client := &http.Client{Timeout: 5 * time.Second}

	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go worker(ctx, i, client, jobs, &wg)
	}

	go func() {
		for i := 0; i < jobsCount; i++ {
			jobs <- Job{
				Request: randomOrder(),
			}
		}
		close(jobs)
	}()

	wg.Wait()

	log.Println("Load finished")
}

func worker(
	ctx context.Context,
	id int,
	client *http.Client,
	jobs <-chan Job,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	for job := range jobs {
		err := doRequest(client, job.Request)
		if err != nil {
			log.Printf("[worker %d] error: %v\n", id, err)
		}
	}
}

func doRequest(client *http.Client, reqBody CreateOrderRequest) error {
	data, err := json.Marshal(reqBody)
	if err != nil {
		return err
	}

	req, err := http.NewRequest(
		"POST",
		"http://localhost:8000/orders",
		bytes.NewBuffer(data),
	)
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}

func randomOrder() CreateOrderRequest {
	return CreateOrderRequest{
		UserID: rand.Intn(100),
		Items: []Item{
			{
				ProductID: 10,
				Quantity:  rand.Intn(5) + 1,
			},
		},
	}
}
