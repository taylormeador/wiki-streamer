package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	cfg := LoadConfig()

	ctx, stop := signal.NotifyContext(
		context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// init Kafka client
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.KafkaBrokers...),
		kgo.DefaultProduceTopic(cfg.KafkaTopic),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		cl.Flush(flushCtx)
	}()

	// enter streaming loop with retries
	maxAttempts := 10
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err = connectAndStream(ctx, cl, cfg)
		if ctx.Err() != nil {
			return
		}
		log.Println(err)

		multiplier := time.Duration(1 << (attempt - 1))
		delay := time.Second * multiplier
		log.Printf("Retrying in %d seconds", delay/1000000000)

		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return
		}

	}

}

func connectAndStream(ctx context.Context, cl *kgo.Client, cfg Config) error {
	req, err := http.NewRequestWithContext(ctx, "GET", cfg.WikiStreamURL, nil)
	if err != nil {
		log.Fatalf("Failed to create request: %v", err)
	}

	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("Connection", "keep-alive")
	req.Header.Set("User-Agent", cfg.UserAgent)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("Failed to connect to stream: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Unexpected status code: %d", resp.StatusCode)
	}

	log.Println("Connected to SSE stream. Waiting for events...")

	return readStream(ctx, resp.Body, cl, cfg.KafkaTopic)
}

func readStream(ctx context.Context, body io.Reader, cl *kgo.Client, topic string) error {
	scanner := bufio.NewScanner(body)
	var buffer bytes.Buffer

	for scanner.Scan() {
		line := scanner.Text()

		// An empty line signals the end of a single SSE event block
		if line == "" {
			if buffer.Len() > 0 {
				processEvent(ctx, buffer.String(), cl, topic)
				buffer.Reset()
			}
			continue
		}

		// Look for the "data:" prefix and load the contents into the buffer
		if after, ok := strings.CutPrefix(line, "data:"); ok {
			// handle multiple lines
			if buffer.Len() > 0 {
				buffer.WriteString("\n")
			}

			payload := strings.TrimSpace(after)
			buffer.WriteString(payload)
		}
	}

	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

func processEvent(ctx context.Context, data string, cl *kgo.Client, topic string) {
	log.Printf("New Event Received:\n%s\n\n", data)

	record := &kgo.Record{Topic: topic, Value: []byte(data)}
	cl.Produce(ctx, record, func(_ *kgo.Record, err error) {
		if err != nil {
			fmt.Printf("record had a produce error: %v\n", err)
			// TODO send to DLQ
		}
	})
}
