package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	cfg := LoadConfig()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
		cl.Flush(ctx)
		cl.Close()
	}()

	// init wiki stream
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
		log.Fatalf("Failed to connect to stream: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Fatalf("Unexpected status code: %d", resp.StatusCode)
	}

	log.Println("Connected to SSE stream. Waiting for events...")

	// enter loop
	readStream(resp.Body, cl, ctx, cfg.KafkaTopic)
}

func readStream(body io.Reader, cl *kgo.Client, ctx context.Context, topic string) {
	scanner := bufio.NewScanner(body)
	var buffer bytes.Buffer

	for scanner.Scan() {
		line := scanner.Text()

		// An empty line signals the end of a single SSE event block
		if line == "" {
			if buffer.Len() > 0 {
				processEvent(buffer.String(), cl, ctx, topic)
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
		log.Printf("Stream read error: %v", err)
	}
}

func processEvent(data string, cl *kgo.Client, ctx context.Context, topic string) {
	log.Printf("New Event Received:\n%s\n\n", data)

	record := &kgo.Record{Topic: topic, Value: []byte(data)}
	cl.Produce(ctx, record, func(_ *kgo.Record, err error) {
		if err != nil {
			fmt.Printf("record had a produce error: %v\n", err)
		}
	})
}
