package main

import (
	"context"
	"encoding/json"
	"log"
	"os/signal"
	"syscall"

	"github.com/twmb/franz-go/pkg/kgo"
)

type BurstAlert struct {
	Title      string `json:"title"`
	Domain     string `json:"domain"`
	EditCount  int    `json:"edit_count"`
	WindowMs   int64  `json:"window_ms"`
	DetectedAt int64  `json:"detected_at"`
}

func main() {
	cfg := LoadConfig()

	ctx, stop := signal.NotifyContext(
		context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.KafkaBrokers...),
		kgo.ConsumeTopics(cfg.AlertTopic),
		kgo.ConsumerGroup(cfg.GroupID),
	)
	if err != nil {
		log.Fatalf("failed to create kafka client: %v", err)
	}
	defer cl.Close()

	log.Printf("listening for burst alerts on topic %q...", cfg.AlertTopic)

	for {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			return
		}

		fetches.EachError(func(topic string, partition int32, err error) {
			log.Printf("fetch error on %s[%d]: %v", topic, partition, err)
		})

		fetches.EachRecord(func(record *kgo.Record) {
			handleAlert(record.Value)
		})
	}
}

func handleAlert(data []byte) {
	var alert BurstAlert
	if err := json.Unmarshal(data, &alert); err != nil {
		log.Printf("failed to parse alert: %v", err)
		return
	}

	// TODO: fetch the page diff, summarize with an LLM, and send an email.
	log.Printf("BURST ALERT: %q had %d edits (window %dms, detected at %d)",
		alert.Title, alert.EditCount, alert.WindowMs, alert.DetectedAt)
}
