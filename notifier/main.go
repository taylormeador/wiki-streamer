package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type BurstAlert struct {
	Title       string `json:"title"`
	Domain      string `json:"domain"`
	EditCount   int    `json:"edit_count"`
	WindowMs    int64  `json:"window_ms"`
	DetectedAt  int64  `json:"detected_at"`
	RevisionOld int64  `json:"revision_old"`
	RevisionNew int64  `json:"revision_new"`
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
			handleAlert(ctx, record.Value)
		})
	}
}

func handleAlert(ctx context.Context, data []byte) {
	var alert BurstAlert
	if err := json.Unmarshal(data, &alert); err != nil {
		log.Printf("failed to parse alert: %v", err)
		return
	}

	// TODO: fetch the page diff, summarize with an LLM, and send an email.
	log.Printf("BURST ALERT: %q had %d edits (window %dms, detected at %d) old: %d new: %d",
		alert.Title, alert.EditCount, alert.WindowMs, alert.DetectedAt, alert.RevisionOld, alert.RevisionNew)

	// TODO what if revision id is not valid?
	url := fmt.Sprintf("https://en.wikipedia.org/w/rest.php/v1/revision/%d/compare/%d", alert.RevisionOld, alert.RevisionNew)

	ctx, cancel := context.WithTimeout(ctx, time.Duration(5*time.Second))
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		log.Printf("Failed to create request: %v", err)
		return
	}

	req.Header.Set("User-Agent", "wiki-streamer/0.1 (contact: taylor.r.meador@gmail.com)")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("Failed to get revision diff: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("Unexpected status code: %d", resp.StatusCode)
		return
	}

	var compare CompareResponse
	if err := json.NewDecoder(resp.Body).Decode(&compare); err != nil {
		log.Printf("failed to parse compare response: %v", err)
		return
	}

	log.Printf("parsed diff for %q: %d line(s)", alert.Title, len(compare.Diff))
}
