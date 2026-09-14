package main

import (
	"os"
	"strings"
)

type Config struct {
	KafkaBrokers []string
	AlertTopic   string
	GroupID      string
}

func LoadConfig() Config {
	return Config{
		KafkaBrokers: strings.Split(getEnv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"), ","),
		AlertTopic:   getEnv("KAFKA_ALERT_TOPIC", "page-burst-alert"),
		GroupID:      getEnv("KAFKA_GROUP_ID", "wiki-streamer-notifier"),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
