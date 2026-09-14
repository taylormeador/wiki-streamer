package main

import (
	"os"
	"strings"
)

type Config struct {
	KafkaBrokers  []string
	KafkaTopic    string
	WikiStreamURL string
	UserAgent     string
}

func LoadConfig() Config {
	return Config{
		KafkaBrokers:  strings.Split(getEnv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"), ","),
		KafkaTopic:    getEnv("KAFKA_TOPIC", "recent-change"),
		WikiStreamURL: getEnv("WIKI_STREAM_URL", "https://stream.wikimedia.org/v2/stream/recentchange"),
		UserAgent:     getEnv("USER_AGENT", "wiki-streamer/0.1 (contact: taylor.r.meador@gmail.com)"),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
