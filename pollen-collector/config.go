package main

import (
	"fmt"

	"github.com/BurntSushi/toml"
)

// CollectorConfig holds configuration for the pollen collector
type CollectorConfig struct {
	PredictionAPIEndpoint string
	PredictionAPIKey      string
}

func getConfig() *CollectorConfig {
	var config *CollectorConfig
	if _, err := toml.DecodeFile("collector.toml", &config); err != nil {
		fmt.Println(err)
		panic(err)
	}
	return config
}
