// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package remotesamplingexporter // import "go.opentelemetry.io/collector/exporter/remotesamplingexporter"

import (
	"encoding"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

// EncodingType defines the type for content encoding
type EncodingType string

const (
	EncodingProto EncodingType = "proto"
	EncodingJSON  EncodingType = "json"
)

var _ encoding.TextUnmarshaler = (*EncodingType)(nil)

// UnmarshalText unmarshalls text to an EncodingType.
func (e *EncodingType) UnmarshalText(text []byte) error {
	if e == nil {
		return errors.New("cannot unmarshal to a nil *EncodingType")
	}

	str := string(text)
	switch str {
	case string(EncodingProto):
		*e = EncodingProto
	case string(EncodingJSON):
		*e = EncodingJSON
	default:
		return fmt.Errorf("invalid encoding type: %s", str)
	}

	return nil
}

// Config defines configuration for OTLP/HTTP exporter.
type Config struct {
	ClientConfig confighttp.ClientConfig         `mapstructure:",squash"` // squash ensures fields are correctly decoded in embedded struct.
	QueueConfig  exporterhelper.QueueBatchConfig `mapstructure:"sending_queue"`
	RetryConfig  configretry.BackOffConfig       `mapstructure:"retry_on_failure"`

	// The URL to send traces to. If omitted the Endpoint + "/v1/traces" will be used.
	TracesEndpoint string `mapstructure:"traces_endpoint"`

	// The URL to send metrics to. If omitted the Endpoint + "/v1/metrics" will be used.
	MetricsEndpoint string `mapstructure:"metrics_endpoint"`

	// The URL to send logs to. If omitted the Endpoint + "/v1/logs" will be used.
	LogsEndpoint string `mapstructure:"logs_endpoint"`

	// The URL to send profiles to. If omitted the Endpoint + "/v1development/profiles" will be used.
	ProfilesEndpoint string `mapstructure:"profiles_endpoint"`

	// The encoding to export telemetry (default: "proto")
	Encoding EncodingType `mapstructure:"encoding"`

	// Path to directory for storing pending data. (default "otlp-data")
	TmpDataPath string `mapstructure:"tmp_data_path"`

	// URL to the sampling server. required.
	TraceSamplingURL string `mapstructure:"trace_sampling_url"`

	// Wait duration since the trace data was added to pending queue. (default "30s")
	DecisionWait time.Duration `mapstructure:"decision_wait"`
}

var _ component.Config = (*Config)(nil)

// Validate checks if the exporter configuration is valid
func (cfg *Config) Validate() error {
	if cfg.ClientConfig.Endpoint == "" && cfg.TracesEndpoint == "" && cfg.MetricsEndpoint == "" && cfg.LogsEndpoint == "" && cfg.ProfilesEndpoint == "" {
		return errors.New("at least one endpoint must be specified")
	}
	return nil
}
