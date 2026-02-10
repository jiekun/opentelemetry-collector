// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package retroactivesamplingprocessor // import "go.opentelemetry.io/collector/processor/retroactivesamplingprocessor"

import (
	"errors"
	"time"

	"go.opentelemetry.io/collector/component"
)

// Config defines configuration for batch processor.
type Config struct {
	// Timeout sets the time after which a batch will be sent regardless of size.
	// When this is set to zero, batched data will be sent immediately.
	Timeout time.Duration `mapstructure:"timeout"`

	// The unique id of this processor. The requests it sends will carry such ID
	// so the retroactive sampling server could know who it's talking to.
	SamplingAgentID string `mapstructure:"sampling_agent_id"`

	// Path to directory for storing pending data. (default "otlp-data")
	TmpDataPath string `mapstructure:"tmp_data_path"`

	// URL to the sampling server. required.
	RetroactiveSamplingURLs []string `mapstructure:"retroactive_sampling_urls"`

	// Wait duration since the trace data was added to pending queue. (default "30s")
	DecisionWait time.Duration `mapstructure:"decision_wait"`

	// prevent unkeyed literal initialization
	_ struct{}
}

var _ component.Config = (*Config)(nil)

// Validate checks if the processor configuration is valid
func (cfg *Config) Validate() error {
	if len(cfg.RetroactiveSamplingURLs) == 0 {
		return errors.New("retroactive_sampling_urls must be set")
	}
	if cfg.DecisionWait < 0 {
		return errors.New("decision_wait must be greater or equal to 0")
	}
	if cfg.Timeout < 0 {
		return errors.New("timeout must be greater or equal to 0")
	}
	return nil
}
