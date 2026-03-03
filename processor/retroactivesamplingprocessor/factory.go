// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:generate mdatagen metadata.yaml

package retroactivesamplingprocessor // import "go.opentelemetry.io/collector/processor/retroactivesamplingprocessor"

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/retroactivesamplingprocessor/internal/metadata"
)

const (
	defaultTimeout      = 1000 * time.Millisecond
	defaultDecisionWait = 30 * time.Second
	defaultSamplingRate = 0 // 1 means 1%. [0, 100]
)

// NewFactory returns a new factory for the Batch processor.
func NewFactory() processor.Factory {
	return processor.NewFactory(
		metadata.Type,
		createDefaultConfig,
		processor.WithTraces(createTraces, metadata.TracesStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		Timeout:      defaultTimeout,
		TmpDataPath:  "./otlp-data",
		DecisionWait: defaultDecisionWait,
		SamplingRate: defaultSamplingRate,
	}
}

func createTraces(
	_ context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Traces,
) (processor.Traces, error) {
	return newTracesRetroactiveSamplingProcessor(set, nextConsumer, cfg.(*Config))
}
