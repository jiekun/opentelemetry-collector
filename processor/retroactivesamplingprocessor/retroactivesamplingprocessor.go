// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package retroactivesamplingprocessor // import "go.opentelemetry.io/collector/processor/retroactivesamplingprocessor"

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/cgroup"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding/zstd"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/persistentqueue"
	"github.com/cespare/xxhash/v2"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
	"io"
	"math/rand"
	"net/http"
	"path/filepath"
	"sync"
	"time"
)

const (
	persistentQueueDirname = "persistent-queue"
)

var (
	ptraceBufPool bytesutil.ByteBufferPool
)

type tracesRetroactiveSamplingProcessor struct {
	cfg          *Config
	logger       *zap.Logger
	nextConsumer consumer.Traces

	// each sampling server has a dedicate client
	samplingClients []*samplingClient
	shards          []*consumerShard
	fq              *persistentqueue.FastQueue

	marshaler   *ptrace.ProtoMarshaler
	unmarshaler *ptrace.ProtoUnmarshaler
	wg          sync.WaitGroup
}

// samplingClient batches data that required by sampling, and sends request to remote server.
// the data must be hashed and distributed to corresponding worker by trace ID.
type samplingClient struct {
	cfg *Config

	mu          *sync.Mutex
	exportURL   string
	decisionURL string
	pendingBuf  *bytesutil.ByteBuffer
	client      *http.Client
}

type consumerShard struct {
	mu sync.Mutex

	traceIDIndexMapCur  map[[16]byte]struct{}
	traceIDIndexMapPrev map[[16]byte]struct{}
}

// newTracesRetroactiveSamplingProcessor creates a new batch processor that batches traces by size or with timeout
func newTracesRetroactiveSamplingProcessor(set processor.Settings, next consumer.Traces, cfg *Config) (processor.Traces, error) {
	if cfg.SamplingAgentID == "" {
		cfg.SamplingAgentID = fmt.Sprintf("%d:%d", time.Now().UnixNano(), rand.Intn(1024))
		set.Logger.Info("SamplingAgentID not set. generating a random one", zap.String("SamplingAgentID", cfg.SamplingAgentID))
	}

	// create persistent queue
	h := xxhash.Sum64([]byte(cfg.SamplingAgentID))
	queuePath := filepath.Join(cfg.TmpDataPath, persistentQueueDirname, fmt.Sprintf("%016X", h))
	fq := persistentqueue.MustOpenFastQueue(queuePath, cfg.SamplingAgentID, 1, 0, false)

	samplingClients := make([]*samplingClient, len(cfg.RetroactiveSamplingURLs))
	for i := 0; i < len(cfg.RetroactiveSamplingURLs); i++ {
		samplingClients[i] = &samplingClient{
			cfg:         cfg,
			mu:          &sync.Mutex{},
			exportURL:   cfg.RetroactiveSamplingURLs[i] + "/api/v1/export_spans",
			decisionURL: cfg.RetroactiveSamplingURLs[i] + "/api/v1/sampled_traces",
			pendingBuf:  &bytesutil.ByteBuffer{},
			client:      &http.Client{},
		}
	}

	shards := make([]*consumerShard, cgroup.AvailableCPUs())
	for i := 0; i < len(shards); i++ {
		shards[i] = &consumerShard{
			mu:                  sync.Mutex{},
			traceIDIndexMapCur:  make(map[[16]byte]struct{}),
			traceIDIndexMapPrev: make(map[[16]byte]struct{}),
		}
	}
	return &tracesRetroactiveSamplingProcessor{
		cfg:          cfg,
		logger:       set.Logger,
		nextConsumer: next,

		samplingClients: samplingClients,
		shards:          shards,
		fq:              fq,

		marshaler: &ptrace.ProtoMarshaler{},

		wg: sync.WaitGroup{},
	}, nil
}

// Start is invoked during service startup.
func (t *tracesRetroactiveSamplingProcessor) Start(ctx context.Context, _ component.Host) error {
	t.mustStartFastQueueConsumer()
	t.mustStartSamplingClientsFlusher()
	go t.mustStartSamplingDecisionReceiver()
	return nil
}

// Shutdown is invoked during service shutdown.
func (t *tracesRetroactiveSamplingProcessor) Shutdown(context.Context) error {
	return nil
}

func (t *tracesRetroactiveSamplingProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

// startSamplingDecisionReceiver starts an background task to request for sampling decisions from servers.
// The sampling decisions will be stored as in-memory cache and rotate after 2 minutes.
func (t *tracesRetroactiveSamplingProcessor) mustStartSamplingDecisionReceiver() {
	// create HTTP server to receive remote sampling decisions.
	// run N clients according to the remote URLs.
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			var byteBuf []byte
			for i := 0; i < len(t.samplingClients); i++ {
				byteBuf = byteBuf[:0]
				sc := t.samplingClients[i]

				req, err := http.NewRequest("GET", sc.decisionURL, nil)
				if err != nil {
					t.logger.Error("Error creating request", zap.String("decisionURL", sc.decisionURL), zap.Error(err))
					continue
				}

				req.Header.Set("User-Agent", "retroactivesamplingprocessor/0.1")
				req.Header.Set("Sampling-Agent-ID", sc.cfg.SamplingAgentID)
				t.logger.Debug("Sending request", zap.String("decisionURL", sc.decisionURL))
				resp, err := sc.client.Do(req)
				if err != nil {
					t.logger.Error("get decision error", zap.String("decisionURL", sc.decisionURL), zap.Error(err))
					continue
				}

				if resp.StatusCode/100 != 2 {
					t.logger.Error("unexpected status code", zap.String("decisionURL", sc.decisionURL), zap.Int("statusCode", resp.StatusCode))
					continue
				}

				respBytes, err := readResponseBody(resp)
				if err != nil {
					t.logger.Error("unexpected error reading response body", zap.String("decisionURL", sc.decisionURL), zap.Error(err))
					continue
				}

				byteBuf, err = zstd.Decompress(byteBuf, respBytes)
				if err != nil {
					t.logger.Error("unexpected error decompressing response", zap.String("decisionURL", sc.decisionURL), zap.Error(err))
					continue
				}

				if len(byteBuf) < 4 {
					t.logger.Error("unexpected length in response", zap.String("decisionURL", sc.decisionURL), zap.Int("len(byteBuf)", len(byteBuf)))
					continue
				}

				// read first 4 bytes as length
				traceIDCount := binary.BigEndian.Uint32(byteBuf[:4])
				// verify
				if len(byteBuf) != int(traceIDCount*16+4) {
					t.logger.Error("incorrect byteBuf length", zap.String("decisionURL", sc.decisionURL), zap.Uint32("expect", traceIDCount*16+4), zap.Int("len(byteBuf)", len(byteBuf)))
					continue
				}

				for j := 4; j < len(byteBuf); j += 16 {
					tb := byteBuf[j : j+16]
					s := t.shards[int(tb[3]+tb[7]+tb[11]+tb[15])%len(t.shards)]
					s.mu.Lock()
					tid := [16]byte{}
					copy(tid[:], tb)
					s.traceIDIndexMapCur[tid] = struct{}{}
					s.mu.Unlock()
				}
			}
		}
	}
}

func (t *tracesRetroactiveSamplingProcessor) startDecisionCleaner() {
	go func() {
		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				for i := 0; i < len(t.shards); i++ {
					s := t.shards[i]
					s.mu.Lock()

					// drop the previous map and create a new one
					n := len(s.traceIDIndexMapPrev)
					s.traceIDIndexMapPrev = make(map[[16]byte]struct{}, n)

					// swap the previous map and current map
					s.traceIDIndexMapCur, s.traceIDIndexMapPrev = s.traceIDIndexMapPrev, s.traceIDIndexMapCur

					s.mu.Unlock()
				}
			}
		}
	}()
}

func readResponseBody(resp *http.Response) ([]byte, error) {
	if resp.ContentLength == 0 {
		return nil, nil
	}

	maxRead := resp.ContentLength
	respBytes := make([]byte, maxRead)
	n, err := io.ReadFull(resp.Body, respBytes)

	// No bytes read and an EOF error indicates there is no body to read.
	if n == 0 && (err == nil || errors.Is(err, io.EOF)) {
		return nil, nil
	}

	// io.ReadFull will return io.ErrorUnexpectedEOF if the Content-Length header
	// wasn't set, since we will try to read past the length of the body. If this
	// is the case, the body will still have the full message in it, so we want to
	// ignore the error and parse the message.
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, err
	}

	return respBytes[:n], nil
}

func (t *tracesRetroactiveSamplingProcessor) mustStartSamplingClientsFlusher() {
	// run N clients according to the remote URLs.
	for i := 0; i < len(t.samplingClients); i++ {
		go func() {
			sc := t.samplingClients[i]

			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-ticker.C:
					sc.mu.Lock()

					if sc.pendingBuf.Len() < 200*1024 {
						// let's wait for the next round. Do worry about the pending data as this is a benchmark PoC.
						continue
					}

					// do request
					err := sc.doFlushSamplingBuf()
					if err != nil {
						t.logger.Error("Flushing sampling buffer failed", zap.Error(err))
					}
					sc.pendingBuf.Reset()
					sc.mu.Unlock()
				}
			}
		}()
	}
}

var zstdBufPool bytesutil.ByteBufferPool

func (sc *samplingClient) doFlushSamplingBuf() error {
	bb := zstdBufPool.Get()
	defer zstdBufPool.Put(bb)

	bb.B = zstd.CompressLevel(bb.B[:0], sc.pendingBuf.B, 1)
	req, err := http.NewRequest("POST", sc.exportURL, bb.NewReader())
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Encoding", "zstd")
	req.Header.Set("User-Agent", "retroactivesamplingprocessor/0.1")
	req.Header.Set("Sampling-Agent-ID", sc.cfg.SamplingAgentID)

	resp, err := sc.client.Do(req)
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	return nil
}

var (
	decisionSampled    = []byte{1}
	decisionNotSampled = []byte{0}
)

func (t *tracesRetroactiveSamplingProcessor) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		for j := 0; j < td.ResourceSpans().At(i).ScopeSpans().Len(); j++ {
			for k := 0; k < td.ResourceSpans().At(i).ScopeSpans().At(j).Spans().Len(); k++ {
				// make sure same traceID goes to same client
				tb := td.ResourceSpans().At(i).ScopeSpans().At(j).Spans().At(k).TraceID()
				sc := t.samplingClients[int(tb[3]+tb[7]+tb[11]+tb[15])%len(t.samplingClients)]

				sc.mu.Lock()

				// samplingBuf is the metadata extracted from each span.
				// it should be marshaled into:
				// [<decision>|<traceID>|<startTime>|<endTime>|<statusCode>] in bytes request.
				// [1 byte    |16 bytes |8 bytes    |8 bytes  |1 byte      ] = 34 bytes
				// If decision is true, then it contains only [<decision>|<traceID>] as the rest are not useful anymore.

				sc.pendingBuf.Write(decisionSampled)
				sc.pendingBuf.Write(tb[:])

				binary.BigEndian.AppendUint64(sc.pendingBuf.B, uint64(td.ResourceSpans().At(i).ScopeSpans().At(j).Spans().At(k).StartTimestamp()))
				binary.BigEndian.AppendUint64(sc.pendingBuf.B, uint64(td.ResourceSpans().At(i).ScopeSpans().At(j).Spans().At(k).EndTimestamp()))
				sc.pendingBuf.B = append(sc.pendingBuf.B, byte(td.ResourceSpans().At(i).ScopeSpans().At(j).Spans().At(k).Status().Code()))

				// flush on 1 MB
				if sc.pendingBuf.Len() >= 1*1024*1024 {
					// todo flush, otherwise waiting for per second flush
					if err := sc.doFlushSamplingBuf(); err != nil {
						t.logger.Error("Flushing sampling buffer failed", zap.Error(err))
					}
				}
				sc.mu.Unlock()
			}
		}
	}

	exportTraceServiceRequest := ptraceotlp.NewExportRequestFromTraces(td)

	// push trace to pq
	bb := ptraceBufPool.Get()
	bb.B = bytesutil.ResizeNoCopyNoOverallocate(bb.B, 4+exportTraceServiceRequest.SizeProto())
	// append current timestamp in seconds to the first 4 byte of the buffer
	binary.BigEndian.PutUint32(bb.B[0:4], uint32(time.Now().Unix()))
	// finally, append export exportTraceServiceRequest to the rest buffer
	exportTraceServiceRequest.MarshalProtoTo(bb.B[4:len(bb.B)])
	t.fq.TryWriteBlock(bb.B)
	ptraceBufPool.Put(bb)

	return nil
}

func (t *tracesRetroactiveSamplingProcessor) mustStartFastQueueConsumer() {
	// run N (CPU) consumer.
	for i := 0; i < len(t.shards); i++ {
		t.wg.Add(1)
		go func() {
			defer t.wg.Done()
			t.consumeFastQueue()
		}()
	}
}

func (t *tracesRetroactiveSamplingProcessor) consumeFastQueue() {
	for {
		var ok bool

		bb := ptraceBufPool.Get()
		bb.B, ok = t.fq.MustReadBlock(bb.B)
		if !ok {
			return
		}
		if len(bb.B) < 8 {
			panic(fmt.Sprintf("BUG: too writeRequest buf for remote sampling: %v", bb.B))
		}

		// read timestamp, and block the goroutine if the wait duration is not met *decisionWait
		addTimestamp := time.Unix(int64(binary.BigEndian.Uint32(bb.B[0:4])), 0)
		shouldWaitDuration := t.cfg.DecisionWait - time.Since(addTimestamp)
		if shouldWaitDuration > 0 {
			time.Sleep(shouldWaitDuration)
		}

		er := ptraceotlp.NewExportRequest()
		err := er.UnmarshalProto(bb.B[4:])
		if err != nil {
			t.logger.Error("failed to unmarshal trace from disk queue", zap.Error(err))
			return
		}
		ptraceBufPool.Put(bb)

		dropSpanCnt := 0

		// read trace IDs
		td := er.Traces()
		for i := 0; i < td.ResourceSpans().Len(); i++ {
			for j := 0; j < td.ResourceSpans().At(i).ScopeSpans().Len(); j++ {
				td.ResourceSpans().At(i).ScopeSpans().At(j).Spans().RemoveIf(func(s ptrace.Span) bool {
					tb := s.TraceID()
					resultShard := t.shards[int(tb[3]+tb[7]+tb[11]+tb[15])%len(t.shards)]

					// be careful playing with the mutex.
					resultShard.mu.Lock()
					if _, exist := resultShard.traceIDIndexMapCur[s.TraceID()]; exist {
						resultShard.mu.Unlock()
						return false
					}
					if _, exist := resultShard.traceIDIndexMapPrev[s.TraceID()]; exist {
						resultShard.mu.Unlock()
						return false
					}
					resultShard.mu.Unlock()

					// probability sampler: 1% TODO config
					if xxhash.Sum64(tb[:])%100 < 1 {
						return false
					}

					dropSpanCnt++
					return true
				})
			}
		}

		if td.SpanCount() == 0 {
			return
		}

		t.nextConsumer.ConsumeTraces(context.TODO(), td)
	}
}
