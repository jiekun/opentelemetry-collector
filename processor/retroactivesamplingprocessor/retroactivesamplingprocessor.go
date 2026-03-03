// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package retroactivesamplingprocessor // import "go.opentelemetry.io/collector/processor/retroactivesamplingprocessor"

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"path/filepath"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/cgroup"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding/zstd"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/persistentqueue"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/procutil"
	"github.com/cespare/xxhash/v2"
	"go.uber.org/zap"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"go.opentelemetry.io/collector/processor"
)

const (
	persistentQueueDirname = "persistent-queue"
)

var ptraceBufPool bytesutil.ByteBufferPool

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
			exportURL:   cfg.RetroactiveSamplingURLs[i],
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
	rh := func(w http.ResponseWriter, r *http.Request) bool {
		switch r.URL.Path {
		case "/api/v1/remotesampling_decision":
			b, err := io.ReadAll(r.Body)
			if err != nil {
				logger.Errorf("cannot read body: %s", err)
				return false
			}
			bb := make([]byte, 0, len(b))
			bb, err = zstd.Decompress(bb, b)
			if err != nil {
				logger.Errorf("cannot decompress body: %s", err)
				return false
			}

			if len(bb) < 4 || (len(bb)-4)%16 != 0 {
				logger.Errorf("unexpected length of bb: %d", len(bb))
				return false
			}

			// read first 4 bytes as length
			traceIDCount := binary.BigEndian.Uint32(bb[:4])

			// verify
			if len(bb) != int(traceIDCount*16+4) {
				t.logger.Error("incorrect byteBuf length", zap.Uint32("expect", traceIDCount*16+4), zap.Int("len(byteBuf)", len(bb)))
				return false
			}

			for j := 4; j < len(bb); j += 16 {
				tb := bb[j : j+16]
				s := t.shards[int(tb[3]+tb[7]+tb[11]+tb[15])%len(t.shards)]
				s.mu.Lock()
				tid := [16]byte{}
				copy(tid[:], tb)
				s.traceIDIndexMapCur[tid] = struct{}{}
				s.mu.Unlock()
			}

			return true
		}
		return false
	}
	go httpserver.Serve([]string{"0.0.0.0:10499"}, rh, httpserver.ServeOptions{})
	sig := procutil.WaitForSigterm()
	logger.Infof("received signal %s, exit retroactive sampling server", sig)
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

					//if sc.pendingBuf.Len() < 200*1024 {
					//	// let's wait for the next round. Do worry about the pending data as this is a benchmark PoC.
					//	sc.mu.Unlock()
					//	continue
					//}

					// do request
					if sc.pendingBuf.Len() <= 0 {
						sc.mu.Unlock()
						continue
					}

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
	req, err := http.NewRequest(http.MethodPost, sc.exportURL, bb.NewReader())
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Encoding", "zstd")
	req.Header.Set("User-Agent", "retroactivesamplingprocessor/0.1")
	req.Header.Set("Sampling-Agent-ID", sc.cfg.SamplingAgentID)

	resp, err := sc.client.Do(req)
	if err != nil {
		return fmt.Errorf("unexpected err: %v", err)
	}
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

				sc.pendingBuf.Write(decisionNotSampled)
				sc.pendingBuf.Write(tb[:])

				sc.pendingBuf.B = binary.BigEndian.AppendUint64(sc.pendingBuf.B, uint64(td.ResourceSpans().At(i).ScopeSpans().At(j).Spans().At(k).StartTimestamp()))
				sc.pendingBuf.B = binary.BigEndian.AppendUint64(sc.pendingBuf.B, uint64(td.ResourceSpans().At(i).ScopeSpans().At(j).Spans().At(k).EndTimestamp()))
				sc.pendingBuf.B = append(sc.pendingBuf.B, byte(int8(td.ResourceSpans().At(i).ScopeSpans().At(j).Spans().At(k).Status().Code())))

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
			continue
		}
		ptraceBufPool.Put(bb)

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

					// probability sampler: 1%
					if xxhash.Sum64(tb[:])%100 < t.cfg.SamplingRate {
						return false
					}

					return true
				})
			}
		}

		if td.SpanCount() == 0 {
			continue
		}

		t.nextConsumer.ConsumeTraces(context.TODO(), td)
	}
}
