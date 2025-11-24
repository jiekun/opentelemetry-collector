// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package remotesamplingexporter // import "go.opentelemetry.io/collector/exporter/remotesamplingexporter"

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/persistentqueue"
	"github.com/cespare/xxhash/v2"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/internal/statusutil"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/pdata/pprofile/pprofileotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"go.uber.org/zap"
	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/protobuf/proto"
)

var (
	writeRequestBufPool bytesutil.ByteBufferPool
)

type samplingRequest struct {
	samplingTraceList []samplingTrace `json:"sampling_trace_list"`
}

type samplingTrace struct {
	traceID    string `json:"trace_id"`
	startTime  uint64 `json:"start_time"`
	endTime    uint64 `json:"end_time"`
	statusCode int32  `json:"status_code"`
}

type samplingDecision struct {
	traceIDList []string `json:"trace_id_list"`
}

type remotesamplingExporter struct {
	// Input configuration.
	config      *Config
	client      *http.Client
	tracesURL   string
	metricsURL  string
	logsURL     string
	profilesURL string
	logger      *zap.Logger
	settings    component.TelemetrySettings
	// Default user-agent header.
	userAgent string

	// remote sampling related
	tracesSamplingURL     string
	fq                    *persistentqueue.FastQueue
	sampledTraceIDMapCur  *sync.Map
	sampledTraceIDMapPrev *sync.Map
}

const (
	headerRetryAfter         = "Retry-After"
	maxHTTPResponseReadBytes = 64 * 1024

	jsonContentType     = "application/json"
	protobufContentType = "application/x-protobuf"

	persistentQueueDirname = "persistent-queue"
)

// Create new exporter.
func newExporter(cfg component.Config, set exporter.Settings) (*remotesamplingExporter, error) {
	oCfg := cfg.(*Config)

	if oCfg.ClientConfig.Endpoint != "" {
		_, err := url.Parse(oCfg.ClientConfig.Endpoint)
		if err != nil {
			return nil, errors.New("endpoint must be a valid URL")
		}
	}

	if oCfg.TraceSamplingURL == "" {
		return nil, errors.New("TraceSamplingURL must be set")
	}
	if _, err := url.Parse(oCfg.TraceSamplingURL); err != nil {
		return nil, errors.New("trace_sampling_url must be a valid URL to the sampling server")
	}

	userAgent := fmt.Sprintf("%s/%s (%s/%s)",
		set.BuildInfo.Description, set.BuildInfo.Version, runtime.GOOS, runtime.GOARCH)

	// create persistent queue
	h := xxhash.Sum64([]byte(oCfg.ClientConfig.Endpoint))
	queuePath := filepath.Join(oCfg.TmpDataPath, persistentQueueDirname, fmt.Sprintf("%016X", h))

	fq := persistentqueue.MustOpenFastQueue(queuePath, oCfg.ClientConfig.Endpoint, 4, 0, false)

	// client construction is deferred to start
	return &remotesamplingExporter{
		config:            oCfg,
		logger:            set.Logger,
		userAgent:         userAgent,
		settings:          set.TelemetrySettings,
		tracesSamplingURL: oCfg.TraceSamplingURL,

		fq:                    fq,
		sampledTraceIDMapCur:  &sync.Map{},
		sampledTraceIDMapPrev: &sync.Map{},
	}, nil
}

// start actually creates the HTTP client. The client construction is deferred till this point as this
// is the only place we get hold of Extensions which are required to construct auth round tripper.
func (e *remotesamplingExporter) start(ctx context.Context, host component.Host) error {

	client, err := e.config.ClientConfig.ToClient(ctx, host, e.settings)
	if err != nil {
		return err
	}
	e.client = client

	return nil
}

func (e *remotesamplingExporter) pushTraces(ctx context.Context, td ptrace.Traces) error {
	exportTraceServiceRequest := ptraceotlp.NewExportRequestFromTraces(td)

	// construct sampling request
	sc := td.SpanCount()
	sr := samplingRequest{
		samplingTraceList: make([]samplingTrace, 0, sc),
	}
	traceIDMap := make(map[[16]byte]struct{})

	for i := 0; i < td.ResourceSpans().Len(); i++ {
		for j := 0; j < td.ResourceSpans().At(i).ScopeSpans().Len(); j++ {
			for k := 0; k < td.ResourceSpans().At(i).ScopeSpans().At(j).Spans().Len(); k++ {
				tid := td.ResourceSpans().At(i).ScopeSpans().At(j).Spans().At(k).TraceID()
				traceIDMap[tid] = struct{}{}
				sr.samplingTraceList = append(sr.samplingTraceList, samplingTrace{
					traceID:    tid.String(),
					startTime:  uint64(td.ResourceSpans().At(i).ScopeSpans().At(j).Spans().At(k).StartTimestamp()),
					endTime:    uint64(td.ResourceSpans().At(i).ScopeSpans().At(j).Spans().At(k).EndTimestamp()),
					statusCode: int32(td.ResourceSpans().At(i).ScopeSpans().At(j).Spans().At(k).Status().Code()),
				})
			}
		}
	}

	traceIDBuf := make([]byte, 0, len(traceIDMap)*16)
	for k := range traceIDMap {
		traceIDBuf = append(traceIDBuf, k[:]...)
	}

	// push trace to pq
	bb := writeRequestBufPool.Get()
	bb.B = bytesutil.ResizeNoCopyNoOverallocate(bb.B, 8+len(traceIDBuf)+exportTraceServiceRequest.SizeProto())
	// append current timestamp in seconds to the first 4 byte of the buffer
	binary.BigEndian.PutUint32(bb.B[0:4], uint32(time.Now().Unix()))
	// append int32 traceID count to 4 byte of the buffer after timestamp.
	binary.BigEndian.PutUint32(bb.B[4:8], uint32(sc))
	// append traceIDBuf to the buffer
	bb.Write(traceIDBuf)
	// finally, append export exportTraceServiceRequest to the rest buffer
	exportTraceServiceRequest.MarshalProtoTo(bb.B[8+len(traceIDBuf):])
	e.fq.TryWriteBlock(bb.B)
	writeRequestBufPool.Put(bb)

	srb, err := json.Marshal(sr)
	if err != nil {
		e.logger.Error("failed to marshal traces sampling request", zap.Error(err))
		return err
	}

	return e.exportSamplingRequest(ctx, e.tracesSamplingURL, srb)
}

func (e *remotesamplingExporter) export(ctx context.Context, url string, request []byte, partialSuccessHandler partialSuccessHandler) error {
	e.logger.Debug("Preparing to make HTTP request", zap.String("url", url))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(request))
	if err != nil {
		return consumererror.NewPermanent(err)
	}

	switch e.config.Encoding {
	case EncodingJSON:
		req.Header.Set("Content-Type", jsonContentType)
	case EncodingProto:
		req.Header.Set("Content-Type", protobufContentType)
	default:
		return fmt.Errorf("invalid encoding: %s", e.config.Encoding)
	}

	req.Header.Set("User-Agent", e.userAgent)

	resp, err := e.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to make an HTTP request: %w", err)
	}

	defer func() {
		// Discard any remaining response body when we are done reading.
		_, _ = io.CopyN(io.Discard, resp.Body, maxHTTPResponseReadBytes)
		resp.Body.Close()
	}()

	if resp.StatusCode >= 200 && resp.StatusCode <= 299 {
		return handlePartialSuccessResponse(resp, partialSuccessHandler)
	}

	respStatus := readResponseStatus(resp)

	// Format the error message. Use the status if it is present in the response.
	var errString string
	var formattedErr error
	if respStatus != nil {
		errString = fmt.Sprintf(
			"error exporting items, request to %s responded with HTTP Status Code %d, Message=%s, Details=%v",
			url, resp.StatusCode, respStatus.Message, respStatus.Details)
	} else {
		errString = fmt.Sprintf(
			"error exporting items, request to %s responded with HTTP Status Code %d",
			url, resp.StatusCode)
	}
	formattedErr = statusutil.NewStatusFromMsgAndHTTPCode(errString, resp.StatusCode).Err()

	if !isRetryableStatusCode(resp.StatusCode) {
		return consumererror.NewPermanent(formattedErr)
	}

	// Check if the server is overwhelmed.
	// See spec https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/protocol/otlp.md#otlphttp-throttling
	isThrottleError := resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusServiceUnavailable
	if isThrottleError {
		// Use Values to check if the header is present, and if present even if it is empty return ThrottleRetry.
		values := resp.Header.Values(headerRetryAfter)
		if len(values) == 0 {
			return formattedErr
		}
		// The value of Retry-After field can be either an HTTP-date or a number of
		// seconds to delay after the response is received. See https://datatracker.ietf.org/doc/html/rfc7231#section-7.1.3
		//
		// Retry-After = HTTP-date / delay-seconds
		//
		// First try to parse delay-seconds, since that is what the receiver will send.
		if seconds, err := strconv.Atoi(values[0]); err == nil {
			return exporterhelper.NewThrottleRetry(formattedErr, time.Duration(seconds)*time.Second)
		}
		if date, err := time.Parse(time.RFC1123, values[0]); err == nil {
			return exporterhelper.NewThrottleRetry(formattedErr, time.Until(date))
		}
	}
	return formattedErr
}

func (e *remotesamplingExporter) exportSamplingRequest(ctx context.Context, url string, request []byte) error {
	e.logger.Debug("Preparing to make HTTP request", zap.String("url", url))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(request))
	if err != nil {
		return consumererror.NewPermanent(err)
	}

	req.Header.Set("Content-Type", jsonContentType)
	req.Header.Set("User-Agent", e.userAgent)

	resp, err := e.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to make an HTTP request: %w", err)
	}

	defer func() {
		// Discard any remaining response body when we are done reading.
		_, _ = io.CopyN(io.Discard, resp.Body, maxHTTPResponseReadBytes)
		resp.Body.Close()
	}()

	if resp.StatusCode >= 200 && resp.StatusCode <= 299 {
		return nil
	}

	respStatus := readResponseStatus(resp)

	// Format the error message. Use the status if it is present in the response.
	var errString string
	if respStatus != nil {
		errString = fmt.Sprintf(
			"error exporting items, request to %s responded with HTTP Status Code %d, Message=%s, Details=%v",
			url, resp.StatusCode, respStatus.Message, respStatus.Details)
	} else {
		errString = fmt.Sprintf(
			"error exporting items, request to %s responded with HTTP Status Code %d",
			url, resp.StatusCode)
	}
	return statusutil.NewStatusFromMsgAndHTTPCode(errString, resp.StatusCode).Err()
}

func (e *remotesamplingExporter) pushMetrics(ctx context.Context, md pmetric.Metrics) error {
	tr := pmetricotlp.NewExportRequestFromMetrics(md)

	var err error
	var request []byte
	switch e.config.Encoding {
	case EncodingJSON:
		request, err = tr.MarshalJSON()
	case EncodingProto:
		request, err = tr.MarshalProto()
	default:
		err = fmt.Errorf("invalid encoding: %s", e.config.Encoding)
	}

	if err != nil {
		return consumererror.NewPermanent(err)
	}
	return e.export(ctx, e.metricsURL, request, e.metricsPartialSuccessHandler)
}

func (e *remotesamplingExporter) pushLogs(ctx context.Context, ld plog.Logs) error {
	tr := plogotlp.NewExportRequestFromLogs(ld)

	var err error
	var request []byte
	switch e.config.Encoding {
	case EncodingJSON:
		request, err = tr.MarshalJSON()
	case EncodingProto:
		request, err = tr.MarshalProto()
	default:
		err = fmt.Errorf("invalid encoding: %s", e.config.Encoding)
	}

	if err != nil {
		return consumererror.NewPermanent(err)
	}

	return e.export(ctx, e.logsURL, request, e.logsPartialSuccessHandler)
}

func (e *remotesamplingExporter) pushProfiles(ctx context.Context, td pprofile.Profiles) error {
	tr := pprofileotlp.NewExportRequestFromProfiles(td)

	var err error
	var request []byte
	switch e.config.Encoding {
	case EncodingJSON:
		request, err = tr.MarshalJSON()
	case EncodingProto:
		request, err = tr.MarshalProto()
	default:
		err = fmt.Errorf("invalid encoding: %s", e.config.Encoding)
	}

	if err != nil {
		return consumererror.NewPermanent(err)
	}

	return e.export(ctx, e.profilesURL, request, e.profilesPartialSuccessHandler)
}

func (e *remotesamplingExporter) startDecisionCleaner() {
	go func() {
		ticker := time.NewTicker(150 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				e.sampledTraceIDMapPrev.Clear()
				e.sampledTraceIDMapCur, e.sampledTraceIDMapPrev = e.sampledTraceIDMapPrev, e.sampledTraceIDMapCur
			}
		}
	}()
}

// startBufferExporter starts a consumeExportTraceRequest for exporting traces.
// it consumes data from the persistent queue, and sends the *ExportTraceServiceRequest to destination if this
// *ExportTraceServiceRequest contains sampled traces.
func (e *remotesamplingExporter) startBufferExporter() {
	// start 4 workers
	for i := 0; i < 4; i++ {
		go func() {
			for {
				e.consumeExportTraceRequest()
			}
		}()
	}
}

func (e *remotesamplingExporter) consumeExportTraceRequest() {
	var ok bool
	bb := writeRequestBufPool.Get()
	defer writeRequestBufPool.Put(bb)

	bb.B, ok = e.fq.MustReadBlock(bb.B)
	if !ok {
		return
	}
	if len(bb.B) < 8 {
		panic(fmt.Sprintf("BUG: too writeRequest buf for remote sampling: %v", bb.B))
	}

	// read timestamp, and block the goroutine if the wait duration is not met *decisionWait
	addTimestamp := time.Unix(int64(binary.BigEndian.Uint32(bb.B[0:4])), 0)
	shouldWaitDuration := e.config.DecisionWait - time.Since(addTimestamp)
	if shouldWaitDuration > 0 {
		time.Sleep(shouldWaitDuration)
	}

	// read the length of trace ID.
	traceIDCount := binary.BigEndian.Uint32(bb.B[4:8])
	if traceIDCount == 0 {
		return
	}

	// read trace IDs
	isSampled := false
	for idx := 8; idx < 8+int(traceIDCount)*16; idx += 16 {
		traceIDBytes := bb.B[idx : idx+16]
		traceID := hex.EncodeToString(traceIDBytes)
		if _, exist := e.sampledTraceIDMapCur.Load(traceID); exist {
			isSampled = true
			break
		}
		if _, exist := e.sampledTraceIDMapPrev.Load(traceID); exist {
			isSampled = true
			break
		}
	}

	if !isSampled {
		return
	}

	// send to destination
	if err := e.export(context.TODO(), e.tracesURL, bb.B[8+int(traceIDCount)*16:], e.tracesPartialSuccessHandler); err != nil {
		e.logger.Error("failed to export traces", zap.Error(err))
	}
}

// startSamplingDecisionReceiver starts an HTTP server to receive sampling decisions.
// The sampling decisions will be stored as in-memory cache and rotate after 5 minutes.
func (e *remotesamplingExporter) startSamplingDecisionReceiver() {
	// create HTTP server to receive remote sampling decisions.
	http.HandleFunc("/api/v1/remotesampling_decision", func(w http.ResponseWriter, req *http.Request) {
		decision := &samplingDecision{}
		body, err := io.ReadAll(req.Body)
		if err != nil {
			e.logger.Error("error reading body", zap.Error(err))
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		err = json.Unmarshal(body, decision)
		if err != nil {
			e.logger.Error("error unmarshaling body", zap.Error(err))
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		for i := range decision.traceIDList {
			e.sampledTraceIDMapCur.LoadOrStore(decision.traceIDList[i], struct{}{})
		}
		w.WriteHeader(http.StatusOK)
		return
	})

	go func() {
		if err := http.ListenAndServe("0.0.0.0:10429", nil); err != nil {
			e.logger.Fatal("failed to start HTTP server", zap.Error(err))
		}
	}()
}

// Determine if the status code is retryable according to the specification.
// For more, see https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/protocol/otlp.md#failures-1
func isRetryableStatusCode(code int) bool {
	switch code {
	case http.StatusTooManyRequests:
		return true
	case http.StatusBadGateway:
		return true
	case http.StatusServiceUnavailable:
		return true
	case http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

func readResponseBody(resp *http.Response) ([]byte, error) {
	if resp.ContentLength == 0 {
		return nil, nil
	}

	maxRead := resp.ContentLength

	// if maxRead == -1, the ContentLength header has not been sent, so read up to
	// the maximum permitted body size. If it is larger than the permitted body
	// size, still try to read from the body in case the value is an error. If the
	// body is larger than the maximum size, proto unmarshaling will likely fail.
	if maxRead == -1 || maxRead > maxHTTPResponseReadBytes {
		maxRead = maxHTTPResponseReadBytes
	}
	protoBytes := make([]byte, maxRead)
	n, err := io.ReadFull(resp.Body, protoBytes)

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

	return protoBytes[:n], nil
}

// Read the response and decode the status.Status from the body.
// Returns nil if the response is empty or cannot be decoded.
func readResponseStatus(resp *http.Response) *status.Status {
	var respStatus *status.Status
	if resp.StatusCode >= 400 && resp.StatusCode <= 599 {
		// Request failed. Read the body. OTLP spec says:
		// "Response body for all HTTP 4xx and HTTP 5xx responses MUST be a
		// Protobuf-encoded Status message that describes the problem."
		respBytes, err := readResponseBody(resp)
		if err != nil {
			return nil
		}

		// Decode it as Status struct. See https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/protocol/otlp.md#failures
		respStatus = &status.Status{}
		err = proto.Unmarshal(respBytes, respStatus)
		if err != nil {
			return nil
		}
	}

	return respStatus
}

func handlePartialSuccessResponse(resp *http.Response, partialSuccessHandler partialSuccessHandler) error {
	bodyBytes, err := readResponseBody(resp)
	if err != nil {
		return err
	}

	return partialSuccessHandler(bodyBytes, resp.Header.Get("Content-Type"))
}

type partialSuccessHandler func(bytes []byte, contentType string) error

func (e *remotesamplingExporter) tracesPartialSuccessHandler(protoBytes []byte, contentType string) error {
	if protoBytes == nil {
		return nil
	}
	exportResponse := ptraceotlp.NewExportResponse()
	switch contentType {
	case protobufContentType:
		err := exportResponse.UnmarshalProto(protoBytes)
		if err != nil {
			return fmt.Errorf("error parsing protobuf response: %w", err)
		}
	case jsonContentType:
		err := exportResponse.UnmarshalJSON(protoBytes)
		if err != nil {
			return fmt.Errorf("error parsing json response: %w", err)
		}
	default:
		return nil
	}

	partialSuccess := exportResponse.PartialSuccess()
	if partialSuccess.ErrorMessage() != "" || partialSuccess.RejectedSpans() != 0 {
		e.logger.Warn("Partial success response",
			zap.String("message", exportResponse.PartialSuccess().ErrorMessage()),
			zap.Int64("dropped_spans", exportResponse.PartialSuccess().RejectedSpans()),
		)
	}
	return nil
}

func (e *remotesamplingExporter) metricsPartialSuccessHandler(protoBytes []byte, contentType string) error {
	if protoBytes == nil {
		return nil
	}
	exportResponse := pmetricotlp.NewExportResponse()
	switch contentType {
	case protobufContentType:
		err := exportResponse.UnmarshalProto(protoBytes)
		if err != nil {
			return fmt.Errorf("error parsing protobuf response: %w", err)
		}
	case jsonContentType:
		err := exportResponse.UnmarshalJSON(protoBytes)
		if err != nil {
			return fmt.Errorf("error parsing json response: %w", err)
		}
	default:
		return nil
	}

	partialSuccess := exportResponse.PartialSuccess()
	if partialSuccess.ErrorMessage() != "" || partialSuccess.RejectedDataPoints() != 0 {
		e.logger.Warn("Partial success response",
			zap.String("message", exportResponse.PartialSuccess().ErrorMessage()),
			zap.Int64("dropped_data_points", exportResponse.PartialSuccess().RejectedDataPoints()),
		)
	}
	return nil
}

func (e *remotesamplingExporter) logsPartialSuccessHandler(protoBytes []byte, contentType string) error {
	if protoBytes == nil {
		return nil
	}
	exportResponse := plogotlp.NewExportResponse()
	switch contentType {
	case protobufContentType:
		err := exportResponse.UnmarshalProto(protoBytes)
		if err != nil {
			return fmt.Errorf("error parsing protobuf response: %w", err)
		}
	case jsonContentType:
		err := exportResponse.UnmarshalJSON(protoBytes)
		if err != nil {
			return fmt.Errorf("error parsing json response: %w", err)
		}
	default:
		return nil
	}

	partialSuccess := exportResponse.PartialSuccess()
	if partialSuccess.ErrorMessage() != "" || partialSuccess.RejectedLogRecords() != 0 {
		e.logger.Warn("Partial success response",
			zap.String("message", exportResponse.PartialSuccess().ErrorMessage()),
			zap.Int64("dropped_log_records", exportResponse.PartialSuccess().RejectedLogRecords()),
		)
	}
	return nil
}

func (e *remotesamplingExporter) profilesPartialSuccessHandler(protoBytes []byte, contentType string) error {
	if protoBytes == nil {
		return nil
	}
	exportResponse := pprofileotlp.NewExportResponse()
	switch contentType {
	case protobufContentType:
		err := exportResponse.UnmarshalProto(protoBytes)
		if err != nil {
			return fmt.Errorf("error parsing protobuf response: %w", err)
		}
	case jsonContentType:
		err := exportResponse.UnmarshalJSON(protoBytes)
		if err != nil {
			return fmt.Errorf("error parsing json response: %w", err)
		}
	default:
		return nil
	}

	partialSuccess := exportResponse.PartialSuccess()
	if partialSuccess.ErrorMessage() != "" || partialSuccess.RejectedProfiles() != 0 {
		e.logger.Warn("Partial success response",
			zap.String("message", exportResponse.PartialSuccess().ErrorMessage()),
			zap.Int64("dropped_samples", exportResponse.PartialSuccess().RejectedProfiles()),
		)
	}
	return nil
}
