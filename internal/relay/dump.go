package relay

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type dumpMode string

const (
	dumpModeError dumpMode = "error"
	dumpModeAll   dumpMode = "all"
)

type relayDumpConfig struct {
	Enabled        bool
	Mode           dumpMode
	Dir            string
	MaxBodyBytes   int
	MaxStreamBytes int
	Redact         bool
}

func loadRelayDumpConfig() relayDumpConfig {
	cfg := relayDumpConfig{
		Enabled:        strings.EqualFold(os.Getenv("OCTOPUS_RELAY_DUMP_ENABLED"), "true"),
		Mode:           dumpModeError,
		Dir:            strings.TrimSpace(os.Getenv("OCTOPUS_RELAY_DUMP_DIR")),
		MaxBodyBytes:   64 * 1024,
		MaxStreamBytes: 64 * 1024,
		Redact:         true,
	}
	if cfg.Dir == "" {
		// Default to the same "data" directory used by the app config/db. In Docker this
		// maps to /app/data via volume.
		cfg.Dir = filepath.Join("data", "debug-dumps")
	}
	if v := strings.TrimSpace(os.Getenv("OCTOPUS_RELAY_DUMP_MODE")); v != "" {
		switch dumpMode(strings.ToLower(v)) {
		case dumpModeError, dumpModeAll:
			cfg.Mode = dumpMode(strings.ToLower(v))
		}
	}
	if v := strings.TrimSpace(os.Getenv("OCTOPUS_RELAY_DUMP_MAX_BODY_BYTES")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.MaxBodyBytes = n
		}
	}
	if v := strings.TrimSpace(os.Getenv("OCTOPUS_RELAY_DUMP_MAX_STREAM_BYTES")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.MaxStreamBytes = n
		}
	}
	if v := strings.TrimSpace(os.Getenv("OCTOPUS_RELAY_DUMP_REDACT")); v != "" {
		cfg.Redact = strings.EqualFold(v, "true")
	}
	return cfg
}

type relayDump struct {
	cfg relayDumpConfig

	dumpID  string
	startAt time.Time

	inbound  dumpHTTP
	outbound dumpHTTP
	upstream dumpHTTPResponse

	err string

	// Streaming capture (best-effort): keep only the first N bytes of upstream SSE event data.
	streamCapturedBytes int
	streamTruncated     bool
	streamBuf           bytes.Buffer
}

type dumpHTTP struct {
	Method  string            `json:"method,omitempty"`
	URL     string            `json:"url,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
	Body    dumpBody          `json:"body,omitempty"`
}

type dumpHTTPResponse struct {
	StatusCode int               `json:"status_code,omitempty"`
	Headers    map[string]string `json:"headers,omitempty"`
	Body       dumpBody          `json:"body,omitempty"`
}

type dumpBody struct {
	Encoding  string `json:"encoding,omitempty"` // "utf-8"
	Truncated bool   `json:"truncated,omitempty"`
	Bytes     int    `json:"bytes,omitempty"`
	Text      string `json:"text,omitempty"`
}

func newRelayDump(cfg relayDumpConfig) *relayDump {
	if !cfg.Enabled {
		return nil
	}
	return &relayDump{
		cfg:     cfg,
		dumpID:  generateDumpID(),
		startAt: time.Now(),
	}
}

func generateDumpID() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return time.Now().UTC().Format("20060102T150405.000Z0700") + "-" + hex.EncodeToString(b)
}

func (d *relayDump) shouldWrite(success bool) bool {
	if d == nil || !d.cfg.Enabled {
		return false
	}
	switch d.cfg.Mode {
	case dumpModeAll:
		return true
	case dumpModeError:
		return !success
	default:
		return !success
	}
}

func (d *relayDump) setInbound(req *http.Request, rawBody []byte) {
	if d == nil {
		return
	}
	d.inbound.Method = req.Method
	if req.URL != nil {
		d.inbound.URL = req.URL.String()
	}
	d.inbound.Headers = redactHeaders(req.Header, d.cfg.Redact)
	d.inbound.Body = makeDumpBody(rawBody, d.cfg.MaxBodyBytes, d.cfg.Redact)
}

func (d *relayDump) setOutbound(req *http.Request, body []byte) {
	if d == nil {
		return
	}
	d.outbound.Method = req.Method
	if req.URL != nil {
		d.outbound.URL = req.URL.String()
	}
	d.outbound.Headers = redactHeaders(req.Header, d.cfg.Redact)
	d.outbound.Body = makeDumpBody(body, d.cfg.MaxBodyBytes, d.cfg.Redact)
}

func (d *relayDump) setUpstream(resp *http.Response, body []byte) {
	if d == nil {
		return
	}
	d.upstream.StatusCode = resp.StatusCode
	d.upstream.Headers = redactHeaders(resp.Header, d.cfg.Redact)
	d.upstream.Body = makeDumpBody(body, d.cfg.MaxBodyBytes, d.cfg.Redact)
}

func (d *relayDump) appendUpstreamStreamData(s string) {
	if d == nil || d.cfg.MaxStreamBytes <= 0 || d.streamTruncated {
		return
	}
	// We only capture event "data" payloads (not full SSE framing) to keep it simple.
	// Add a delimiter between events for readability.
	if d.streamBuf.Len() > 0 {
		d.streamBuf.WriteString("\n\n")
		d.streamCapturedBytes += 2
	}

	b := []byte(s)
	remain := d.cfg.MaxStreamBytes - d.streamCapturedBytes
	if remain <= 0 {
		d.streamTruncated = true
		d.upstream.Body = dumpBody{
			Encoding:  "utf-8",
			Truncated: true,
			Bytes:     d.streamCapturedBytes,
			Text:      d.streamBuf.String(),
		}
		return
	}
	if len(b) > remain {
		b = b[:remain]
		d.streamTruncated = true
	}
	d.streamBuf.Write(b)
	d.streamCapturedBytes += len(b)

	d.upstream.Body = dumpBody{
		Encoding:  "utf-8",
		Truncated: d.streamTruncated,
		Bytes:     d.streamCapturedBytes,
		Text:      d.streamBuf.String(),
	}
}

func (d *relayDump) setError(err error) {
	if d == nil || err == nil {
		return
	}
	d.err = err.Error()
}

func (d *relayDump) write(success bool) {
	if d == nil || !d.shouldWrite(success) {
		return
	}
	_ = os.MkdirAll(d.cfg.Dir, 0o755)

	out := map[string]any{
		"dump_id":  d.dumpID,
		"start_at": d.startAt.Format(time.RFC3339Nano),
		"mode":     string(d.cfg.Mode),
		"inbound":  d.inbound,
		"outbound": d.outbound,
		"upstream": d.upstream,
	}
	if d.err != "" {
		out["error"] = d.err
	}

	b, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return
	}
	_ = os.WriteFile(filepath.Join(d.cfg.Dir, d.dumpID+".json"), b, 0o644)
}

func redactHeaders(h http.Header, enabled bool) map[string]string {
	if h == nil {
		return nil
	}
	out := make(map[string]string, len(h))
	for k, v := range h {
		lk := strings.ToLower(k)
		switch lk {
		case "authorization", "x-api-key", "cookie", "set-cookie":
			if enabled {
				out[k] = "***REDACTED***"
			} else {
				out[k] = strings.Join(v, ",")
			}
		default:
			out[k] = strings.Join(v, ",")
		}
	}
	return out
}

func makeDumpBody(raw []byte, max int, redact bool) dumpBody {
	if raw == nil {
		return dumpBody{}
	}

	orig := raw
	truncated := false
	if max > 0 && len(raw) > max {
		raw = raw[:max]
		truncated = true
	}

	// Best-effort JSON redaction for common credential fields.
	if redact && json.Valid(raw) {
		raw = redactJSONBody(raw)
	}

	return dumpBody{
		Encoding:  "utf-8",
		Truncated: truncated,
		Bytes:     len(orig),
		Text:      string(raw),
	}
}

func redactJSONBody(b []byte) []byte {
	var anyVal any
	if err := json.Unmarshal(b, &anyVal); err != nil {
		return b
	}
	redactJSONValue(anyVal)
	out, err := json.Marshal(anyVal)
	if err != nil {
		return b
	}
	// Preserve readability a bit; keep it compact but ensure it's valid JSON.
	return bytes.TrimSpace(out)
}

func redactJSONValue(v any) {
	switch vv := v.(type) {
	case map[string]any:
		for k, val := range vv {
			lk := strings.ToLower(k)
			switch lk {
			case "api_key", "apikey", "token", "access_token", "authorization", "x-api-key", "channelkey", "channel_key":
				vv[k] = "***REDACTED***"
			default:
				redactJSONValue(val)
			}
		}
	case []any:
		for i := range vv {
			redactJSONValue(vv[i])
		}
	}
}
