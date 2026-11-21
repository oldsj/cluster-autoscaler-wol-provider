// Package httpreq is the http plugin: a generic webhook, for smart plugs, Home Assistant, a BMC's
// Redfish API, or anything else that switches power with an HTTP request. It powers on, powers off,
// and asks the approval hook.
//
// Settings:
//
//	url: "https://hub.example/api/services/switch/turn_on"   # required; a template
//	method: POST                                              # default POST
//	headers: {Content-Type: application/json}
//	headerFiles:                                              # header value read from a file on
//	  Authorization: /etc/wol-provider/http/authorization     #   every request: a mounted Secret
//	body: '{"entity_id": "switch.{{ .Name }}"}'               # a template; default empty
//	timeout: 10s                                              # default 10s
//	caFile: /etc/wol-provider/http/ca.crt                     # extra CA for the server; optional
//
// url and body are Go templates with {{ .Name }} (machine), {{ .Node }}, {{ .Group }} and
// {{ .Action }} (on, off or approve). Power on and off succeed on any 2xx status.
//
// As the approval hook: 200-299 except 202 approves; 202 means pending, and the provider polls the
// response's Location header with GET (or repeats the request when there's none) until it gets
// another answer; any other status denies.
package httpreq

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/oldsj/cluster-autoscaler-wol-provider/internal/plugin"
)

// Settings are the http plugin's settings.
type Settings struct {
	URL         string            `json:"url"`
	Method      string            `json:"method,omitempty"`
	Headers     map[string]string `json:"headers,omitempty"`
	HeaderFiles map[string]string `json:"headerFiles,omitempty"`
	Body        string            `json:"body,omitempty"`
	Timeout     plugin.Duration   `json:"timeout,omitempty"`
	CAFile      string            `json:"caFile,omitempty"`
}

// Request is one configured HTTP request.
type Request struct {
	s      Settings
	url    plugin.Template
	body   plugin.Template
	client *http.Client
}

// NewRequest builds a request from settings.
func NewRequest(raw json.RawMessage) (*Request, error) {
	var s Settings
	if err := plugin.Decode(raw, &s); err != nil {
		return nil, fmt.Errorf("http: %w", err)
	}
	if s.URL == "" {
		return nil, fmt.Errorf("http: url is required")
	}
	if s.Method == "" {
		s.Method = http.MethodPost
	}
	u, err := plugin.ParseTemplate("url", s.URL)
	if err != nil {
		return nil, fmt.Errorf("http: %w", err)
	}
	b, err := plugin.ParseTemplate("body", s.Body)
	if err != nil {
		return nil, fmt.Errorf("http: %w", err)
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if s.CAFile != "" {
		pem, err := os.ReadFile(s.CAFile)
		if err != nil {
			return nil, fmt.Errorf("http: caFile: %w", err)
		}
		pool, _ := x509.SystemCertPool()
		if pool == nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("http: caFile: no PEM certificates in %s", s.CAFile)
		}
		transport.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	}
	return &Request{s: s, url: u, body: b, client: &http.Client{Transport: transport, Timeout: s.Timeout.Or(10 * time.Second)}}, nil
}

// Response is the part of a reply the plugins use.
type Response struct {
	Status   int
	Location string
	Body     string // the first 512 bytes, for error messages
}

// Do sends the request for t, with the action available to the templates. A non-empty override
// replaces the request with a GET of that URL (polling a pending approval).
func (r *Request) Do(ctx context.Context, t plugin.Target, action, override string) (Response, error) {
	extra := map[string]string{"Action": action}
	method, target, body := http.MethodGet, override, ""
	if override == "" {
		var err error
		if target, err = r.url.Render(t, extra); err != nil {
			return Response{}, fmt.Errorf("http: url: %w", err)
		}
		if body, err = r.body.Render(t, extra); err != nil {
			return Response{}, fmt.Errorf("http: body: %w", err)
		}
		method = r.s.Method
	}
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, target, rd)
	if err != nil {
		return Response{}, fmt.Errorf("http: %w", err)
	}
	for k, v := range r.s.Headers {
		req.Header.Set(k, v)
	}
	for k, path := range r.s.HeaderFiles {
		v, err := os.ReadFile(path)
		if err != nil {
			return Response{}, fmt.Errorf("http: header %s: %w", k, err)
		}
		req.Header.Set(k, strings.TrimSpace(string(v)))
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return Response{}, fmt.Errorf("http: %s: %w", method, redact(err))
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	_, _ = io.Copy(io.Discard, resp.Body)
	out := Response{Status: resp.StatusCode, Body: strings.TrimSpace(string(b))}
	if loc := resp.Header.Get("Location"); loc != "" {
		if u, err := resp.Request.URL.Parse(loc); err == nil {
			out.Location = u.String()
		}
	}
	return out, nil
}

// redact drops the URL's query from client errors, since it may carry a token.
func redact(err error) error {
	if ue, ok := err.(*url.Error); ok {
		if u, perr := url.Parse(ue.URL); perr == nil {
			u.RawQuery, u.User = "", nil
			ue.URL = u.String()
		}
		return ue
	}
	return err
}

func (r *Request) expect2xx(ctx context.Context, t plugin.Target, action string) error {
	resp, err := r.Do(ctx, t, action, "")
	if err != nil {
		return err
	}
	if resp.Status < 200 || resp.Status > 299 {
		return fmt.Errorf("http: power %s %s: status %d: %s", action, t.Name, resp.Status, resp.Body)
	}
	return nil
}

// On powers machines on with the request.
type On struct{ *Request }

// PowerOn sends the request; any 2xx succeeds.
func (o On) PowerOn(ctx context.Context, t plugin.Target) error { return o.expect2xx(ctx, t, "on") }

// Off powers machines off with the request.
type Off struct{ *Request }

// PowerOff sends the request; any 2xx succeeds.
func (o Off) PowerOff(ctx context.Context, t plugin.Target) error { return o.expect2xx(ctx, t, "off") }

// NewOn builds the power-on plugin.
func NewOn(raw json.RawMessage) (plugin.PowerOn, error) {
	r, err := NewRequest(raw)
	if err != nil {
		return nil, err
	}
	return On{r}, nil
}

// NewOff builds the power-off plugin.
func NewOff(raw json.RawMessage) (plugin.PowerOff, error) {
	r, err := NewRequest(raw)
	if err != nil {
		return nil, err
	}
	return Off{r}, nil
}

// Approver asks the request before each power-off.
type Approver struct{ *Request }

// NewApprover builds the approval hook.
func NewApprover(raw json.RawMessage) (plugin.Approver, error) {
	r, err := NewRequest(raw)
	if err != nil {
		return nil, err
	}
	return Approver{r}, nil
}

// pollSame is the ticket for a pending answer without a Location: repeat the original request.
const pollSame = "repeat"

// Approve sends the request (empty ticket) or polls a pending one.
func (a Approver) Approve(ctx context.Context, t plugin.Target, ticket string) (plugin.Decision, string, error) {
	override := ""
	if ticket != "" && ticket != pollSame {
		override = ticket
	}
	resp, err := a.Do(ctx, t, "approve", override)
	if err != nil {
		return plugin.Pending, ticket, err
	}
	switch {
	case resp.Status == http.StatusAccepted:
		next := resp.Location
		if next == "" {
			next = ticket
		}
		if next == "" {
			next = pollSame
		}
		return plugin.Pending, next, nil
	case resp.Status >= 200 && resp.Status <= 299:
		return plugin.Approved, "", nil
	default:
		return plugin.Denied, "", fmt.Errorf("http: approval for %s: status %d: %s", t.Name, resp.Status, resp.Body)
	}
}
