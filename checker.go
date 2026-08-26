package pharos

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Status vocabulary for a combined mesh+HTTP endpoint status.
const (
	StatusOperational   = "operational"
	StatusMaintenance   = "maintenance"
	StatusDegraded      = "degraded"
	StatusPartialOutage = "partial_outage"
	StatusMajorOutage   = "major_outage"
)

// MeshStatusProvider returns the mesh connectivity status for a service ("healthy", "degraded",
// "connecting", or "unreachable"). It is the injected seam for the mesh overlay — a status page with
// no mesh wires nil and gets pure HTTP checks; a mesh-aware consumer wires its own provider.
type MeshStatusProvider interface {
	MeshStatus(serviceName string) string
}

// ProbeResult is one raw health probe outcome, before any mesh overlay.
type ProbeResult struct {
	Status         string // one of the Status* constants (never partial_outage — that is mesh-derived)
	ResponseTimeMS int
	StatusCode     int
	ErrorMessage   string
}

// Prober performs a single health probe of an endpoint URL. The default HTTPProber does an HTTP GET
// of url+Path ("/heartbeat"); injecting the probe keeps the checker's mesh-overlay and incident logic
// testable without real network I/O, and lets a consumer probe by another means.
type Prober interface {
	Probe(ctx context.Context, url string, timeout time.Duration) ProbeResult
}

// HTTPProber is the default Prober: an HTTP GET of url+Path, mapping the response code to a status
// (2xx/3xx operational, 503 maintenance, 4xx degraded, 5xx major_outage, transport error
// major_outage). Redirects are not followed.
type HTTPProber struct {
	Client    *http.Client
	UserAgent string
	Path      string
}

// NewHTTPProber builds the default HTTP prober (no-redirect client, 10s ceiling, "/heartbeat").
func NewHTTPProber() *HTTPProber {
	return &HTTPProber{
		Client: &http.Client{
			Timeout: 10 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse // do not follow redirects
			},
		},
		UserAgent: "pharos-healthchecker/1.0",
		Path:      "/heartbeat",
	}
}

// Probe issues the health request and maps the outcome to a ProbeResult.
func (p *HTTPProber) Probe(ctx context.Context, url string, timeout time.Duration) ProbeResult {
	start := time.Now()
	var r ProbeResult
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	checkURL := strings.TrimRight(url, "/") + p.Path
	req, err := http.NewRequestWithContext(cctx, http.MethodGet, checkURL, nil)
	if err != nil {
		r.Status = StatusMajorOutage
		r.ErrorMessage = err.Error()
		return r
	}
	req.Header.Set("User-Agent", p.UserAgent)

	resp, err := p.Client.Do(req)
	r.ResponseTimeMS = int(time.Since(start).Milliseconds())
	if err != nil {
		r.Status = StatusMajorOutage
		r.ErrorMessage = err.Error()
		return r
	}
	defer resp.Body.Close()
	r.StatusCode = resp.StatusCode

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 400:
		r.Status = StatusOperational // 2xx and 3xx (redirects) are OK
	case resp.StatusCode == 503:
		r.Status = StatusMaintenance
		r.ErrorMessage = "Service unavailable"
	case resp.StatusCode >= 400 && resp.StatusCode < 500:
		r.Status = StatusDegraded
		r.ErrorMessage = "Client error"
	case resp.StatusCode >= 500:
		r.Status = StatusMajorOutage
		r.ErrorMessage = "Server error"
	default:
		r.Status = StatusDegraded
		r.ErrorMessage = "Unexpected status code"
	}
	return r
}

// CheckerConfig wires a HealthChecker. Store is required; everything else has a working default or is
// optional.
type CheckerConfig struct {
	Store StatusStore
	// Interval between check cycles once Start()'s loop is running (default 30s).
	Interval time.Duration
	// Prober performs each endpoint probe; nil ⇒ NewHTTPProber().
	Prober Prober
	// Mesh overlays connectivity status onto the HTTP result; nil ⇒ pure HTTP checks.
	Mesh MeshStatusProvider
	// OnIncident is called when an endpoint incident is created or resolved (slug, title, "created" |
	// "resolved"); nil ⇒ no callback.
	OnIncident func(slug, title, status string)
	// NodeID attributes each recorded check to a checking node.
	NodeID string
	// ConfigEndpoints, if non-empty, are checked instead of Store.GetAllEndpoints (config-driven).
	ConfigEndpoints []Endpoint
	// GracePeriod is the mesh-convergence wait before the first check in Start() (default 30s); 0
	// keeps the default, use a negative value to skip.
	GracePeriod time.Duration
	// Now is the clock (default time.Now); injectable for tests.
	Now func() time.Time
	// Logf is an optional operational/diagnostic sink; nil ⇒ silent.
	Logf func(string, ...any)
}

// HealthChecker periodically probes endpoints (HTTP + optional mesh overlay), records checks and
// status changes, and opens/closes auto-incidents on degradation/recovery. It owns its incident
// bookkeeping in memory (activeIncidents), reconciled against the store on Start.
type HealthChecker struct {
	store           StatusStore
	interval        time.Duration
	prober          Prober
	mesh            MeshStatusProvider
	onIncident      func(slug, title, status string)
	nodeID          string
	configEndpoints []Endpoint
	grace           time.Duration
	now             func() time.Time
	logf            func(string, ...any)

	mu              sync.Mutex
	activeIncidents map[string]string // endpoint slug → open incident id
	stopCh          chan struct{}
	wg              sync.WaitGroup
}

// NewHealthChecker validates the config and returns a checker.
func NewHealthChecker(c CheckerConfig) (*HealthChecker, error) {
	if c.Store == nil {
		return nil, errNoStore
	}
	h := &HealthChecker{
		store:           c.Store,
		interval:        c.Interval,
		prober:          c.Prober,
		mesh:            c.Mesh,
		onIncident:      c.OnIncident,
		nodeID:          c.NodeID,
		configEndpoints: c.ConfigEndpoints,
		grace:           c.GracePeriod,
		now:             c.Now,
		logf:            c.Logf,
		activeIncidents: map[string]string{},
		stopCh:          make(chan struct{}),
	}
	if h.interval <= 0 {
		h.interval = 30 * time.Second
	}
	if h.prober == nil {
		h.prober = NewHTTPProber()
	}
	if h.grace == 0 {
		h.grace = 30 * time.Second
	}
	if h.now == nil {
		h.now = time.Now
	}
	if h.logf == nil {
		h.logf = func(string, ...any) {}
	}
	return h, nil
}

var errNoStore = &configError{"pharos: CheckerConfig.Store is required"}

type configError struct{ msg string }

func (e *configError) Error() string { return e.msg }

// Start runs the check loop until Stop: it first resolves stale incidents from a prior run, waits the
// grace period for mesh convergence, runs an initial check, then checks every Interval.
func (h *HealthChecker) Start() {
	h.wg.Add(1)
	defer h.wg.Done()

	h.ResolveStaleIncidents(context.Background())

	if h.grace > 0 {
		h.logf("healthchecker: waiting %v for mesh convergence before first check", h.grace)
		select {
		case <-time.After(h.grace):
		case <-h.stopCh:
			return
		}
	}

	h.RunOnce(context.Background())
	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			h.RunOnce(context.Background())
		case <-h.stopCh:
			return
		}
	}
}

// Stop halts the loop and waits for it to exit.
func (h *HealthChecker) Stop() {
	close(h.stopCh)
	h.wg.Wait()
}

// endpoints returns the config endpoints if set, otherwise the store's.
func (h *HealthChecker) endpoints(ctx context.Context) []Endpoint {
	if len(h.configEndpoints) > 0 {
		return h.configEndpoints
	}
	eps, err := h.store.GetAllEndpoints(ctx)
	if err != nil {
		h.logf("healthchecker: get endpoints: %v", err)
		return nil
	}
	return eps
}

// RunOnce performs one full check cycle over every enabled endpoint. Exported so a consumer can drive
// checks on its own schedule and so the incident logic is testable without the loop.
func (h *HealthChecker) RunOnce(ctx context.Context) {
	for i := range h.endpoints(ctx) {
		ep := h.endpoints(ctx)[i]
		if !ep.Enabled {
			continue
		}
		h.checkEndpoint(ctx, ep)
	}
}

// checkEndpoint probes one endpoint, applies the mesh overlay, records the check + any status change,
// and opens/closes its auto-incident.
func (h *HealthChecker) checkEndpoint(ctx context.Context, ep Endpoint) {
	timeout := time.Duration(ep.TimeoutMS) * time.Millisecond
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	res := h.prober.Probe(ctx, ep.URL, timeout)
	httpStatus := res.Status // raw HTTP status before mesh overlay

	// Mesh overlay: any non-healthy mesh downgrades an otherwise-operational endpoint.
	if h.mesh != nil && res.Status == StatusOperational {
		switch h.mesh.MeshStatus(ep.Slug) {
		case "healthy":
		case "degraded", "connecting":
			res.Status = StatusDegraded
			res.ErrorMessage = "HTTP healthy but mesh " + h.mesh.MeshStatus(ep.Slug)
		case "unreachable":
			res.Status = StatusPartialOutage
			res.ErrorMessage = "HTTP healthy but mesh unreachable"
		}
	}

	if err := h.store.InsertCheck(ctx, &Check{
		EndpointSlug:   ep.Slug,
		Status:         res.Status, // combined mesh+HTTP
		HTTPStatus:     httpStatus, // raw HTTP only
		ResponseTimeMS: res.ResponseTimeMS,
		StatusCode:     res.StatusCode,
		ErrorMessage:   res.ErrorMessage,
		CheckedAt:      h.now().Unix(),
		NodeID:         h.nodeID,
	}); err != nil {
		h.logf("healthchecker: record check %s: %v", ep.Slug, err)
	}

	if res.Status == ep.LastStatus || ep.LastStatus == "" {
		return // no transition (or first-ever check)
	}
	if err := h.store.InsertStatusChange(ctx, ep.Slug, ep.LastStatus, res.Status, res.ErrorMessage, ""); err != nil {
		h.logf("healthchecker: record status change %s: %v", ep.Slug, err)
	}
	if isDown(res.Status) {
		h.openIncident(ctx, ep, res.Status)
	} else {
		h.resolveIncident(ctx, ep)
	}
}

func isDown(status string) bool {
	return status == StatusDegraded || status == StatusPartialOutage || status == StatusMajorOutage
}

func (h *HealthChecker) openIncident(ctx context.Context, ep Endpoint, status string) {
	h.mu.Lock()
	_, already := h.activeIncidents[ep.Slug]
	h.mu.Unlock()
	if already {
		return
	}
	impact := "minor"
	if status == StatusMajorOutage || status == StatusPartialOutage {
		impact = "major"
	}
	title := ep.Name + " — " + statusLabel(status)
	inc := &Incident{Title: title, Status: "investigating", Impact: impact, AffectedServices: []string{ep.Slug}}
	if err := h.store.InsertIncident(ctx, inc); err != nil {
		h.logf("healthchecker: create incident %s: %v", ep.Slug, err)
		return
	}
	h.mu.Lock()
	h.activeIncidents[ep.Slug] = inc.ID
	h.mu.Unlock()
	h.logf("healthchecker: incident created for %s: %s (%s)", ep.Slug, title, inc.ID)
	if h.onIncident != nil {
		h.onIncident(ep.Slug, title, "created")
	}
}

func (h *HealthChecker) resolveIncident(ctx context.Context, ep Endpoint) {
	h.mu.Lock()
	id, ok := h.activeIncidents[ep.Slug]
	h.mu.Unlock()
	if !ok {
		return
	}
	title := ep.Name + " — Recovered"
	if err := h.store.UpdateIncident(ctx, &Incident{
		ID: id, Title: title, Status: "resolved", Impact: "none",
		AffectedServices: []string{ep.Slug}, ResolvedAt: h.now().Unix(),
	}); err != nil {
		h.logf("healthchecker: resolve incident %s: %v", ep.Slug, err)
		return
	}
	h.mu.Lock()
	delete(h.activeIncidents, ep.Slug)
	h.mu.Unlock()
	h.logf("healthchecker: incident resolved for %s: %s", ep.Slug, id)
	if h.onIncident != nil {
		h.onIncident(ep.Slug, title, "resolved")
	}
}

// ResolveStaleIncidents bulk-resolves every unresolved incident left by a prior run — on restart the
// in-memory map is empty, so pre-restart incidents would otherwise be orphaned. Runs at Start.
func (h *HealthChecker) ResolveStaleIncidents(ctx context.Context) {
	incs, err := h.store.GetActiveIncidents(ctx)
	if err != nil {
		h.logf("healthchecker: resolveStaleIncidents query: %v", err)
		return
	}
	for _, inc := range incs {
		if err := h.store.UpdateIncident(ctx, &Incident{
			ID: inc.ID, Title: inc.Title + " (auto-resolved on restart)", Status: "resolved",
			Impact: "none", AffectedServices: inc.AffectedServices, ResolvedAt: h.now().Unix(),
		}); err != nil {
			h.logf("healthchecker: resolveStaleIncidents update %s: %v", inc.ID, err)
		}
	}
}

// ScanExistingDegradation opens a retroactive incident for any endpoint already in a down LastStatus
// with no active incident — covers a restart while endpoints were already degraded, where no
// transition (and so no auto-incident) would otherwise occur.
func (h *HealthChecker) ScanExistingDegradation(ctx context.Context) {
	for _, ep := range h.endpoints(ctx) {
		if !isDown(ep.LastStatus) {
			continue
		}
		h.mu.Lock()
		_, ok := h.activeIncidents[ep.Slug]
		h.mu.Unlock()
		if ok {
			continue
		}
		impact := "minor"
		if ep.LastStatus == StatusMajorOutage || ep.LastStatus == StatusPartialOutage {
			impact = "major"
		}
		inc := &Incident{Title: ep.Name + " " + ep.LastStatus, Status: "investigating", Impact: impact, AffectedServices: []string{ep.Slug}}
		if err := h.store.InsertIncident(ctx, inc); err != nil {
			h.logf("healthchecker: retroactive incident %s: %v", ep.Slug, err)
			continue
		}
		h.mu.Lock()
		h.activeIncidents[ep.Slug] = inc.ID
		h.mu.Unlock()
	}
}

func statusLabel(status string) string {
	switch status {
	case StatusDegraded:
		return "Degraded"
	case StatusPartialOutage:
		return "Partial Outage"
	case StatusMajorOutage:
		return "Major Outage"
	default:
		return status
	}
}
