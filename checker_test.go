package pharos

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type fakeProber struct{ res ProbeResult }

func (f *fakeProber) Probe(context.Context, string, time.Duration) ProbeResult { return f.res }

type fakeMesh struct{ status string }

func (f *fakeMesh) MeshStatus(string) string { return f.status }

func at1000() time.Time { return time.Unix(1000, 0) }

func TestChecker_MeshOverlayAndRawStatusRecorded(t *testing.T) {
	store := NewMemStore()
	store.now = at1000
	prober := &fakeProber{res: ProbeResult{Status: StatusOperational, StatusCode: 200, ResponseTimeMS: 5}}
	h, err := NewHealthChecker(CheckerConfig{
		Store:           store,
		Prober:          prober,
		Mesh:            &fakeMesh{"unreachable"},
		ConfigEndpoints: []Endpoint{{Slug: "api", Name: "API", URL: "http://x", Enabled: true}},
		Now:             at1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	h.RunOnce(ctx)
	checks, _ := store.GetRecentChecks(ctx, "api", 1)
	if len(checks) != 1 {
		t.Fatalf("a check must be recorded; got %d", len(checks))
	}
	if checks[0].Status != StatusPartialOutage {
		t.Fatalf("mesh unreachable must downgrade operational→partial_outage (combined); got %q", checks[0].Status)
	}
	if checks[0].HTTPStatus != StatusOperational {
		t.Fatalf("raw HTTP status must be preserved separately from the mesh overlay; got %q", checks[0].HTTPStatus)
	}
}

func TestChecker_NoStoreErrors(t *testing.T) {
	if _, err := NewHealthChecker(CheckerConfig{}); err == nil {
		t.Fatal("a nil Store must error")
	}
}

func TestChecker_IncidentCreateThenResolve(t *testing.T) {
	store := NewMemStore()
	store.now = at1000
	var events []string
	prober := &fakeProber{}
	h, _ := NewHealthChecker(CheckerConfig{
		Store:           store,
		Prober:          prober,
		ConfigEndpoints: []Endpoint{{Slug: "api", Name: "API", URL: "http://x", Enabled: true, LastStatus: StatusOperational}},
		OnIncident:      func(_, _, status string) { events = append(events, status) },
		Now:             at1000,
	})
	// degrade: operational → major_outage opens an incident
	prober.res = ProbeResult{Status: StatusMajorOutage, StatusCode: 500}
	h.RunOnce(ctx)
	if active, _ := store.GetActiveIncidents(ctx); len(active) != 1 {
		t.Fatalf("degradation must open exactly one incident; got %d", len(active))
	}
	// a second down check while already down must NOT open a duplicate
	h.RunOnce(ctx)
	if active, _ := store.GetActiveIncidents(ctx); len(active) != 1 {
		t.Fatalf("a persisting outage must not open a second incident; got %d", len(active))
	}
	// recover: the endpoint's last status now reflects the outage; operational resolves it
	h.configEndpoints[0].LastStatus = StatusMajorOutage
	prober.res = ProbeResult{Status: StatusOperational, StatusCode: 200}
	h.RunOnce(ctx)
	if active, _ := store.GetActiveIncidents(ctx); len(active) != 0 {
		t.Fatalf("recovery must resolve the incident; got %d active", len(active))
	}
	if len(events) != 2 || events[0] != "created" || events[1] != "resolved" {
		t.Fatalf("OnIncident must fire created then resolved; got %v", events)
	}
}

func TestChecker_ResolveStaleIncidents(t *testing.T) {
	store := NewMemStore()
	store.now = at1000
	store.InsertIncident(ctx, &Incident{Title: "orphan", Status: "investigating", StartedAt: 500})
	h, _ := NewHealthChecker(CheckerConfig{Store: store, Now: at1000})
	h.ResolveStaleIncidents(ctx)
	if active, _ := store.GetActiveIncidents(ctx); len(active) != 0 {
		t.Fatalf("stale incidents from a prior run must be resolved; got %d", len(active))
	}
}

func TestHTTPProber_StatusMappingHitsHeartbeat(t *testing.T) {
	cases := []struct {
		code int
		want string
	}{
		{200, StatusOperational},
		{301, StatusOperational},
		{503, StatusMaintenance},
		{404, StatusDegraded},
		{500, StatusMajorOutage},
	}
	for _, c := range cases {
		hitHeartbeat := false
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/heartbeat" {
				hitHeartbeat = true
			}
			w.WriteHeader(c.code)
		}))
		res := NewHTTPProber().Probe(ctx, srv.URL, 2*time.Second)
		if !hitHeartbeat {
			t.Errorf("code %d: prober must GET /heartbeat", c.code)
		}
		if res.Status != c.want {
			t.Errorf("code %d → %q, want %q", c.code, res.Status, c.want)
		}
		srv.Close()
	}
}
