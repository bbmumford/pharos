package pharos

import "testing"

// A config endpoint carries no recorded status, so the checker itself must
// carry each endpoint's last status forward: degradation opens one incident
// and fires OnIncident, recovery resolves it.
func TestChecker_ConfigEndpointsOpenAndResolveIncidents(t *testing.T) {
	store := NewMemStore()
	store.now = at1000
	var events []string
	prober := &fakeProber{res: ProbeResult{Status: StatusOperational, StatusCode: 200}}
	h, err := NewHealthChecker(CheckerConfig{
		Store:           store,
		Prober:          prober,
		ConfigEndpoints: []Endpoint{{Slug: "api", Name: "API", URL: "http://x", Enabled: true}},
		OnIncident:      func(_, _, status string) { events = append(events, status) },
		Now:             at1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	h.RunOnce(ctx) // first check: operational, no transition
	prober.res = ProbeResult{Status: StatusMajorOutage, StatusCode: 500}
	h.RunOnce(ctx)
	if active, _ := store.GetActiveIncidents(ctx); len(active) != 1 {
		t.Fatalf("a config endpoint going down must open one incident; got %d", len(active))
	}
	h.RunOnce(ctx)
	if active, _ := store.GetActiveIncidents(ctx); len(active) != 1 {
		t.Fatalf("a persisting outage must not open a second incident; got %d", len(active))
	}
	prober.res = ProbeResult{Status: StatusOperational, StatusCode: 200}
	h.RunOnce(ctx)
	if active, _ := store.GetActiveIncidents(ctx); len(active) != 0 {
		t.Fatalf("recovery must resolve the incident; got %d active", len(active))
	}
	if len(events) != 2 || events[0] != "created" || events[1] != "resolved" {
		t.Fatalf("OnIncident must fire created then resolved; got %v", events)
	}
}

// A checker started over a store that already holds checks carries on from
// the latest one: the first check after a restart that finds the endpoint
// down, where the store last recorded it up, is a transition.
func TestChecker_ARestartedConfigCheckerCarriesOnFromTheStore(t *testing.T) {
	store := NewMemStore()
	store.now = at1000
	cfg := CheckerConfig{
		Store:           store,
		Prober:          &fakeProber{res: ProbeResult{Status: StatusOperational, StatusCode: 200}},
		ConfigEndpoints: []Endpoint{{Slug: "api", Name: "API", URL: "http://x", Enabled: true}},
		Now:             at1000,
	}
	before, _ := NewHealthChecker(cfg)
	before.RunOnce(ctx)
	var events []string
	cfg.Prober = &fakeProber{res: ProbeResult{Status: StatusMajorOutage, StatusCode: 500}}
	cfg.OnIncident = func(_, _, status string) { events = append(events, status) }
	after, _ := NewHealthChecker(cfg)
	after.RunOnce(ctx)
	if len(events) != 1 || events[0] != "created" {
		t.Fatalf("the restarted checker's first down check must open an incident; got %v", events)
	}
}

// The SQL store answers the latest check of an endpoint even when two checks
// carry the same second, so a config-mode checker over it sees every change.
func TestChecker_ConfigEndpointsOverTheSQLStore(t *testing.T) {
	store := newSQLStore(t, at1000())
	var events []string
	prober := &fakeProber{res: ProbeResult{Status: StatusOperational, StatusCode: 200}}
	h, err := NewHealthChecker(CheckerConfig{
		Store:           store,
		Prober:          prober,
		ConfigEndpoints: []Endpoint{{Slug: "api", Name: "API", URL: "http://x", Enabled: true}},
		OnIncident:      func(_, _, status string) { events = append(events, status) },
		Now:             at1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	h.RunOnce(ctx)
	prober.res = ProbeResult{Status: StatusMajorOutage, StatusCode: 500}
	h.RunOnce(ctx)
	h.RunOnce(ctx)
	prober.res = ProbeResult{Status: StatusOperational, StatusCode: 200}
	h.RunOnce(ctx)
	if len(events) != 2 || events[0] != "created" || events[1] != "resolved" {
		t.Fatalf("OnIncident must fire created then resolved; got %v", events)
	}
}
