package pharos

import (
	"errors"
	"testing"
)

func TestDetermineServiceStatus_WorstOf(t *testing.T) {
	cases := []struct {
		name string
		last []string
		want string
	}{
		{"empty", nil, StatusOperational},
		{"all ok/blank", []string{StatusOperational, ""}, StatusOperational},
		{"one degraded", []string{StatusOperational, StatusDegraded}, StatusDegraded},
		{"some outage → partial", []string{StatusOperational, StatusMajorOutage}, StatusPartialOutage},
		{"all outage → major", []string{StatusMajorOutage, StatusPartialOutage}, StatusMajorOutage},
		{"degraded loses to outage", []string{StatusDegraded, StatusMajorOutage}, StatusPartialOutage},
	}
	for _, c := range cases {
		eps := make([]Endpoint, len(c.last))
		for i, s := range c.last {
			eps[i] = Endpoint{LastStatus: s}
		}
		if got := DetermineServiceStatus(eps); got != c.want {
			t.Errorf("%s: got %q want %q", c.name, got, c.want)
		}
	}
}

func TestReader_OverallStatusRollup(t *testing.T) {
	store := NewMemStore()
	store.now = at1000
	store.UpsertService(ctx, &Service{Slug: "api", DisplayOrder: 1})
	store.UpsertService(ctx, &Service{Slug: "web", DisplayOrder: 2})
	store.UpsertService(ctx, &Service{Slug: "cdn", DisplayOrder: 3})
	// last status is DERIVED from the latest check (not a stored field); endpoints must be enabled
	for _, slug := range []string{"api-1", "web-1", "web-2", "cdn-1"} {
		svc := map[string]string{"api-1": "api", "web-1": "web", "web-2": "web", "cdn-1": "cdn"}[slug]
		store.UpsertEndpoint(ctx, &Endpoint{Slug: slug, ServiceSlug: svc, Enabled: true})
	}
	// api all-out → major; web one-out → partial; cdn ok → operational
	store.InsertCheck(ctx, &Check{EndpointSlug: "api-1", Status: StatusMajorOutage, CheckedAt: 100})
	store.InsertCheck(ctx, &Check{EndpointSlug: "web-1", Status: StatusOperational, CheckedAt: 100})
	store.InsertCheck(ctx, &Check{EndpointSlug: "web-2", Status: StatusMajorOutage, CheckedAt: 100})
	store.InsertCheck(ctx, &Check{EndpointSlug: "cdn-1", Status: StatusOperational, CheckedAt: 100})
	store.InsertIncident(ctx, &Incident{Title: "down", Status: "investigating", StartedAt: 999})
	store.InsertMaintenanceWindow(ctx, &MaintenanceWindow{Title: "m", AffectedServices: []string{"api"}, StartAt: 900, EndAt: 1100, Status: "in_progress"})

	r := NewReader(store)
	got, err := r.OverallStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// worst-of: api=major → overall major
	if got.Status != StatusMajorOutage {
		t.Fatalf("overall must be worst-of (major); got %q", got.Status)
	}
	if got.ServicesOperational != 1 || got.ServicesDegraded != 0 || got.ServicesOutage != 2 {
		t.Fatalf("counts: op=%d deg=%d out=%d (want 1/0/2)", got.ServicesOperational, got.ServicesDegraded, got.ServicesOutage)
	}
	if got.ActiveIncidents != 1 || got.ActiveMaintenance != 1 {
		t.Fatalf("active incidents=%d maintenance=%d (want 1/1)", got.ActiveIncidents, got.ActiveMaintenance)
	}
}

func TestReader_ServiceStatusAndUnknown(t *testing.T) {
	store := NewMemStore()
	store.now = at1000
	store.UpsertService(ctx, &Service{Slug: "api", Name: "API"})
	store.UpsertEndpoint(ctx, &Endpoint{Slug: "api-1", ServiceSlug: "api", Enabled: true})
	store.InsertCheck(ctx, &Check{EndpointSlug: "api-1", Status: StatusDegraded, CheckedAt: 100})
	store.UpsertUptimeMetric(ctx, &UptimeMetric{EndpointSlug: "api-1", Date: "2021-01-01", UptimePercent: 99})

	r := NewReader(store)
	d, err := r.ServiceStatus(ctx, "api")
	if err != nil {
		t.Fatal(err)
	}
	if d.Status != StatusDegraded || d.Name != "API" || len(d.Endpoints) != 1 || len(d.UptimeHistory) != 1 {
		t.Fatalf("service detail wrong: %+v", d)
	}
	if _, err := r.ServiceStatus(ctx, "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatal("unknown service must be ErrNotFound")
	}
	if _, err := r.ServiceStatus(ctx, ""); !errors.Is(err, ErrNotFound) {
		t.Fatal("empty slug must be ErrNotFound")
	}
}
