package pharos

import (
	"context"
	"errors"
	"testing"
	"time"
)

func storeAt(t time.Time) *MemStore {
	m := NewMemStore()
	m.now = func() time.Time { return t }
	return m
}

var ctx = context.Background()

func TestServices_OrderAndLookup(t *testing.T) {
	m := storeAt(time.Unix(1000, 0))
	if err := m.UpsertService(ctx, &Service{Slug: "b", Name: "B", DisplayOrder: 2}); err != nil {
		t.Fatalf("m.UpsertService: %v", err)
	}
	if err := m.UpsertService(ctx, &Service{Slug: "a", Name: "A", DisplayOrder: 1}); err != nil {
		t.Fatalf("m.UpsertService: %v", err)
	}
	got, _ := m.GetAllServices(ctx)
	if len(got) != 2 || got[0].Slug != "a" || got[1].Slug != "b" {
		t.Fatalf("services must order by display_order; got %+v", got)
	}
	if s, err := m.GetServiceBySlug(ctx, "a"); err != nil || s.Name != "A" {
		t.Fatalf("GetServiceBySlug: %+v %v", s, err)
	}
	if _, err := m.GetServiceBySlug(ctx, "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatal("unknown slug must be ErrNotFound")
	}
}

func TestEndpoints_DerivedLastStatusAndEnabledFilter(t *testing.T) {
	m := storeAt(time.Unix(1000, 0))
	if err := m.UpsertEndpoint(ctx, &Endpoint{Slug: "on", ServiceSlug: "s", Name: "on", Enabled: true}); err != nil {
		t.Fatalf("m.UpsertEndpoint: %v", err)
	}
	if err := m.UpsertEndpoint(ctx, &Endpoint{Slug: "off", ServiceSlug: "s", Name: "off", Enabled: false}); err != nil {
		t.Fatalf("m.UpsertEndpoint: %v", err)
	}
	// last status is derived from the LATEST check, not stored
	if err := m.InsertCheck(ctx, &Check{EndpointSlug: "on", Status: StatusOperational, HTTPStatus: StatusOperational, CheckedAt: 100}); err != nil {
		t.Fatalf("m.InsertCheck: %v", err)
	}
	if err := m.InsertCheck(ctx, &Check{EndpointSlug: "on", Status: StatusMajorOutage, HTTPStatus: StatusMajorOutage, ResponseTimeMS: 7, CheckedAt: 200}); err != nil {
		t.Fatalf("m.InsertCheck: %v", err)
	}

	// GetAllEndpoints excludes disabled and derives last status from the newest check
	all, _ := m.GetAllEndpoints(ctx)
	if len(all) != 1 || all[0].Slug != "on" {
		t.Fatalf("GetAllEndpoints must return only enabled endpoints; got %+v", all)
	}
	if all[0].LastStatus != StatusMajorOutage || all[0].LastCheckAt != 200 || all[0].LastResponseMS != 7 {
		t.Fatalf("last status must derive from the newest check; got status=%q at=%d ms=%d", all[0].LastStatus, all[0].LastCheckAt, all[0].LastResponseMS)
	}
	// GetEndpointsByService returns disabled too; a never-checked endpoint derives "unknown"
	svc, _ := m.GetEndpointsByService(ctx, "s")
	if len(svc) != 2 {
		t.Fatalf("GetEndpointsByService must include disabled; got %d", len(svc))
	}
	byName := map[string]Endpoint{}
	for _, e := range svc {
		byName[e.Slug] = e
	}
	if byName["off"].LastStatus != "unknown" {
		t.Fatalf("a never-checked endpoint must derive \"unknown\"; got %q", byName["off"].LastStatus)
	}
}

func TestChecks_RecentOrderAndLimit(t *testing.T) {
	m := storeAt(time.Unix(1000, 0))
	for i, at := range []int64{100, 300, 200} {
		c := &Check{EndpointSlug: "api", Status: "up", CheckedAt: at}
		if err := m.InsertCheck(ctx, c); err != nil {
			t.Fatalf("m.InsertCheck: %v", err)
		}
		if c.ID == 0 {
			t.Fatalf("InsertCheck must assign an id (case %d)", i)
		}
	}
	if err := m.InsertCheck(ctx, &Check{EndpointSlug: "other", CheckedAt: 999}); err != nil {
		t.Fatalf("m.InsertCheck: %v", err)
	}
	got, _ := m.GetRecentChecks(ctx, "api", 2)
	if len(got) != 2 || got[0].CheckedAt != 300 || got[1].CheckedAt != 200 {
		t.Fatalf("recent checks must be checked_at DESC, limited, endpoint-scoped; got %+v", got)
	}
}

func TestMaintenance_CutoffCompletedAndActiveWindow(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	m := storeAt(now)
	// active window covering "api" right now
	if err := m.InsertMaintenanceWindow(ctx, &MaintenanceWindow{Title: "DB upgrade", AffectedServices: []string{"api"}, StartAt: now.Unix() - 100, EndAt: now.Unix() + 100, Status: "in_progress"}); err != nil {
		t.Fatalf("m.InsertMaintenanceWindow: %v", err)
	}
	// completed window inside the day range
	if err := m.InsertMaintenanceWindow(ctx, &MaintenanceWindow{Title: "done", AffectedServices: []string{"api"}, StartAt: now.Unix() - 50, EndAt: now.Unix() - 10, Status: "completed"}); err != nil {
		t.Fatalf("m.InsertMaintenanceWindow: %v", err)
	}
	// old window outside the day cutoff
	if err := m.InsertMaintenanceWindow(ctx, &MaintenanceWindow{Title: "ancient", AffectedServices: []string{"api"}, StartAt: now.Unix() - 100*86400, EndAt: now.Unix() - 100*86400, Status: "scheduled"}); err != nil {
		t.Fatalf("m.InsertMaintenanceWindow: %v", err)
	}

	active, _ := m.GetMaintenanceWindows(ctx, false, 30)
	if len(active) != 1 || active[0].Title != "DB upgrade" {
		t.Fatalf("!includeCompleted within 30d must return only the active window; got %+v", active)
	}
	all, _ := m.GetMaintenanceWindows(ctx, true, 30)
	if len(all) != 2 {
		t.Fatalf("includeCompleted within 30d must return active+completed (not the ancient one); got %d", len(all))
	}
	in, title, _ := m.IsInMaintenanceWindow(ctx, "api")
	if !in || title != "DB upgrade" {
		t.Fatalf("api must be in the active window; got in=%v title=%q", in, title)
	}
	if in, _, _ := m.IsInMaintenanceWindow(ctx, "other"); in {
		t.Fatal("a service not covered must not be in a window")
	}
}

func TestIncidents_CutoffResolvedAndTimeline(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	m := storeAt(now)
	act := &Incident{Title: "down", Status: "investigating", AffectedServices: []string{"api"}, StartedAt: now.Unix() - 100}
	if err := m.InsertIncident(ctx, act); err != nil {
		t.Fatalf("m.InsertIncident: %v", err)
	}
	if err := m.InsertIncident(ctx, &Incident{Title: "fixed", Status: "resolved", StartedAt: now.Unix() - 200}); err != nil {
		t.Fatalf("m.InsertIncident: %v", err)
	}
	if err := m.InsertIncident(ctx, &Incident{Title: "old", Status: "investigating", StartedAt: now.Unix() - 400*86400}); err != nil {
		t.Fatalf("m.InsertIncident: %v", err)
	} // outside the 365d active window

	active, _ := m.GetActiveIncidents(ctx)
	if len(active) != 1 || active[0].Title != "down" {
		t.Fatalf("active incidents = unresolved within 365d; got %+v", active)
	}
	all, _ := m.GetIncidents(ctx, true, 30)
	if len(all) != 2 || all[0].StartedAt < all[1].StartedAt {
		t.Fatalf("includeResolved within 30d must return 2, started_at DESC; got %+v", all)
	}
	// timeline: an insert then read back, created_at DESC
	if err := m.InsertIncidentUpdate(ctx, &IncidentUpdate{IncidentID: act.ID, Status: "identified", Message: "root cause", CreatedAt: now.Unix()}); err != nil {
		t.Fatalf("m.InsertIncidentUpdate: %v", err)
	}
	if err := m.InsertIncidentUpdate(ctx, &IncidentUpdate{IncidentID: act.ID, Status: "monitoring", Message: "deployed fix", CreatedAt: now.Unix() + 10}); err != nil {
		t.Fatalf("m.InsertIncidentUpdate: %v", err)
	}
	ups, _ := m.GetIncidentUpdates(ctx, act.ID)
	if len(ups) != 2 || ups[0].Status != "monitoring" {
		t.Fatalf("incident timeline must return its updates created_at DESC; got %+v", ups)
	}
}

func TestUptime_EndpointAndServiceFilters(t *testing.T) {
	now := time.Unix(1_600_000_000, 0)
	m := storeAt(now)
	if err := m.UpsertEndpoint(ctx, &Endpoint{Slug: "api-1", ServiceSlug: "api"}); err != nil {
		t.Fatalf("m.UpsertEndpoint: %v", err)
	}
	if err := m.UpsertEndpoint(ctx, &Endpoint{Slug: "web-1", ServiceSlug: "web"}); err != nil {
		t.Fatalf("m.UpsertEndpoint: %v", err)
	}
	today := now.Format("2006-01-02")
	if err := m.UpsertUptimeMetric(ctx, &UptimeMetric{EndpointSlug: "api-1", Date: today, UptimePercent: 99.9}); err != nil {
		t.Fatalf("m.UpsertUptimeMetric: %v", err)
	}
	if err := m.UpsertUptimeMetric(ctx, &UptimeMetric{EndpointSlug: "web-1", Date: today, UptimePercent: 100}); err != nil {
		t.Fatalf("m.UpsertUptimeMetric: %v", err)
	}

	if got, _ := m.GetUptimeMetrics(ctx, "api-1", "", 7); len(got) != 1 || got[0].EndpointSlug != "api-1" {
		t.Fatalf("endpoint filter must narrow to one; got %+v", got)
	}
	if got, _ := m.GetUptimeMetrics(ctx, "", "web", 7); len(got) != 1 || got[0].EndpointSlug != "web-1" {
		t.Fatalf("service filter must widen to that service's endpoints; got %+v", got)
	}
	if got, _ := m.GetUptimeMetrics(ctx, "", "", 7); len(got) != 2 {
		t.Fatalf("no filter must return all; got %d", len(got))
	}
}

func TestSubscribers_LifecycleAndOpacity(t *testing.T) {
	now := time.Unix(1000, 0)
	m := storeAt(now)
	// the store only ever sees ciphertext + hashes (crypto is a service-layer seam)
	s := &Subscriber{EmailEncrypted: "ENC", EmailHash: "H", VerifyTokenHash: "VT", UnsubTokenHash: "UT"}
	if err := m.InsertSubscriber(ctx, s); err != nil {
		t.Fatalf("m.InsertSubscriber: %v", err)
	}
	if s.ID == "" {
		t.Fatal("InsertSubscriber must assign an id")
	}
	if exists, _ := m.SubscriberExistsByEmailHash(ctx, "H"); !exists {
		t.Fatal("existing email hash must be found")
	}
	// not verified yet → not in verified set
	if v, _ := m.GetVerifiedSubscribers(ctx); len(v) != 0 {
		t.Fatal("an unverified subscriber must not be in the verified set")
	}
	if err := m.VerifySubscriber(ctx, "VT"); err != nil {
		t.Fatalf("VerifySubscriber: %v", err)
	}
	if v, _ := m.GetVerifiedSubscribers(ctx); len(v) != 1 {
		t.Fatal("a verified subscriber must appear in the verified set")
	}
	if err := m.UnsubscribeByToken(ctx, "UT"); err != nil {
		t.Fatalf("UnsubscribeByToken: %v", err)
	}
	if v, _ := m.GetVerifiedSubscribers(ctx); len(v) != 0 {
		t.Fatal("an unsubscribed subscriber must drop out of the verified set")
	}
	if err := m.VerifySubscriber(ctx, "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatal("an unknown verify token must be ErrNotFound")
	}
}
