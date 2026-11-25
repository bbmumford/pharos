package pharos

import (
	"database/sql"
	"errors"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func newSQLStore(t *testing.T, now time.Time) *SQLStore {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1) // keep the :memory: DB on a single connection
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("db.Close: %v", err)
		}
	})
	s := NewSQLStore(db)
	s.now = func() time.Time { return now }
	seq := 0
	s.newID = func() string { seq++; return "id-" + string(rune('a'+seq-1)) }
	if err := s.InitSchema(ctx); err != nil {
		t.Fatalf("InitSchema: %v", err)
	}
	return s
}

func TestSQLStore_ServiceEndpointDerivedStatusAndEnabled(t *testing.T) {
	s := newSQLStore(t, time.Unix(1000, 0))
	if err := s.UpsertService(ctx, &Service{Slug: "api", Name: "API", DisplayOrder: 1}); err != nil {
		t.Fatalf("s.UpsertService: %v", err)
	}
	if err := s.UpsertEndpoint(ctx, &Endpoint{Slug: "on", ServiceSlug: "api", Name: "on", URL: "http://x", Enabled: true}); err != nil {
		t.Fatalf("s.UpsertEndpoint: %v", err)
	}
	if err := s.UpsertEndpoint(ctx, &Endpoint{Slug: "off", ServiceSlug: "api", Name: "off", URL: "http://y", Enabled: false}); err != nil {
		t.Fatalf("s.UpsertEndpoint: %v", err)
	}
	if err := s.InsertCheck(ctx, &Check{EndpointSlug: "on", Status: StatusOperational, HTTPStatus: StatusOperational, CheckedAt: 100}); err != nil {
		t.Fatalf("s.InsertCheck: %v", err)
	}
	if err := s.InsertCheck(ctx, &Check{EndpointSlug: "on", Status: StatusMajorOutage, HTTPStatus: StatusMajorOutage, ResponseTimeMS: 7, CheckedAt: 200}); err != nil {
		t.Fatalf("s.InsertCheck: %v", err)
	}

	all, err := s.GetAllEndpoints(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 || all[0].Slug != "on" {
		t.Fatalf("GetAllEndpoints returns only enabled; got %+v", all)
	}
	if all[0].LastStatus != StatusMajorOutage || all[0].LastCheckAt != 200 || all[0].LastResponseMS != 7 {
		t.Fatalf("last status must derive from the newest check; got %q at=%d ms=%d", all[0].LastStatus, all[0].LastCheckAt, all[0].LastResponseMS)
	}
	svc, _ := s.GetEndpointsByService(ctx, "api")
	if len(svc) != 2 {
		t.Fatalf("GetEndpointsByService includes disabled; got %d", len(svc))
	}
	if _, err := s.GetServiceBySlug(ctx, "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatal("unknown service must be ErrNotFound")
	}
	if got, _ := s.GetRecentChecks(ctx, "on", 1); len(got) != 1 || got[0].CheckedAt != 200 {
		t.Fatalf("recent checks must be checked_at DESC; got %+v", got)
	}
}

func TestSQLStore_MaintenanceAndIncidentAndUptime(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	s := newSQLStore(t, now)
	// maintenance active window covering "api"
	if err := s.InsertMaintenanceWindow(ctx, &MaintenanceWindow{Title: "m", AffectedServices: []string{"api"}, StartAt: now.Unix() - 100, EndAt: now.Unix() + 100, Status: "in_progress"}); err != nil {
		t.Fatalf("s.InsertMaintenanceWindow: %v", err)
	}
	if in, title, _ := s.IsInMaintenanceWindow(ctx, "api"); !in || title != "m" {
		t.Fatalf("api must be in the active window; in=%v title=%q", in, title)
	}
	act, _ := s.GetMaintenanceWindows(ctx, false, 30)
	if len(act) != 1 || len(act[0].AffectedServices) != 1 || act[0].AffectedServices[0] != "api" {
		t.Fatalf("maintenance JSON affected-services must round-trip; got %+v", act)
	}
	// incident lifecycle
	inc := &Incident{Title: "down", Status: "investigating", AffectedServices: []string{"api"}, StartedAt: now.Unix() - 10}
	if err := s.InsertIncident(ctx, inc); err != nil {
		t.Fatalf("s.InsertIncident: %v", err)
	}
	if inc.ID == "" {
		t.Fatal("InsertIncident must assign an id")
	}
	if err := s.InsertIncidentUpdate(ctx, &IncidentUpdate{IncidentID: inc.ID, Status: "identified", Message: "cause", CreatedAt: now.Unix()}); err != nil {
		t.Fatalf("s.InsertIncidentUpdate: %v", err)
	}
	if ups, _ := s.GetIncidentUpdates(ctx, inc.ID); len(ups) != 1 {
		t.Fatal("incident update must round-trip")
	}
	if a, _ := s.GetActiveIncidents(ctx); len(a) != 1 {
		t.Fatalf("one active incident; got %d", len(a))
	}
	if err := s.UpdateIncident(ctx, &Incident{ID: inc.ID, Title: "up", Status: "resolved", AffectedServices: []string{"api"}, ResolvedAt: now.Unix()}); err != nil {
		t.Fatalf("s.UpdateIncident: %v", err)
	}
	if a, _ := s.GetActiveIncidents(ctx); len(a) != 0 {
		t.Fatal("resolved incident must drop out of active")
	}
	// uptime filters
	today := now.Format("2006-01-02")
	if err := s.UpsertEndpoint(ctx, &Endpoint{Slug: "api-1", ServiceSlug: "api", Name: "api-1", URL: "http://z", Enabled: true}); err != nil {
		t.Fatalf("s.UpsertEndpoint: %v", err)
	}
	if err := s.UpsertUptimeMetric(ctx, &UptimeMetric{EndpointSlug: "api-1", Date: today, UptimePercent: 99.5, TotalChecks: 10, SuccessfulChecks: 9}); err != nil {
		t.Fatalf("s.UpsertUptimeMetric: %v", err)
	}
	if m, _ := s.GetUptimeMetrics(ctx, "", "api", 7); len(m) != 1 || m[0].UptimePercent != 99.5 {
		t.Fatalf("service uptime filter must find api-1's metric; got %+v", m)
	}
}

func TestSQLStore_SubscriberLifecycle(t *testing.T) {
	s := newSQLStore(t, time.Unix(1000, 0))
	sub := &Subscriber{EmailEncrypted: "enc:a", EmailHash: "ha", VerifyTokenHash: "vh", UnsubTokenHash: "uh"}
	if err := s.InsertSubscriber(ctx, sub); err != nil {
		t.Fatal(err)
	}
	if ok, _ := s.SubscriberExistsByEmailHash(ctx, "ha"); !ok {
		t.Fatal("must exist by email hash")
	}
	if v, _ := s.GetVerifiedSubscribers(ctx); len(v) != 0 {
		t.Fatal("unverified subscriber must not be in the verified set")
	}
	if err := s.VerifySubscriber(ctx, "vh"); err != nil {
		t.Fatal(err)
	}
	if v, _ := s.GetVerifiedSubscribers(ctx); len(v) != 1 || v[0].EmailEncrypted != "enc:a" {
		t.Fatalf("verified set must carry the ciphertext; got %+v", v)
	}
	if err := s.VerifySubscriber(ctx, "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatal("unknown verify token must be ErrNotFound")
	}
	if err := s.UnsubscribeByToken(ctx, "uh"); err != nil {
		t.Fatal(err)
	}
	if v, _ := s.GetVerifiedSubscribers(ctx); len(v) != 0 {
		t.Fatal("unsubscribed must drop out of the verified set")
	}
}
