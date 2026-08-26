package pharos

import (
	"testing"
	"time"
)

func TestAggregator_UptimeAndAverageForDate(t *testing.T) {
	store := NewMemStore()
	store.now = at1000
	// day bucket for 2021-01-01 (UTC midnight) = 1609459200; place checks at noon that day.
	const noon = int64(1609459200 + 43200)
	for i, s := range []struct {
		status string
		rtt    int
	}{
		{StatusOperational, 10},
		{StatusOperational, 20},
		{StatusOperational, 30},
		{StatusMajorOutage, 40},
	} {
		store.InsertCheck(ctx, &Check{EndpointSlug: "api", Status: s.status, ResponseTimeMS: s.rtt, CheckedAt: noon + int64(i)})
	}
	// a check on a DIFFERENT day must be excluded
	store.InsertCheck(ctx, &Check{EndpointSlug: "api", Status: StatusMajorOutage, ResponseTimeMS: 999, CheckedAt: noon + 2*86400})

	a, err := NewUptimeAggregator(AggregatorConfig{Store: store, Now: at1000})
	if err != nil {
		t.Fatal(err)
	}
	a.AggregateForDate(ctx, "api", "2021-01-01")

	got, _ := store.GetUptimeMetrics(ctx, "api", "", 100000)
	if len(got) != 1 {
		t.Fatalf("one metric must be written; got %d", len(got))
	}
	m := got[0]
	if m.TotalChecks != 4 || m.SuccessfulChecks != 3 {
		t.Fatalf("only that day's 4 checks (3 operational) count; got total=%d ok=%d", m.TotalChecks, m.SuccessfulChecks)
	}
	if m.UptimePercent != 75.0 {
		t.Fatalf("uptime%% must be 3/4=75; got %v", m.UptimePercent)
	}
	if m.AvgResponseTimeMS != 25 {
		t.Fatalf("avg response = (10+20+30+40)/4 = 25; got %d", m.AvgResponseTimeMS)
	}
}

func TestAggregator_NoChecksWritesNothing(t *testing.T) {
	store := NewMemStore()
	store.now = at1000
	a, _ := NewUptimeAggregator(AggregatorConfig{Store: store, Now: at1000})
	a.AggregateForDate(ctx, "api", "2021-01-01")
	if got, _ := store.GetUptimeMetrics(ctx, "api", "", 100000); len(got) != 0 {
		t.Fatalf("a day with no checks must write no metric; got %d", len(got))
	}
}

func TestAggregator_RunOnceCoversTodayAndYesterday(t *testing.T) {
	fixed := func() time.Time { return time.Unix(1609545600+43200, 0) } // 2021-01-02 noon
	store := NewMemStore()
	store.now = fixed
	store.UpsertEndpoint(ctx, &Endpoint{Slug: "api", ServiceSlug: "svc"})
	store.InsertCheck(ctx, &Check{EndpointSlug: "api", Status: StatusOperational, CheckedAt: 1609459200 + 43200}) // 2021-01-01
	store.InsertCheck(ctx, &Check{EndpointSlug: "api", Status: StatusOperational, CheckedAt: 1609545600 + 43200}) // 2021-01-02
	a, _ := NewUptimeAggregator(AggregatorConfig{Store: store, Now: fixed})
	a.RunOnce(ctx)
	got, _ := store.GetUptimeMetrics(ctx, "api", "", 100000)
	if len(got) != 2 {
		t.Fatalf("RunOnce must aggregate today AND yesterday; got %d metrics", len(got))
	}
}
