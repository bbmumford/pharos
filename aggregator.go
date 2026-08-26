package pharos

import (
	"context"
	"sync"
	"time"
)

// AggregatorConfig wires an UptimeAggregator. Store is required.
type AggregatorConfig struct {
	Store StatusStore
	// Interval between aggregation runs in the Start loop (default 1h).
	Interval time.Duration
	// SampleLimit is how many recent checks per endpoint to scan (default 2880 ≈ one day at 30s).
	SampleLimit int
	// Now is the clock (default time.Now); injectable for tests.
	Now func() time.Time
	// Logf is an optional diagnostic sink; nil ⇒ silent.
	Logf func(string, ...any)
}

// UptimeAggregator rolls health checks up into daily uptime metrics. Each run aggregates today and
// yesterday (yesterday too, to backfill checks a prior run missed) for every endpoint.
type UptimeAggregator struct {
	store       StatusStore
	interval    time.Duration
	sampleLimit int
	now         func() time.Time
	logf        func(string, ...any)
	stopCh      chan struct{}
	wg          sync.WaitGroup
}

// NewUptimeAggregator validates the config and returns an aggregator.
func NewUptimeAggregator(c AggregatorConfig) (*UptimeAggregator, error) {
	if c.Store == nil {
		return nil, &configError{"pharos: AggregatorConfig.Store is required"}
	}
	a := &UptimeAggregator{
		store:       c.Store,
		interval:    c.Interval,
		sampleLimit: c.SampleLimit,
		now:         c.Now,
		logf:        c.Logf,
		stopCh:      make(chan struct{}),
	}
	if a.interval <= 0 {
		a.interval = time.Hour
	}
	if a.sampleLimit <= 0 {
		a.sampleLimit = 2880
	}
	if a.now == nil {
		a.now = time.Now
	}
	if a.logf == nil {
		a.logf = func(string, ...any) {}
	}
	return a, nil
}

// Start runs an initial aggregation then repeats every Interval until Stop.
func (a *UptimeAggregator) Start() {
	a.wg.Add(1)
	defer a.wg.Done()
	a.RunOnce(context.Background())
	ticker := time.NewTicker(a.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			a.RunOnce(context.Background())
		case <-a.stopCh:
			return
		}
	}
}

// Stop halts the loop and waits for it to exit.
func (a *UptimeAggregator) Stop() {
	close(a.stopCh)
	a.wg.Wait()
}

// RunOnce aggregates today and yesterday for every endpoint. Exported so a consumer can drive it and
// for testing.
func (a *UptimeAggregator) RunOnce(ctx context.Context) {
	endpoints, err := a.store.GetAllEndpoints(ctx)
	if err != nil {
		a.logf("aggregator: get endpoints: %v", err)
		return
	}
	today := a.now().Format("2006-01-02")
	yesterday := a.now().AddDate(0, 0, -1).Format("2006-01-02")
	for _, ep := range endpoints {
		a.AggregateForDate(ctx, ep.Slug, today)
		a.AggregateForDate(ctx, ep.Slug, yesterday)
	}
}

// AggregateForDate computes and upserts one endpoint's uptime metric for one UTC day ("YYYY-MM-DD"):
// uptime% = operational / total over the checks whose instant falls in that day, plus average
// response time. A day with no checks writes nothing. Exported for one-off recompute and testing.
func (a *UptimeAggregator) AggregateForDate(ctx context.Context, endpointSlug, date string) {
	checks, err := a.store.GetRecentChecks(ctx, endpointSlug, a.sampleLimit)
	if err != nil {
		a.logf("aggregator: get checks %s: %v", endpointSlug, err)
		return
	}
	dateStart, err := time.Parse("2006-01-02", date)
	if err != nil {
		a.logf("aggregator: bad date %q: %v", date, err)
		return
	}
	dateEnd := dateStart.AddDate(0, 0, 1)

	var total, successful int
	var totalResponse int64
	for _, c := range checks {
		t := time.Unix(c.CheckedAt, 0)
		if t.Before(dateStart) || t.After(dateEnd) {
			continue
		}
		total++
		if c.Status == StatusOperational {
			successful++
		}
		totalResponse += int64(c.ResponseTimeMS)
	}
	if total == 0 {
		return
	}
	if err := a.store.UpsertUptimeMetric(ctx, &UptimeMetric{
		EndpointSlug:      endpointSlug,
		Date:              date,
		UptimePercent:     float64(successful) / float64(total) * 100,
		TotalChecks:       total,
		SuccessfulChecks:  successful,
		AvgResponseTimeMS: int(totalResponse / int64(total)),
	}); err != nil {
		a.logf("aggregator: upsert metric %s %s: %v", endpointSlug, date, err)
	}
}
