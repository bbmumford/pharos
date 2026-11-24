package pharos

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// flakyEndpoints answers the endpoint list once and then fails, as a store
// that loses its connection mid-cycle does.
type flakyEndpoints struct {
	*MemStore
	mu    sync.Mutex
	reads int
}

func (f *flakyEndpoints) GetAllEndpoints(ctx context.Context) ([]Endpoint, error) {
	f.mu.Lock()
	f.reads++
	n := f.reads
	f.mu.Unlock()
	if n > 1 {
		return nil, errors.New("store unreachable")
	}
	return f.MemStore.GetAllEndpoints(ctx)
}

// A check cycle in store mode survives the endpoint read failing after the
// cycle began: every endpoint of the list it started with is checked.
func TestChecker_RunOnceSurvivesAFailedEndpointRead(t *testing.T) {
	mem := NewMemStore()
	mem.now = at1000
	for _, slug := range []string{"api", "web"} {
		if err := mem.UpsertEndpoint(ctx, &Endpoint{Slug: slug, Name: slug, URL: "http://" + slug, Enabled: true}); err != nil {
			t.Fatal(err)
		}
	}
	store := &flakyEndpoints{MemStore: mem}
	h, err := NewHealthChecker(CheckerConfig{Store: store, Prober: &fakeProber{res: ProbeResult{Status: StatusOperational, StatusCode: 200}}, Now: at1000})
	if err != nil {
		t.Fatal(err)
	}
	func() {
		defer func() {
			if p := recover(); p != nil {
				t.Fatalf("RunOnce panicked when the endpoint read failed mid-cycle: %v", p)
			}
		}()
		h.RunOnce(ctx)
	}()
	for _, slug := range []string{"api", "web"} {
		if checks, _ := mem.GetRecentChecks(ctx, slug, 1); len(checks) != 1 {
			t.Fatalf("endpoint %s was not checked", slug)
		}
	}
}
