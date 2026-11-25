package pharos

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sort"
	"strings"
	"sync"
	"time"
)

// MemStore is an in-memory reference StatusStore — the test/dev backend and the semantic reference a
// durable (SQLite/Turso) store must match. Thread-safe; clock and id generator are injectable for
// deterministic tests. It implements the query semantics the contract requires: recent-checks
// ordering/limit, the maintenance/incident day-cutoff and status filters, and the
// active-maintenance-window lookup.
type MemStore struct {
	mu              sync.Mutex
	services        map[string]*Service
	endpoints       map[string]*Endpoint
	checks          []*Check
	nextCheckID     int64
	maintenance     map[string]*MaintenanceWindow
	incidents       map[string]*Incident
	incidentUpdates []*IncidentUpdate
	uptime          map[string]*UptimeMetric // key: endpointSlug + "\x00" + date
	statusLog       []statusChange
	subscribers     map[string]*Subscriber // by id
	now             func() time.Time
	newID           func() string
}

type statusChange struct {
	endpointSlug, oldStatus, newStatus, reason, incidentID string
	createdAt                                              int64
}

// NewMemStore builds an empty in-memory status store.
func NewMemStore() *MemStore {
	return &MemStore{
		services:    map[string]*Service{},
		endpoints:   map[string]*Endpoint{},
		maintenance: map[string]*MaintenanceWindow{},
		incidents:   map[string]*Incident{},
		uptime:      map[string]*UptimeMetric{},
		subscribers: map[string]*Subscriber{},
		now:         time.Now,
		newID:       randomID,
	}
}

func randomID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func uptimeKey(slug, date string) string { return slug + "\x00" + date }

var _ StatusStore = (*MemStore)(nil)

// ── Services ──

func (m *MemStore) GetAllServices(_ context.Context) ([]Service, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Service, 0, len(m.services))
	for _, s := range m.services {
		out = append(out, *s)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].DisplayOrder != out[j].DisplayOrder {
			return out[i].DisplayOrder < out[j].DisplayOrder
		}
		return out[i].Slug < out[j].Slug
	})
	return out, nil
}

func (m *MemStore) GetServiceBySlug(_ context.Context, slug string) (*Service, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.services[slug]
	if !ok {
		return nil, ErrNotFound
	}
	cp := *s
	return &cp, nil
}

// UpsertService lets a seed or consumer populate services through the contract, keeping seeding
// store-agnostic (GetAllServices/GetServiceBySlug are read-only).
func (m *MemStore) UpsertService(_ context.Context, s *Service) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := *s
	if cp.CreatedAt == 0 {
		cp.CreatedAt = m.now().Unix()
	}
	m.services[cp.Slug] = &cp
	return nil
}

// ── Endpoints ──

// derived returns e with LastStatus/LastHTTPStatus/LastCheckAt/LastResponseMS set from its most
// recent check ("unknown" when never checked). An endpoint's last status is defined as its latest
// check's status: these four fields are not persisted on the endpoint, they are computed on read, so
// UpsertEndpoint never has to be re-issued when a check arrives. Caller holds m.mu.
func (m *MemStore) derived(e Endpoint) Endpoint {
	var latest *Check
	for _, c := range m.checks {
		if c.EndpointSlug != e.Slug {
			continue
		}
		if latest == nil || c.CheckedAt > latest.CheckedAt || (c.CheckedAt == latest.CheckedAt && c.ID > latest.ID) {
			latest = c
		}
	}
	if latest == nil {
		e.LastStatus, e.LastHTTPStatus, e.LastCheckAt, e.LastResponseMS = "unknown", "", 0, 0
	} else {
		e.LastStatus, e.LastHTTPStatus, e.LastCheckAt, e.LastResponseMS = latest.Status, latest.HTTPStatus, latest.CheckedAt, latest.ResponseTimeMS
	}
	return e
}

// GetEndpointsByService returns every endpoint for a service (enabled or not), ordered by name, each
// with its derived last-check status.
func (m *MemStore) GetEndpointsByService(_ context.Context, serviceSlug string) ([]Endpoint, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Endpoint
	for _, e := range m.endpoints {
		if e.ServiceSlug == serviceSlug {
			out = append(out, m.derived(*e))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// GetAllEndpoints returns only ENABLED endpoints, ordered by service+name, each with its derived
// last-check status. The health checker and the overall-status roll-up consume this, so a disabled
// endpoint neither gets probed nor affects a service's status.
func (m *MemStore) GetAllEndpoints(_ context.Context) ([]Endpoint, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Endpoint
	for _, e := range m.endpoints {
		if !e.Enabled {
			continue
		}
		out = append(out, m.derived(*e))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ServiceSlug != out[j].ServiceSlug {
			return out[i].ServiceSlug < out[j].ServiceSlug
		}
		return out[i].Name < out[j].Name
	})
	return out, nil
}

func (m *MemStore) GetEndpointBySlug(_ context.Context, slug string) (*Endpoint, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.endpoints[slug]
	if !ok {
		return nil, ErrNotFound
	}
	cp := m.derived(*e)
	return &cp, nil
}

func (m *MemStore) UpsertEndpoint(_ context.Context, e *Endpoint) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := *e
	if cp.CreatedAt == 0 {
		cp.CreatedAt = m.now().Unix()
	}
	m.endpoints[cp.Slug] = &cp
	return nil
}

// ── Health checks ──

func (m *MemStore) InsertCheck(_ context.Context, c *Check) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextCheckID++
	cp := *c
	cp.ID = m.nextCheckID
	if cp.CheckedAt == 0 {
		cp.CheckedAt = m.now().Unix()
	}
	m.checks = append(m.checks, &cp)
	c.ID = cp.ID // reflect the assigned id back to the caller, as an autoincrement insert does
	return nil
}

func (m *MemStore) GetRecentChecks(_ context.Context, endpointSlug string, limit int) ([]Check, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Check
	for _, c := range m.checks {
		if c.EndpointSlug == endpointSlug {
			out = append(out, *c)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CheckedAt != out[j].CheckedAt {
			return out[i].CheckedAt > out[j].CheckedAt // checked_at DESC
		}
		return out[i].ID > out[j].ID
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// ── Maintenance windows ──

func (m *MemStore) GetMaintenanceWindows(_ context.Context, includeCompleted bool, days int) ([]MaintenanceWindow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cutoff := m.now().AddDate(0, 0, -days).Unix()
	var out []MaintenanceWindow
	for _, w := range m.maintenance {
		if w.StartAt < cutoff && w.EndAt < cutoff {
			continue
		}
		if !includeCompleted && (w.Status == "completed" || w.Status == "cancelled") {
			continue
		}
		out = append(out, *cloneMW(w))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].StartAt < out[j].StartAt }) // start_at ASC
	return out, nil
}

func (m *MemStore) InsertMaintenanceWindow(_ context.Context, w *MaintenanceWindow) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := cloneMW(w)
	if cp.ID == "" {
		cp.ID = m.newID()
	}
	if cp.Status == "" {
		cp.Status = "scheduled"
	}
	if cp.CreatedAt == 0 {
		cp.CreatedAt = m.now().Unix()
	}
	m.maintenance[cp.ID] = cp
	w.ID = cp.ID
	return nil
}

func (m *MemStore) UpdateMaintenanceWindow(_ context.Context, w *MaintenanceWindow) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.maintenance[w.ID]; !ok {
		return ErrNotFound
	}
	m.maintenance[w.ID] = cloneMW(w)
	return nil
}

func (m *MemStore) IsInMaintenanceWindow(_ context.Context, serviceSlug string) (bool, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now().Unix()
	var best *MaintenanceWindow
	for _, w := range m.maintenance {
		if w.Status != "scheduled" && w.Status != "in_progress" {
			continue
		}
		if w.StartAt > now || w.EndAt < now {
			continue
		}
		if !contains(w.AffectedServices, serviceSlug) {
			continue
		}
		if best == nil || w.StartAt < best.StartAt {
			best = w
		}
	}
	if best == nil {
		return false, "", nil
	}
	return true, best.Title, nil
}

// ── Incidents ──

func (m *MemStore) GetIncidents(_ context.Context, includeResolved bool, days int) ([]Incident, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cutoff := m.now().AddDate(0, 0, -days).Unix()
	var out []Incident
	for _, i := range m.incidents {
		if i.StartedAt < cutoff {
			continue
		}
		if !includeResolved && i.Status == "resolved" {
			continue
		}
		out = append(out, *cloneIncident(i))
	}
	sort.Slice(out, func(a, b int) bool { return out[a].StartedAt > out[b].StartedAt }) // started_at DESC
	return out, nil
}

func (m *MemStore) GetActiveIncidents(ctx context.Context) ([]Incident, error) {
	return m.GetIncidents(ctx, false, 365)
}

func (m *MemStore) InsertIncident(_ context.Context, i *Incident) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := cloneIncident(i)
	if cp.ID == "" {
		cp.ID = m.newID()
	}
	if cp.Status == "" {
		cp.Status = "investigating"
	}
	if cp.StartedAt == 0 {
		cp.StartedAt = m.now().Unix()
	}
	if cp.CreatedAt == 0 {
		cp.CreatedAt = m.now().Unix()
	}
	m.incidents[cp.ID] = cp
	i.ID = cp.ID
	return nil
}

func (m *MemStore) UpdateIncident(_ context.Context, i *Incident) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.incidents[i.ID]; !ok {
		return ErrNotFound
	}
	m.incidents[i.ID] = cloneIncident(i)
	return nil
}

func (m *MemStore) DeleteIncident(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.incidents[id]; !ok {
		return ErrNotFound
	}
	delete(m.incidents, id)
	return nil
}

func (m *MemStore) DeleteAllIncidents(_ context.Context) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := len(m.incidents)
	m.incidents = map[string]*Incident{}
	return n, nil
}

func (m *MemStore) GetIncidentUpdates(_ context.Context, incidentID string) ([]IncidentUpdate, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []IncidentUpdate
	for _, u := range m.incidentUpdates {
		if u.IncidentID == incidentID {
			out = append(out, *u)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt > out[j].CreatedAt }) // created_at DESC
	return out, nil
}

func (m *MemStore) InsertIncidentUpdate(_ context.Context, u *IncidentUpdate) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := *u
	if cp.ID == "" {
		cp.ID = m.newID()
	}
	if cp.CreatedAt == 0 {
		cp.CreatedAt = m.now().Unix()
	}
	m.incidentUpdates = append(m.incidentUpdates, &cp)
	u.ID = cp.ID
	return nil
}

// ── Uptime metrics ──

func (m *MemStore) GetUptimeMetrics(_ context.Context, endpointSlug, serviceSlug string, days int) ([]UptimeMetric, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// serviceSlug widens the set to every endpoint in that service; endpointSlug narrows to one; when
	// both are empty, all endpoints are included.
	var slugs map[string]bool
	if endpointSlug != "" {
		slugs = map[string]bool{endpointSlug: true}
	} else if serviceSlug != "" {
		slugs = map[string]bool{}
		for _, e := range m.endpoints {
			if e.ServiceSlug == serviceSlug {
				slugs[e.Slug] = true
			}
		}
	}
	cutoffDate := m.now().AddDate(0, 0, -days).Format("2006-01-02")
	var out []UptimeMetric
	for _, u := range m.uptime {
		if slugs != nil && !slugs[u.EndpointSlug] {
			continue
		}
		if u.Date < cutoffDate {
			continue
		}
		out = append(out, *u)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Date > out[j].Date }) // date DESC
	return out, nil
}

func (m *MemStore) UpsertUptimeMetric(_ context.Context, u *UptimeMetric) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := *u
	m.uptime[uptimeKey(cp.EndpointSlug, cp.Date)] = &cp
	return nil
}

// ── Status-change audit log ──

func (m *MemStore) InsertStatusChange(_ context.Context, endpointSlug, oldStatus, newStatus, reason, incidentID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.statusLog = append(m.statusLog, statusChange{
		endpointSlug: endpointSlug, oldStatus: oldStatus, newStatus: newStatus,
		reason: reason, incidentID: incidentID, createdAt: m.now().Unix(),
	})
	return nil
}

// ── Subscribers ──

func (m *MemStore) InsertSubscriber(_ context.Context, s *Subscriber) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := *s
	if cp.ID == "" {
		cp.ID = m.newID()
	}
	if cp.CreatedAt == 0 {
		cp.CreatedAt = m.now().Unix()
	}
	m.subscribers[cp.ID] = &cp
	s.ID = cp.ID
	return nil
}

func (m *MemStore) VerifySubscriber(_ context.Context, verifyTokenHash string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, s := range m.subscribers {
		if s.VerifyTokenHash == verifyTokenHash {
			s.Verified = true
			s.VerifiedAt = m.now().Unix()
			return nil
		}
	}
	return ErrNotFound
}

func (m *MemStore) UnsubscribeByToken(_ context.Context, unsubTokenHash string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, s := range m.subscribers {
		if s.UnsubTokenHash == unsubTokenHash {
			s.UnsubscribedAt = m.now().Unix()
			return nil
		}
	}
	return ErrNotFound
}

func (m *MemStore) GetVerifiedSubscribers(_ context.Context) ([]Subscriber, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Subscriber
	for _, s := range m.subscribers {
		if s.Verified && s.UnsubscribedAt == 0 {
			out = append(out, *s)
		}
	}
	return out, nil
}

func (m *MemStore) SubscriberExistsByEmailHash(_ context.Context, emailHash string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, s := range m.subscribers {
		if s.EmailHash == emailHash {
			return true, nil
		}
	}
	return false, nil
}

// ── helpers ──

func cloneMW(w *MaintenanceWindow) *MaintenanceWindow {
	cp := *w
	cp.AffectedServices = append([]string(nil), w.AffectedServices...)
	return &cp
}

func cloneIncident(i *Incident) *Incident {
	cp := *i
	cp.AffectedServices = append([]string(nil), i.AffectedServices...)
	return &cp
}

func contains(ss []string, s string) bool {
	for _, x := range ss {
		if strings.EqualFold(x, s) {
			return true
		}
	}
	return false
}
