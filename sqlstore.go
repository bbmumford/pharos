package pharos

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

// SQLStore is the durable default StatusStore over any database/sql driver — the shipped "standalone
// unit" store. The package imports only database/sql (stdlib); the APP supplies the opened *sql.DB and
// registers the driver (go-libsql/Turso, modernc SQLite, Postgres, …), so pharos pulls no driver
// dependency yet works out of the box. Endpoint last-status is derived from the latest check (a LEFT
// JOIN), affected-services are JSON, subscriber
// emails/tokens are stored exactly as given (opaque — the crypto is the subscriber-service seam). The
// StatusStore interface remains the escape-hatch for mesh-gateway or in-memory backends (MemStore).
type SQLStore struct {
	db    *sql.DB
	now   func() time.Time
	newID func() string
}

// NewSQLStore builds a store over an opened *sql.DB. Call InitSchema once before use.
func NewSQLStore(db *sql.DB) *SQLStore {
	return &SQLStore{db: db, now: time.Now, newID: randomID}
}

var _ StatusStore = (*SQLStore)(nil)

// sqlSchema is the idempotent DDL (pharos_* tables). A status page has no axis, so no owner/tenant
// column — the model is global.
var sqlSchema = []string{
	`CREATE TABLE IF NOT EXISTS pharos_services (slug TEXT PRIMARY KEY, name TEXT NOT NULL, description TEXT, display_order INTEGER DEFAULT 0, created_at INTEGER DEFAULT (unixepoch()))`,
	`CREATE TABLE IF NOT EXISTS pharos_endpoints (slug TEXT PRIMARY KEY, name TEXT NOT NULL, url TEXT NOT NULL, type TEXT NOT NULL DEFAULT 'http', service_slug TEXT NOT NULL, interval_seconds INTEGER DEFAULT 30, timeout_ms INTEGER DEFAULT 5000, enabled INTEGER DEFAULT 1, created_at INTEGER DEFAULT (unixepoch()))`,
	`CREATE TABLE IF NOT EXISTS pharos_checks (id INTEGER PRIMARY KEY AUTOINCREMENT, endpoint_slug TEXT NOT NULL, status TEXT NOT NULL, http_status TEXT NOT NULL DEFAULT '', response_time_ms INTEGER, status_code INTEGER, error_message TEXT, checked_at INTEGER DEFAULT (unixepoch()), node_id TEXT)`,
	`CREATE INDEX IF NOT EXISTS idx_pharos_checks_endpoint_time ON pharos_checks(endpoint_slug, checked_at DESC)`,
	`CREATE INDEX IF NOT EXISTS idx_pharos_checks_time ON pharos_checks(checked_at DESC)`,
	`CREATE TABLE IF NOT EXISTS pharos_maintenance_windows (id TEXT PRIMARY KEY, title TEXT NOT NULL, description TEXT, affected_services TEXT NOT NULL, start_at INTEGER NOT NULL, end_at INTEGER NOT NULL, status TEXT DEFAULT 'scheduled', created_at INTEGER DEFAULT (unixepoch()))`,
	`CREATE TABLE IF NOT EXISTS pharos_incidents (id TEXT PRIMARY KEY, title TEXT NOT NULL, status TEXT DEFAULT 'investigating', impact TEXT DEFAULT 'degraded', affected_services TEXT NOT NULL, started_at INTEGER DEFAULT (unixepoch()), resolved_at INTEGER, created_at INTEGER DEFAULT (unixepoch()))`,
	`CREATE TABLE IF NOT EXISTS pharos_incident_updates (id TEXT PRIMARY KEY, incident_id TEXT NOT NULL, status TEXT NOT NULL, message TEXT NOT NULL, created_at INTEGER DEFAULT (unixepoch()))`,
	`CREATE TABLE IF NOT EXISTS pharos_uptime_metrics (endpoint_slug TEXT NOT NULL, date TEXT NOT NULL, uptime_percent REAL DEFAULT 100.0, total_checks INTEGER DEFAULT 0, successful_checks INTEGER DEFAULT 0, avg_response_time_ms INTEGER, PRIMARY KEY (endpoint_slug, date))`,
	`CREATE TABLE IF NOT EXISTS pharos_status_log (id INTEGER PRIMARY KEY AUTOINCREMENT, endpoint_slug TEXT NOT NULL, old_status TEXT, new_status TEXT NOT NULL, reason TEXT, incident_id TEXT, created_at INTEGER DEFAULT (unixepoch()))`,
	`CREATE TABLE IF NOT EXISTS pharos_subscribers (id TEXT PRIMARY KEY, email_encrypted TEXT NOT NULL, email_hash TEXT NOT NULL UNIQUE, verify_token_hash TEXT NOT NULL, unsub_token_hash TEXT NOT NULL, verified INTEGER NOT NULL DEFAULT 0, created_at INTEGER NOT NULL DEFAULT (unixepoch()), verified_at INTEGER, unsubscribed_at INTEGER)`,
}

// InitSchema creates the tables/indexes (idempotent); run once at boot before use.
func (s *SQLStore) InitSchema(ctx context.Context) error {
	for _, stmt := range sqlSchema {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func marshalServices(ss []string) string {
	b, _ := json.Marshal(ss)
	return string(b)
}

func unmarshalServices(s string) []string {
	var out []string
	if json.Unmarshal([]byte(s), &out) == nil {
		return out
	}
	if s == "" {
		return nil
	}
	return strings.Split(s, ",")
}

// ── Services ──

func (s *SQLStore) GetAllServices(ctx context.Context) ([]Service, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT slug, name, description, display_order, created_at FROM pharos_services ORDER BY display_order ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Service
	for rows.Next() {
		var v Service
		if err := rows.Scan(&v.Slug, &v.Name, &v.Description, &v.DisplayOrder, &v.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func (s *SQLStore) GetServiceBySlug(ctx context.Context, slug string) (*Service, error) {
	var v Service
	err := s.db.QueryRowContext(ctx, `SELECT slug, name, description, display_order, created_at FROM pharos_services WHERE slug = ?`, slug).
		Scan(&v.Slug, &v.Name, &v.Description, &v.DisplayOrder, &v.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &v, nil
}

// UpsertService inserts/updates a service (seeding helper, kept store-agnostic; the read side —
// GetAllServices/GetServiceBySlug — is read-only).
func (s *SQLStore) UpsertService(ctx context.Context, v *Service) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO pharos_services (slug, name, description, display_order) VALUES (?, ?, ?, ?)
		ON CONFLICT(slug) DO UPDATE SET name=excluded.name, description=excluded.description, display_order=excluded.display_order`,
		v.Slug, v.Name, v.Description, v.DisplayOrder)
	return err
}

// ── Endpoints ──

const endpointSelect = `SELECT e.slug, e.name, e.url, e.type, e.service_slug, e.interval_seconds, e.timeout_ms, e.enabled, e.created_at,
	COALESCE(c.status,'unknown'), COALESCE(c.http_status,''), COALESCE(c.checked_at,0), COALESCE(c.response_time_ms,0)
	FROM pharos_endpoints e
	LEFT JOIN pharos_checks c ON c.endpoint_slug = e.slug AND c.checked_at = (SELECT MAX(checked_at) FROM pharos_checks WHERE endpoint_slug = e.slug)`

func scanEndpoint(sc interface{ Scan(...any) error }) (Endpoint, error) {
	var e Endpoint
	var enabled int64
	err := sc.Scan(&e.Slug, &e.Name, &e.URL, &e.Type, &e.ServiceSlug, &e.IntervalSeconds, &e.TimeoutMS, &enabled, &e.CreatedAt,
		&e.LastStatus, &e.LastHTTPStatus, &e.LastCheckAt, &e.LastResponseMS)
	e.Enabled = enabled != 0
	return e, err
}

func (s *SQLStore) GetEndpointsByService(ctx context.Context, serviceSlug string) ([]Endpoint, error) {
	rows, err := s.db.QueryContext(ctx, endpointSelect+` WHERE e.service_slug = ? ORDER BY e.name ASC`, serviceSlug)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Endpoint
	for rows.Next() {
		e, err := scanEndpoint(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *SQLStore) GetAllEndpoints(ctx context.Context) ([]Endpoint, error) {
	rows, err := s.db.QueryContext(ctx, endpointSelect+` WHERE e.enabled = 1 ORDER BY e.service_slug, e.name ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Endpoint
	for rows.Next() {
		e, err := scanEndpoint(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *SQLStore) GetEndpointBySlug(ctx context.Context, slug string) (*Endpoint, error) {
	e, err := scanEndpoint(s.db.QueryRowContext(ctx, endpointSelect+` WHERE e.slug = ?`, slug))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &e, nil
}

func (s *SQLStore) UpsertEndpoint(ctx context.Context, e *Endpoint) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO pharos_endpoints (slug, name, url, type, service_slug, interval_seconds, timeout_ms, enabled)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(slug) DO UPDATE SET name=excluded.name, url=excluded.url, type=excluded.type,
			service_slug=excluded.service_slug, interval_seconds=excluded.interval_seconds,
			timeout_ms=excluded.timeout_ms, enabled=excluded.enabled`,
		e.Slug, e.Name, e.URL, defaultType(e.Type), e.ServiceSlug, e.IntervalSeconds, e.TimeoutMS, boolInt(e.Enabled))
	return err
}

func defaultType(t string) string {
	if t == "" {
		return "http"
	}
	return t
}
func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// ── Health checks ──

func (s *SQLStore) InsertCheck(ctx context.Context, c *Check) error {
	if c.CheckedAt == 0 {
		c.CheckedAt = s.now().Unix()
	}
	res, err := s.db.ExecContext(ctx, `
		INSERT INTO pharos_checks (endpoint_slug, status, http_status, response_time_ms, status_code, error_message, checked_at, node_id)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		c.EndpointSlug, c.Status, c.HTTPStatus, c.ResponseTimeMS, c.StatusCode, c.ErrorMessage, c.CheckedAt, c.NodeID)
	if err != nil {
		return err
	}
	if id, err := res.LastInsertId(); err == nil {
		c.ID = id
	}
	return nil
}

func (s *SQLStore) GetRecentChecks(ctx context.Context, endpointSlug string, limit int) ([]Check, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, endpoint_slug, status, http_status, response_time_ms, status_code, error_message, checked_at, node_id
		FROM pharos_checks WHERE endpoint_slug = ? ORDER BY checked_at DESC, id DESC LIMIT ?`, endpointSlug, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Check
	for rows.Next() {
		var c Check
		if err := rows.Scan(&c.ID, &c.EndpointSlug, &c.Status, &c.HTTPStatus, &c.ResponseTimeMS, &c.StatusCode, &c.ErrorMessage, &c.CheckedAt, &c.NodeID); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// ── Maintenance windows ──

func (s *SQLStore) GetMaintenanceWindows(ctx context.Context, includeCompleted bool, days int) ([]MaintenanceWindow, error) {
	cutoff := s.now().AddDate(0, 0, -days).Unix()
	q := `SELECT id, title, description, affected_services, start_at, end_at, status, created_at FROM pharos_maintenance_windows WHERE (start_at >= ? OR end_at >= ?)`
	if !includeCompleted {
		q += ` AND status != 'completed' AND status != 'cancelled'`
	}
	q += ` ORDER BY start_at ASC`
	rows, err := s.db.QueryContext(ctx, q, cutoff, cutoff)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []MaintenanceWindow
	for rows.Next() {
		var w MaintenanceWindow
		var affected string
		if err := rows.Scan(&w.ID, &w.Title, &w.Description, &affected, &w.StartAt, &w.EndAt, &w.Status, &w.CreatedAt); err != nil {
			return nil, err
		}
		w.AffectedServices = unmarshalServices(affected)
		out = append(out, w)
	}
	return out, rows.Err()
}

func (s *SQLStore) InsertMaintenanceWindow(ctx context.Context, w *MaintenanceWindow) error {
	if w.ID == "" {
		w.ID = s.newID()
	}
	if w.Status == "" {
		w.Status = "scheduled"
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO pharos_maintenance_windows (id, title, description, affected_services, start_at, end_at, status)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		w.ID, w.Title, w.Description, marshalServices(w.AffectedServices), w.StartAt, w.EndAt, w.Status)
	return err
}

func (s *SQLStore) UpdateMaintenanceWindow(ctx context.Context, w *MaintenanceWindow) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE pharos_maintenance_windows SET title=?, description=?, affected_services=?, start_at=?, end_at=?, status=? WHERE id=?`,
		w.Title, w.Description, marshalServices(w.AffectedServices), w.StartAt, w.EndAt, w.Status, w.ID)
	return err
}

func (s *SQLStore) IsInMaintenanceWindow(ctx context.Context, serviceSlug string) (bool, string, error) {
	now := s.now().Unix()
	var title string
	err := s.db.QueryRowContext(ctx, `
		SELECT title FROM pharos_maintenance_windows
		WHERE status IN ('scheduled','in_progress') AND start_at <= ? AND end_at >= ? AND (affected_services LIKE '%' || ? || '%')
		ORDER BY start_at ASC LIMIT 1`, now, now, serviceSlug).Scan(&title)
	if errors.Is(err, sql.ErrNoRows) {
		return false, "", nil
	}
	if err != nil {
		return false, "", err
	}
	return true, title, nil
}

// ── Incidents ──

func (s *SQLStore) GetIncidents(ctx context.Context, includeResolved bool, days int) ([]Incident, error) {
	cutoff := s.now().AddDate(0, 0, -days).Unix()
	q := `SELECT id, title, status, impact, affected_services, started_at, COALESCE(resolved_at,0), created_at FROM pharos_incidents WHERE started_at >= ?`
	if !includeResolved {
		q += ` AND status != 'resolved'`
	}
	q += ` ORDER BY started_at DESC`
	rows, err := s.db.QueryContext(ctx, q, cutoff)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Incident
	for rows.Next() {
		var i Incident
		var affected string
		if err := rows.Scan(&i.ID, &i.Title, &i.Status, &i.Impact, &affected, &i.StartedAt, &i.ResolvedAt, &i.CreatedAt); err != nil {
			return nil, err
		}
		i.AffectedServices = unmarshalServices(affected)
		out = append(out, i)
	}
	return out, rows.Err()
}

func (s *SQLStore) GetActiveIncidents(ctx context.Context) ([]Incident, error) {
	return s.GetIncidents(ctx, false, 365)
}

func (s *SQLStore) InsertIncident(ctx context.Context, i *Incident) error {
	if i.ID == "" {
		i.ID = s.newID()
	}
	if i.Status == "" {
		i.Status = "investigating"
	}
	if i.StartedAt == 0 {
		i.StartedAt = s.now().Unix()
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO pharos_incidents (id, title, status, impact, affected_services, started_at)
		VALUES (?, ?, ?, ?, ?, ?)`,
		i.ID, i.Title, i.Status, i.Impact, marshalServices(i.AffectedServices), i.StartedAt)
	return err
}

func (s *SQLStore) UpdateIncident(ctx context.Context, i *Incident) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE pharos_incidents SET title=?, status=?, impact=?, affected_services=?, resolved_at=? WHERE id=?`,
		i.Title, i.Status, i.Impact, marshalServices(i.AffectedServices), i.ResolvedAt, i.ID)
	return err
}

func (s *SQLStore) DeleteIncident(ctx context.Context, id string) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM pharos_incident_updates WHERE incident_id = ?`, id); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, `DELETE FROM pharos_incidents WHERE id = ?`, id)
	return err
}

func (s *SQLStore) DeleteAllIncidents(ctx context.Context) (int, error) {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM pharos_incident_updates`); err != nil {
		return 0, err
	}
	res, err := s.db.ExecContext(ctx, `DELETE FROM pharos_incidents`)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

func (s *SQLStore) GetIncidentUpdates(ctx context.Context, incidentID string) ([]IncidentUpdate, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, incident_id, status, message, created_at FROM pharos_incident_updates WHERE incident_id = ? ORDER BY created_at DESC`, incidentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []IncidentUpdate
	for rows.Next() {
		var u IncidentUpdate
		if err := rows.Scan(&u.ID, &u.IncidentID, &u.Status, &u.Message, &u.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

func (s *SQLStore) InsertIncidentUpdate(ctx context.Context, u *IncidentUpdate) error {
	if u.ID == "" {
		u.ID = s.newID()
	}
	if u.CreatedAt == 0 {
		u.CreatedAt = s.now().Unix()
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO pharos_incident_updates (id, incident_id, status, message, created_at) VALUES (?, ?, ?, ?, ?)`,
		u.ID, u.IncidentID, u.Status, u.Message, u.CreatedAt)
	return err
}

// ── Uptime metrics ──

func (s *SQLStore) GetUptimeMetrics(ctx context.Context, endpointSlug, serviceSlug string, days int) ([]UptimeMetric, error) {
	cutoff := s.now().AddDate(0, 0, -days).Format("2006-01-02")
	q := `SELECT m.endpoint_slug, m.date, m.uptime_percent, m.total_checks, m.successful_checks, m.avg_response_time_ms FROM pharos_uptime_metrics m`
	var args []any
	switch {
	case endpointSlug != "":
		q += ` WHERE m.endpoint_slug = ? AND m.date >= ?`
		args = append(args, endpointSlug, cutoff)
	case serviceSlug != "":
		q += ` JOIN pharos_endpoints e ON m.endpoint_slug = e.slug WHERE e.service_slug = ? AND m.date >= ?`
		args = append(args, serviceSlug, cutoff)
	default:
		q += ` WHERE m.date >= ?`
		args = append(args, cutoff)
	}
	q += ` ORDER BY m.date DESC, m.endpoint_slug ASC`
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []UptimeMetric
	for rows.Next() {
		var m UptimeMetric
		if err := rows.Scan(&m.EndpointSlug, &m.Date, &m.UptimePercent, &m.TotalChecks, &m.SuccessfulChecks, &m.AvgResponseTimeMS); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func (s *SQLStore) UpsertUptimeMetric(ctx context.Context, m *UptimeMetric) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO pharos_uptime_metrics (endpoint_slug, date, uptime_percent, total_checks, successful_checks, avg_response_time_ms)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(endpoint_slug, date) DO UPDATE SET uptime_percent=excluded.uptime_percent,
			total_checks=excluded.total_checks, successful_checks=excluded.successful_checks, avg_response_time_ms=excluded.avg_response_time_ms`,
		m.EndpointSlug, m.Date, m.UptimePercent, m.TotalChecks, m.SuccessfulChecks, m.AvgResponseTimeMS)
	return err
}

// ── Status-change audit log ──

func (s *SQLStore) InsertStatusChange(ctx context.Context, endpointSlug, oldStatus, newStatus, reason, incidentID string) error {
	var incident any
	if incidentID != "" {
		incident = incidentID
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO pharos_status_log (endpoint_slug, old_status, new_status, reason, incident_id) VALUES (?, ?, ?, ?, ?)`,
		endpointSlug, oldStatus, newStatus, reason, incident)
	return err
}

// ── Subscribers ──

func (s *SQLStore) InsertSubscriber(ctx context.Context, sub *Subscriber) error {
	if sub.ID == "" {
		sub.ID = s.newID()
	}
	if sub.CreatedAt == 0 {
		sub.CreatedAt = s.now().Unix()
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO pharos_subscribers (id, email_encrypted, email_hash, verify_token_hash, unsub_token_hash, verified, created_at)
		VALUES (?, ?, ?, ?, ?, 0, ?)`,
		sub.ID, sub.EmailEncrypted, sub.EmailHash, sub.VerifyTokenHash, sub.UnsubTokenHash, sub.CreatedAt)
	return err
}

func (s *SQLStore) VerifySubscriber(ctx context.Context, verifyTokenHash string) error {
	res, err := s.db.ExecContext(ctx, `UPDATE pharos_subscribers SET verified=1, verified_at=? WHERE verify_token_hash=? AND verified=0 AND unsubscribed_at IS NULL`,
		s.now().Unix(), verifyTokenHash)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *SQLStore) UnsubscribeByToken(ctx context.Context, unsubTokenHash string) error {
	res, err := s.db.ExecContext(ctx, `UPDATE pharos_subscribers SET unsubscribed_at=? WHERE unsub_token_hash=? AND unsubscribed_at IS NULL`,
		s.now().Unix(), unsubTokenHash)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *SQLStore) GetVerifiedSubscribers(ctx context.Context) ([]Subscriber, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, email_encrypted, email_hash, unsub_token_hash FROM pharos_subscribers WHERE verified=1 AND unsubscribed_at IS NULL`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Subscriber
	for rows.Next() {
		var sub Subscriber
		if err := rows.Scan(&sub.ID, &sub.EmailEncrypted, &sub.EmailHash, &sub.UnsubTokenHash); err != nil {
			return nil, err
		}
		sub.Verified = true
		out = append(out, sub)
	}
	return out, rows.Err()
}

func (s *SQLStore) SubscriberExistsByEmailHash(ctx context.Context, emailHash string) (bool, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pharos_subscribers WHERE email_hash = ?`, emailHash).Scan(&n)
	return n > 0, err
}
