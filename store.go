package pharos

import (
	"context"
	"errors"
)

// ErrNotFound is returned by single-record reads when the record does not exist. A SQL-backed store
// maps its no-rows sentinel to this; the reference store returns it directly.
var ErrNotFound = errors.New("pharos: not found")

// StatusStore is the persistence contract for the whole status-page system — services, endpoints,
// health checks, maintenance windows, incidents (+updates), daily uptime metrics, the status-change
// audit log, and email subscribers. It is the one seam a consumer supplies (a SQLite/Turso
// implementation, or the package's reference MemStore); every field it moves is a neutral pharos
// type, so no consumer needs the source's pb wire types. Times are Unix seconds.
//
// The store persists exactly what it is given and interprets no policy: subscriber emails arrive
// already encrypted+hashed and tokens already hashed (the subscriber service owns the crypto behind
// an injected Encryptor), and health/status values arrive already evaluated (the checker + health
// evaluator own that). This keeps the store substitutable and free of hidden behavior.
type StatusStore interface {
	// ── Services ──
	GetAllServices(ctx context.Context) ([]Service, error)
	GetServiceBySlug(ctx context.Context, slug string) (*Service, error)

	// ── Endpoints ──
	GetEndpointsByService(ctx context.Context, serviceSlug string) ([]Endpoint, error)
	GetAllEndpoints(ctx context.Context) ([]Endpoint, error)
	GetEndpointBySlug(ctx context.Context, slug string) (*Endpoint, error)
	UpsertEndpoint(ctx context.Context, e *Endpoint) error

	// ── Health checks ──
	InsertCheck(ctx context.Context, c *Check) error
	GetRecentChecks(ctx context.Context, endpointSlug string, limit int) ([]Check, error)

	// ── Maintenance windows ──
	// GetMaintenanceWindows returns windows within the last `days`; includeCompleted also returns
	// windows whose status is completed. IsInMaintenanceWindow reports whether `serviceSlug` is
	// currently covered by an active window, and if so its title.
	GetMaintenanceWindows(ctx context.Context, includeCompleted bool, days int) ([]MaintenanceWindow, error)
	InsertMaintenanceWindow(ctx context.Context, w *MaintenanceWindow) error
	UpdateMaintenanceWindow(ctx context.Context, w *MaintenanceWindow) error
	IsInMaintenanceWindow(ctx context.Context, serviceSlug string) (inWindow bool, title string, err error)

	// ── Incidents ──
	// GetIncidents returns incidents started within the last `days`; includeResolved also returns
	// resolved ones. GetActiveIncidents returns only unresolved incidents.
	GetIncidents(ctx context.Context, includeResolved bool, days int) ([]Incident, error)
	GetActiveIncidents(ctx context.Context) ([]Incident, error)
	InsertIncident(ctx context.Context, i *Incident) error
	UpdateIncident(ctx context.Context, i *Incident) error
	DeleteIncident(ctx context.Context, id string) error
	DeleteAllIncidents(ctx context.Context) (deleted int, err error)
	GetIncidentUpdates(ctx context.Context, incidentID string) ([]IncidentUpdate, error)

	// ── Uptime metrics ──
	GetUptimeMetrics(ctx context.Context, endpointSlug, serviceSlug string, days int) ([]UptimeMetric, error)
	UpsertUptimeMetric(ctx context.Context, m *UptimeMetric) error

	// ── Status-change audit log ──
	InsertStatusChange(ctx context.Context, endpointSlug, oldStatus, newStatus, reason, incidentID string) error

	// ── Subscribers ──
	// Emails arrive already encrypted (EmailEncrypted) with a deterministic EmailHash for uniqueness,
	// and verify/unsub tokens arrive hashed. The store never sees plaintext.
	InsertSubscriber(ctx context.Context, s *Subscriber) error
	VerifySubscriber(ctx context.Context, verifyTokenHash string) error
	UnsubscribeByToken(ctx context.Context, unsubTokenHash string) error
	GetVerifiedSubscribers(ctx context.Context) ([]Subscriber, error)
	SubscriberExistsByEmailHash(ctx context.Context, emailHash string) (bool, error)
}
