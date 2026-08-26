// Package pharos is the scope-agnostic status-page + uptime-monitoring system (extracted from the
// HSTLES monitoring domain). It is ONE package for the whole system: the neutral data model (this
// file), the persistence contract (store.go), a reference in-memory store, the health checker, the
// uptime aggregator, the subscriber/notification layer, and the read/write handlers — each coupling
// an injected seam WITHIN the package, not a separate module.
//
// A status page has NO tenant/org axis: overall status, services, incidents, maintenance windows and
// uptime are public reads. So — unlike a session or authority core — there is no axis to lift; the
// couplings a platform domain would carry (wire/proto types, a shared debug logger, a baked
// mesh-status reader, baked email crypto) are instead package-owned neutral types and injected seams,
// preserving every capability: HTTP+mesh health checks, combined-vs-raw status, incidents with
// updates, maintenance windows, daily uptime aggregation, a status-change audit log, and verified
// email subscribers.
//
// Times are Unix seconds (int64), so a SQL store maps them to integer columns with no conversion. A
// zero timestamp means "unset".
package pharos

// Service is a logical grouping of endpoints shown as one row on the status page.
type Service struct {
	Slug         string
	Name         string
	Description  string
	DisplayOrder int
	CreatedAt    int64 // Unix seconds
}

// Endpoint is a single monitored URL. LastStatus is the combined mesh+HTTP status used for the status
// page and incident logic; LastHTTPStatus is the raw HTTP result only, kept separate so a health
// evaluator that overlays mesh state does not feed its own output back in.
type Endpoint struct {
	Slug            string
	Name            string
	URL             string
	Type            string // "http" (default) or another probe type
	ServiceSlug     string
	IntervalSeconds int
	TimeoutMS       int
	Enabled         bool
	CreatedAt       int64 // Unix seconds
	LastStatus      string
	LastHTTPStatus  string
	LastCheckAt     int64 // Unix seconds
	LastResponseMS  int
}

// Check is one health-check result. Status is the combined mesh+HTTP status; HTTPStatus is the raw
// HTTP result before any mesh overlay. NodeID names the checking node when checks are federated.
type Check struct {
	ID             int64
	EndpointSlug   string
	Status         string
	HTTPStatus     string
	ResponseTimeMS int
	StatusCode     int
	ErrorMessage   string
	CheckedAt      int64 // Unix seconds
	NodeID         string
}

// MaintenanceWindow is a scheduled maintenance period over a set of services.
type MaintenanceWindow struct {
	ID               string
	Title            string
	Description      string
	AffectedServices []string
	StartAt          int64  // Unix seconds
	EndAt            int64  // Unix seconds
	Status           string // "scheduled" | "in_progress" | "completed"
	CreatedAt        int64  // Unix seconds
}

// Incident is a status-affecting event over a set of services, with a lifecycle of updates.
type Incident struct {
	ID               string
	Title            string
	Status           string // "investigating" | "identified" | "monitoring" | "resolved"
	Impact           string // "degraded" | "partial" | "major" | ...
	AffectedServices []string
	StartedAt        int64 // Unix seconds
	ResolvedAt       int64 // Unix seconds; 0 while unresolved
	CreatedAt        int64 // Unix seconds
}

// IncidentUpdate is one post in an incident's timeline.
type IncidentUpdate struct {
	ID         string
	IncidentID string
	Status     string
	Message    string
	CreatedAt  int64 // Unix seconds
}

// UptimeMetric is one endpoint's aggregated uptime for one calendar day (Date is "YYYY-MM-DD").
type UptimeMetric struct {
	EndpointSlug      string
	Date              string
	UptimePercent     float64
	TotalChecks       int
	SuccessfulChecks  int
	AvgResponseTimeMS int
}

// Subscriber is a status-page email subscriber. The email is stored ENCRYPTED with a separate
// deterministic hash for uniqueness/lookup; the verify/unsubscribe tokens are stored hashed. The
// package never holds the plaintext email or raw tokens — encryption and hashing happen in the
// subscriber service behind an injected Encryptor, and only the results reach the store.
type Subscriber struct {
	ID              string
	EmailEncrypted  string
	EmailHash       string
	VerifyTokenHash string
	UnsubTokenHash  string
	Verified        bool
	CreatedAt       int64 // Unix seconds
	VerifiedAt      int64 // Unix seconds; 0 until verified
	UnsubscribedAt  int64 // Unix seconds; 0 unless unsubscribed
}
