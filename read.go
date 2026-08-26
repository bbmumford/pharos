package pharos

import "context"

// ServiceView is a service with its status computed from its endpoints, plus those endpoints.
type ServiceView struct {
	Service
	Status    string // computed: worst-of the endpoints (operational/degraded/partial_outage/major_outage)
	Endpoints []Endpoint
}

// ServiceDetail is a ServiceView plus the service's uptime history.
type ServiceDetail struct {
	ServiceView
	UptimeHistory []UptimeMetric
}

// OverallStatus is the whole-platform roll-up shown at the top of a status page.
type OverallStatus struct {
	Status              string // worst-of every service
	ServicesOperational int
	ServicesDegraded    int
	ServicesOutage      int
	ActiveIncidents     int
	ActiveMaintenance   int
	OngoingIncidents    []Incident // capped (see maxOngoingIncidents); full list via Incidents read
}

const maxOngoingIncidents = 10

// Reader is the read/compute surface over a StatusStore: it derives service and overall status from
// stored endpoint state and rolls up incidents/maintenance. It holds no authority — a consumer's RPC
// handler authorizes (warrant) and then calls these; writes are direct StatusStore calls the consumer
// makes after its own authorization, keeping pharos warrant-independent.
type Reader struct{ store StatusStore }

// NewReader builds a read/compute surface over the store.
func NewReader(store StatusStore) *Reader { return &Reader{store: store} }

// DetermineServiceStatus is the worst-of-endpoints rule: any outage (all endpoints out → major, some
// → partial) beats any degraded beats operational; no endpoints (or all operational/blank) →
// operational. Exported because it is the reusable status-precedence primitive (§22.8) callers reuse.
func DetermineServiceStatus(endpoints []Endpoint) string {
	if len(endpoints) == 0 {
		return StatusOperational
	}
	var operational, degraded, outage int
	for _, ep := range endpoints {
		switch ep.LastStatus {
		case StatusOperational, "":
			operational++
		case StatusDegraded:
			degraded++
		case StatusPartialOutage, StatusMajorOutage:
			outage++
		}
	}
	if outage > 0 {
		if outage == len(endpoints) {
			return StatusMajorOutage
		}
		return StatusPartialOutage
	}
	if degraded > 0 {
		return StatusDegraded
	}
	return StatusOperational
}

// statusRank orders the service statuses for the overall worst-of (higher = worse).
func statusRank(s string) int {
	switch s {
	case StatusDegraded:
		return 1
	case StatusPartialOutage:
		return 2
	case StatusMajorOutage:
		return 3
	default:
		return 0
	}
}

// endpointsByService bulk-fetches all endpoints once and groups them (avoids the source's N+1
// per-service query that blew the handler deadline on a round-trip store).
func (r *Reader) endpointsByService(ctx context.Context) (map[string][]Endpoint, error) {
	all, err := r.store.GetAllEndpoints(ctx)
	if err != nil {
		return nil, err
	}
	by := make(map[string][]Endpoint)
	for _, ep := range all {
		by[ep.ServiceSlug] = append(by[ep.ServiceSlug], ep)
	}
	return by, nil
}

// OverallStatus rolls every service up to a single worst-of status plus operational/degraded/outage
// counts and active incident/maintenance counts (with the first maxOngoingIncidents incidents). On a
// hard store failure it returns a degraded status alongside the error, so a status page can render
// "degraded" rather than nothing (the source's resilience), while the consumer still sees the error.
func (r *Reader) OverallStatus(ctx context.Context) (OverallStatus, error) {
	services, err := r.store.GetAllServices(ctx)
	if err != nil {
		return OverallStatus{Status: StatusDegraded}, err
	}
	bySvc, err := r.endpointsByService(ctx)
	if err != nil {
		return OverallStatus{Status: StatusDegraded}, err
	}
	out := OverallStatus{Status: StatusOperational}
	for _, svc := range services {
		st := DetermineServiceStatus(bySvc[svc.Slug])
		switch st {
		case StatusOperational:
			out.ServicesOperational++
		case StatusDegraded:
			out.ServicesDegraded++
		case StatusPartialOutage, StatusMajorOutage:
			out.ServicesOutage++
		}
		if statusRank(st) > statusRank(out.Status) {
			out.Status = st
		}
	}
	if incs, err := r.store.GetActiveIncidents(ctx); err == nil {
		out.ActiveIncidents = len(incs)
		for i, inc := range incs {
			if i >= maxOngoingIncidents {
				break
			}
			out.OngoingIncidents = append(out.OngoingIncidents, inc)
		}
	}
	if windows, err := r.store.GetMaintenanceWindows(ctx, false, 1); err == nil {
		for _, w := range windows {
			if w.Status == "in_progress" || w.Status == "scheduled" {
				out.ActiveMaintenance++
			}
		}
	}
	return out, nil
}

// Services lists every service with its computed status and endpoints.
func (r *Reader) Services(ctx context.Context) ([]ServiceView, error) {
	services, err := r.store.GetAllServices(ctx)
	if err != nil {
		return nil, err
	}
	bySvc, err := r.endpointsByService(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]ServiceView, 0, len(services))
	for _, svc := range services {
		eps := bySvc[svc.Slug]
		out = append(out, ServiceView{Service: svc, Status: DetermineServiceStatus(eps), Endpoints: eps})
	}
	return out, nil
}

// ServiceStatus returns one service's status, endpoints, and 90-day uptime history. An unknown or
// empty slug returns ErrNotFound.
func (r *Reader) ServiceStatus(ctx context.Context, slug string) (*ServiceDetail, error) {
	if slug == "" {
		return nil, ErrNotFound
	}
	svc, err := r.store.GetServiceBySlug(ctx, slug)
	if err != nil {
		return nil, err
	}
	eps, err := r.store.GetEndpointsByService(ctx, slug)
	if err != nil {
		return nil, err
	}
	metrics, err := r.store.GetUptimeMetrics(ctx, "", slug, 90)
	if err != nil {
		return nil, err
	}
	return &ServiceDetail{
		ServiceView:   ServiceView{Service: *svc, Status: DetermineServiceStatus(eps), Endpoints: eps},
		UptimeHistory: metrics,
	}, nil
}
