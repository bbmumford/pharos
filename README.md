# pharos

A public status page and uptime monitor in one package, for an operator who wants a status page for their own services. It has no tenant or partition axis.

- **Checks**: `HealthChecker` probes each endpoint over HTTP on an interval, optionally downgrading a result when your mesh reports trouble (`MeshStatusProvider`), records every check, and opens and resolves incidents itself when an endpoint goes down and comes back.
- **Uptime**: `UptimeAggregator` rolls checks up into daily uptime percentages.
- **Status**: `Reader` computes each service's status and the whole platform's for display (`OverallStatus`, `Services`, `ServiceStatus`).
- **Operations**: manual incidents with timeline updates, maintenance windows and a status-change log.
- **Subscribers**: `SubscriberService` keeps email subscribers (addresses sealed through your `Encryptor`, tokens hashed), and `StatusNotifier` alerts them through a `NotificationChannel` you supply.

## Use

```sh
go get github.com/bbmumford/pharos
```

```go
store := pharos.NewSQLStore(db) // any database/sql driver; or pharos.NewMemStore()
if err := store.InitSchema(ctx); err != nil { /* ... */ } // once at boot

checker, err := pharos.NewHealthChecker(pharos.CheckerConfig{
	Store:    store,
	Interval: time.Minute,
	Prober:   pharos.NewHTTPProber(),
})
checker.Start()
defer checker.Stop()

agg, err := pharos.NewUptimeAggregator(pharos.AggregatorConfig{Store: store, Interval: time.Hour})
agg.Start()

status, err := pharos.NewReader(store).OverallStatus(ctx)
```

`CheckerConfig.GracePeriod` delays the first check after `Start` (30 seconds by default; a negative value skips the wait).

## Consumers

ORBTR library, the help.orbtr.io and app.orbtr.io endpoints, and io tools.
