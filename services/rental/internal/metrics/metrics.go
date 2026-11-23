// Package metrics defines Rental's Prometheus metrics in the default registry.
// Every process exports all of them; label-less gauges a process does not use stay
// at 0, so dashboards should select by job (rental-api, rental-worker, rental-maintenance).
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const namespace = "rental"

var latencyBuckets = []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2, 5, 10}

var (
	// HTTPRequests counts API requests by route pattern, status code and error code.
	HTTPRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Name: "http_requests_total", Help: "API requests.",
	}, []string{"route", "status", "error_code"})
	HTTPDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace, Name: "http_request_duration_seconds", Help: "API request latency.",
		Buckets: latencyBuckets,
	}, []string{"route"})
	// OrderReads separates reads served from orders and from orders_history.
	OrderReads = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace, Name: "order_read_duration_seconds", Help: "GET /orders/{id} latency by storage.",
		Buckets: latencyBuckets,
	}, []string{"storage"})

	// DependencyCalls counts Source and Payment calls by dependency, operation and outcome.
	DependencyCalls = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Name: "dependency_calls_total", Help: "Calls to Source and Payment.",
	}, []string{"dependency", "operation", "outcome"})
	DependencyDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace, Name: "dependency_call_duration_seconds", Help: "Latency of Source and Payment calls.",
		Buckets: latencyBuckets,
	}, []string{"dependency", "operation"})

	TariffCache = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Name: "tariff_cache_lookups_total", Help: "Tariff cache lookups by result.",
	}, []string{"result"})
	ConfigRefreshes = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Name: "config_refreshes_total", Help: "Config refreshes by result.",
	}, []string{"result"})
	ConfigUsingDefaults = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace, Name: "config_using_defaults", Help: "1 while no valid config has been received.",
	})
	ProfileFallbacks = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: namespace, Name: "user_profile_fallbacks_total", Help: "Offers priced with the fallback profile.",
	})

	TaskAttempts = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Name: "task_attempts_total", Help: "Task attempts by kind and outcome.",
	}, []string{"kind", "outcome"})
	TaskAttemptDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace, Name: "task_attempt_duration_seconds", Help: "Task attempt latency.",
		Buckets: latencyBuckets,
	}, []string{"kind"})
	TaskClaimLag = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace, Name: "task_claim_lag_seconds", Help: "Delay from next_run_at to claim.",
		Buckets: latencyBuckets,
	}, []string{"kind"})
	TasksFinished = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Name: "tasks_finished_total", Help: "Finished tasks by kind, status and reason.",
	}, []string{"kind", "status", "reason"})
	StaleClaims = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Name: "task_stale_claims_total", Help: "Attempt results dropped because the claim was lost.",
	}, []string{"kind"})
	OrderActivation = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace, Name: "order_activation_seconds", Help: "Time from order creation to ACTIVE.",
		Buckets: latencyBuckets,
	})

	MaintenanceRows = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Name: "maintenance_rows_total", Help: "Rows processed by maintenance steps.",
	}, []string{"step"})
	MaintenanceStepDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace, Name: "maintenance_step_duration_seconds", Help: "Maintenance step latency.",
		Buckets: []float64{.01, .05, .1, .5, 1, 5, 10, 30, 60, 300},
	}, []string{"step"})
	MaintenanceLeaseHeld = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace, Name: "maintenance_lease_held", Help: "1 while this process holds the maintenance lease.",
	})

	// Database state gauges, refreshed by maintenance.
	TasksByState = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace, Name: "tasks", Help: "Tasks by kind and status.",
	}, []string{"kind", "status"})
	OldestTaskAge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace, Name: "tasks_oldest_age_seconds", Help: "Age of the oldest task by status.",
	}, []string{"status"})
	OrdersByStatus = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace, Name: "orders", Help: "Rows in orders by status.",
	}, []string{"status"})
	OldestClosedOrderAge = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace, Name: "orders_oldest_closed_age_seconds", Help: "Age of the oldest closed order not yet archived.",
	})
	HistoryPartitions = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace, Name: "history_partitions", Help: "Attached orders_history partitions.",
	})
	DatabaseBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace, Name: "database_size_bytes", Help: "Size of the Rental database.",
	})
)
