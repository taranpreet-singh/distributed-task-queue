package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	TasksProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tasks_processed_total",
			Help: "Total tasks processed",
		},
		[]string{"task_type", "status"},
	)

	TasksInFlight = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "tasks_in_flight",
		Help: "Tasks currently being processed",
	})

	TaskDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "task_duration_seconds",
			Help:    "Time taken to process a task",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"task_type"},
	)

	DLQTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tasks_dlq_total",
			Help: "Total tasks sent to DLQ",
		},
		[]string{"reason"},
	)
)

func init() {
	prometheus.MustRegister(TasksProcessed, TasksInFlight, TaskDuration, DLQTotal)
}
