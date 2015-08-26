package skunk

import (
	"encoding/json"
	"math"
	"strconv"
	"time"
)

// Metrics is a map of NewRelic metric names to values. It provides a couple convenience methods for building up merge-able metrics.
type Metrics map[string]Metric

// AddFloat adds a single float metric (as a ScalarMetric) to the Metrics map.
func (m Metrics) AddFloat(name string, val float64) {
	m.AddMetric(name, ScalarMetric(val))
}

// AddMetric merges a Metric into the Metrics map. Merges always happen by merging the existing value into the new metric, rather
// than vice versa, to give externally-defined Metrics an opportunity to perform the merge (since otherwise a RangeMetric, for example,
// will just call this anyway).
func (m Metrics) AddMetric(name string, val Metric) {
	if met, ok := m[name]; ok {
		m[name] = val.Merge(met)
	} else {
		m[name] = val
	}
}

// MergeMetrics merges all Metrics in the given map into m.
func (m Metrics) MergeMetrics(metrics Metrics) {
	for k, v := range metrics {
		m.AddMetric(k, v)
	}
}

// Body represents the POSTed body of a NewRelic plugin's metrics data.
type Body struct {
	Agent      AgentRep     `json:"agent"`
	Components []*Component `json:"components"`
}

// AgentRep describes a NewRelic agent.
type AgentRep struct {
	Host string `json:"host"`
	// PID zero is treated as an erroneous/nonexistent PID. If you're shoving NewRelic into a Go-based scheduler in
	// the kernel, I guess you could open an issue for this. Otherwise, this seems reasonable to me.
	PID     int    `json:"pid,omitempty"`
	Version string `json:"version"`
}

// Seconds is a convenience wrapper around a time.Duration to allow any duration to be used when describing the duration
// of components' metrics.
type Seconds struct{ time.Duration }

func (s Seconds) MarshalJSON() ([]byte, error) {
	var i int64
	if int, frac := math.Modf(s.Seconds()); frac >= 0.5 {
		i = int64(int) + 1
	} else {
		i = int64(int)
	}
	return strconv.AppendInt(nil, i, 10), nil
}

// Component describes a component in a NewRelic agent. It must contain a minimum of at least one metric, otherwise the
// component is culled from its parent Body before constructing a JSON payload. All fields of the Component are
// read-only once initialized.
type Component struct {
	Name string `json:"name"`
	GUID string `json:"guid"`
	// Duration is the time elapsed, in seconds, for this snapshot of the component. The duration is rounded to the
	// nearest second. This is only used when constructing a payload using a copy of a Component.
	Duration Seconds `json:"duration"`
	Metrics  Metrics `json:"metrics"`

	// start is the time that the first metric was recorded. If start.IsZero is true, the time needs to be set to
	// the current time once a metric is added. The start time is cleared upon an agent successfully sending
	// a payload to NewRelic.
	start time.Time

	// agent is a pointer to the Agent that owns this component.
	agent *Agent
}

// AddMetric adds a single metric to the Component. If the metric already exists by name in the Component, the value is
// added to the existing metric, otherwise the metric is added as a ScalarMetric.
func (c *Component) AddMetric(name string, value float64) {
	c.MergeMetric(name, ScalarMetric(value))
}

// updateTiming updates the component's timing to the current time.
func (c *Component) updateTiming() {
	// Set the time over which the metric was gathered.
	if c.start.IsZero() {
		c.start = time.Now()
		c.Duration = Seconds{0}
	} else {
		c.Duration.Duration = time.Since(c.start)
	}
}

// AddMetric adds a single metric to the Component. If the metric already exists by name in the Component, the value is
// added to the existing metric, otherwise the metric is added as a ScalarMetric.
func (c *Component) MergeMetric(name string, value Metric) {
	c.agent.ops <- func(*Agent) error {
		c.Metrics.AddMetric(name, value)
		c.updateTiming()
		return nil
	}
}

// MergeMetrics merges a Metrics set into the component's metrics. This can be used to do batch updates of metrics if
// you're sending lots of metrics out and the agent is blocking goroutines due to high-frequency parallel updates.
func (c *Component) MergeMetrics(metrics Metrics) {
	c.agent.ops <- func(*Agent) error {
		c.Metrics.MergeMetrics(metrics)
		c.updateTiming()
		return nil
	}
}

// Metric describes any metric that can have an additional value added to it. All metrics must be marshallable as JSON,
// but are not required to implement MarshalJSON (e.g., RangeMetric).
//
// The Add method of a Metric is used to get the result of adding an additional value to a metric. Metrics themselves
// should be considered immutable, so the result must be a new Metric.
//
// This shouldn't be implemented by other libraries.
type Metric interface {
	Add(value float64) Metric
	Merge(Metric) Metric
}

// ScalarMetric is any singular metric that does not cover a range a values. Adding to a ScalarMetric produces
// a RangeMetric.
type ScalarMetric float64

func (s ScalarMetric) Add(value float64) Metric {
	f := float64(s)
	return RangeMetric{
		Total:  f + value,
		Count:  2,
		Min:    math.Min(value, f),
		Max:    math.Max(value, f),
		Square: math.Pow(f, 2) + math.Pow(value, 2),
	}
}

func (s ScalarMetric) Merge(value Metric) Metric {
	f := float64(s)
	return value.Merge(RangeMetric{Total: f, Count: 1, Min: f, Max: f, Square: math.Pow(f, 2)})
}

func (s ScalarMetric) MarshalJSON() ([]byte, error) {
	return json.Marshal(float64(s))
}

// RangeMetric is any metric that covers a range of values. Adding to a RangeMetric produces a new RangeMetric.
type RangeMetric struct {
	Total float64 `json:"total"`
	Count int     `json:"count"`
	Min   float64 `json:"min"`
	Max   float64 `json:"max"`
	// Square is the sum of squares of all values recorded for the metric. This is simply A₁² + A₂² + Aₙ² where A is
	// the set of numbers recorded for this metric.
	Square float64 `json:"sum_of_squares"`
}

func (r RangeMetric) Add(value float64) Metric {
	return RangeMetric{
		Total:  r.Total + value,
		Count:  r.Count + 1,
		Min:    math.Min(value, r.Min),
		Max:    math.Max(value, r.Max),
		Square: r.Square + math.Pow(value, 2),
	}
}

func (r RangeMetric) Merge(value Metric) Metric {
	switch o := value.(type) {
	case ScalarMetric:
		f := float64(o)
		r.Total += f
		r.Count++
		r.Min = math.Min(r.Min, f)
		r.Max = math.Max(r.Max, f)
		r.Square += math.Pow(f, 2)
	case RangeMetric:
		r.Total += o.Total
		r.Count += o.Count
		r.Min = math.Min(r.Min, o.Min)
		r.Max = math.Max(r.Max, o.Max)
		r.Square += o.Square
	default:
		// Defer to the other metric to attempt the merge.
		return value.Merge(r)
	}
	return r
}
