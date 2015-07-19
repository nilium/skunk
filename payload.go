package skunk

import (
	"encoding/json"
	"math"
	"strconv"
	"time"
)

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
	Duration Seconds           `json:"version"`
	Metrics  map[string]Metric `json:"metrics"`

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
	c.agent.ops <- func(*Agent) error {
		if m, ok := c.Metrics[name]; ok {
			c.Metrics[name] = m.Add(value)
		} else {
			c.Metrics[name] = ScalarMetric(value)
		}

		// Set the time over which the metric was gathered.
		if c.start.IsZero() {
			c.start = time.Now()
			c.Duration = Seconds{0}
		} else {
			c.Duration.Duration = time.Since(c.start)
		}

		return nil
	}
}

// Metric describes any metric that can have an additional value added to it. All metrics must be marshallable as JSON,
// but are not required to implement MarshalJSON (e.g., RangeMetric).
//
// The Add method of a Metric is used to get the result of adding an additional value to a metric. Metrics themselves
// should be considered immutable, so the result must be a new Metric.
type Metric interface {
	Add(value float64) Metric
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
