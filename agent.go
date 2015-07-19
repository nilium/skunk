package skunk

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"
)

// MinuteCycle, QuarterHourCycle, HalfHourCycle, and HourCycle all represent useful reporting cycles for an agent. Other
// cycles are permitted, provide they would not violate the two-POSTs-per-minute limit on NewRelic APIs.
const (
	MinuteCycle   = time.Minute
	HalfHourCycle = time.Minute * 30
	HourCycle     = time.Hour
)

// NewRelicAPI is the URL to POST NewRelic metrics data to. This may be altered to change the
// default endpoint of new agents. Already-initialized agents do not use this.
var NewRelicAPI = `https://platform-api.newrelic.com/platform/v1/metrics`

type Op func(*Agent)

type opGetErr chan<- error

func (c opGetErr) Exec(a *Agent) { c <- a.err }

type Agent struct {
	apiURL string

	// Initialization fields -- these do not change once set.
	apiKey string
	cycle  time.Duration
	client *http.Client

	// Access to the following fields controlled by runloop after init
	body     *Body
	err      error
	lastPoll time.Time
	ticker   *time.Ticker
	ops      chan<- Op
}

func New(version, apiKey string) (*Agent, error) {
	host, err := os.Hostname()
	if err != nil {
		// Hostnames are required.
		return nil, err
	}

	rep := AgentRep{
		Host:    host,
		PID:     os.Getpid(),
		Version: version,
	}

	return NewWithRep(apiKey, rep)
}

func NewWithRep(apiKey string, rep AgentRep) (agent *Agent, err error) {
	switch {
	case len(apiKey) == 0:
		return nil, mkerr(ErrNoAPIKey, nil)
	case len(rep.Version) == 0:
		return nil, mkerr(ErrNoVersion, nil)
	case len(rep.Host) == 0:
		return nil, mkerr(ErrNoHost, nil)
	}

	return &Agent{
		apiKey: apiKey,

		body: &Body{
			Agent:      rep,
			Components: nil,
		},

		lastPoll: time.Now(),
		ticker:   nil,
	}, nil
}

func (a *Agent) Start() {
	if a.ops != nil {
		return
	}

	ops := make(chan Op)
	a.ops = ops
	go a.run(ops)
}

// Close kills the agent's runloop and makes it completely inert. Using the agent afterward will result in a panic. Any
// error held by the agent is returned.
//
// When calling Close, you must ensure that the agent is no longer in use and will not be used by any goroutine after
// Close is called.
func (a *Agent) Close() error {
	err := a.Err()
	a.ops <- shutdown
	return err
}

// shutdown is an Op that closes an agent's ops channel.
func shutdown(a *Agent) {
	if err := a.sendRequest(time.Now()); iserr(err, errMustRetry) {
		log.Println("skunk: received 50x error from NewRelic on shutdown flush - dropping payload on the floor")
	} else if err != nil {
		a.err = err
	}
	close(a.ops)
}

func (a *Agent) Err() (err error) {
	out := make(chan error)
	a.ops <- opGetErr(out).Exec
	return <-out
}

func (a *Agent) run(ops <-chan Op) {
	var timer *time.Timer
	var retry <-chan time.Time

	a.ticker = time.NewTicker(a.cycle)
	defer a.ticker.Stop()

	trySend := func(from time.Time) {
		if err := a.sendRequest(from); err == nil {
			a.lastPoll = from
		} else if iserr(err, errMustRetry) {
			if timer == nil {
				timer = time.NewTimer(time.Minute)
				retry = timer.C
			} else {
				timer.Reset(time.Minute)
			}
		}
	}

	for {
		select {
		case from := <-retry:
			trySend(from)
		case from := <-a.ticker.C:
			trySend(from)
		case op, ok := <-ops:
			if !ok {
				return
			} else if op == nil {
				// This should be impossible. If it happens, log it and skip the op.
				log.Println(ErrNilOpReceived)
				continue
			}
			op(a)
		}
	}
}

func (a *Agent) sendRequest(from time.Time) (err error) {
	var buf bytes.Buffer
	compressed := true
tryGetPayload:
	err = a.getPayload(&buf, from, compressed)
	switch {
	case err == nil:
	case iserr(err, errNoMetrics):
		return nil // Nothing to do.
	default:
		if _, ok := err.(*json.MarshalerError); ok {
			// Can't do anything about this. This error might be worth panicking over.
			return mkerr(ErrEncodingJSON, err)
		}

		if compressed {
			// Try without compression in case it's some anomalous unknown compression error that's eluded
			// everyone but me (i.e., should be almost impossible).
			compressed = false
			buf.Reset()
			goto tryGetPayload
		}
		return err
	}

	req, err := http.NewRequest("POST", a.apiURL, &buf)
	if err != nil {
		// No idea what happened here, assume the worst.
		return err
	}

	// Set headers
	req.Header.Set("X-License-Key", a.apiKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if compressed {
		req.Header.Set("Content-Encoding", "gzip")
	}

	resp, err := a.client.Do(req)
	if resp != nil {
		defer func() {
			closeErr := resp.Body.Close()
			if closeErr != nil {
				log.Printf("skunk: error closing response body: %v", closeErr)
			}
		}()
	}

	if err != nil {
		return err
	}

	if resp.StatusCode == 200 {
		return nil
	}

	var nrErr struct {
		Error string `json:"error"`
	}
	decoder := json.NewDecoder(resp.Body)
	if err = decoder.Decode(nrErr); err == nil && len(nrErr.Error) > 0 {
		log.Printf("skunk: received NewRelic error: %s", nrErr.Error)
	}

	return statusError(resp)
}

func statusError(resp *http.Response) error {
	code := resp.StatusCode
	switch {
	case code >= 200 && code < 300:
		return nil
	case code == 400:
		return mkerr(ErrBadPayload, nil)
	case code == 403:
		return mkerr(ErrForbidden, nil)
	case code == 404:
		return mkerr(ErrBadRequest, nil)
	case code == 405:
		return mkerr(ErrBadRequest, nil)
	case code == 413:
		return mkerr(ErrBodyTooLarge, nil)
	case code >= 500 && code < 600:
		return mkerr(errMustRetry, nil)
	default:
		return fmt.Errorf("skunk: got unexpected status code %d %s from NewRelic.", code, resp.Status)
	}
}

// Component gets a component with the given name and GUID from the Agent. If no such component exists, then a new one
// is allocated and it is returned. No tests are done to ensure that components with the same name but a different GUID
// or vice versa are allocated, so it is possible to end up with potentially inconsistent data.
func (a *Agent) Component(name, guid string) (*Component, error) {
	if len(name) > 32 {
		// Per NewRelic, names must be <= 32 characters in length.
		return nil, mkerr(ErrNameTooLong, nil)
	}

	out := make(chan *Component)
	a.ops <- (addComponent{name, guid, out}).Exec
	return <-out, nil
}

// addComponent is a small wrapper around a parameter bundle to create a component and add it to an agent, or raturn an
// agent with the same name and guid.
type addComponent struct {
	name, guid string
	out        chan<- *Component
}

// Exec searches for a component of the name and guid described by ac. If it finds a component in the agent, a, it is
// sent on ac's out channel. Otherwise, a new component is created, added to the agent, and sent on the out channel.
func (ac addComponent) Exec(a *Agent) {
	body := a.body
	for _, c := range body.Components {
		if c.Name == ac.name && c.GUID == ac.guid {
			ac.out <- c
			return
		}
	}

	c := &Component{
		Name:    ac.name,
		GUID:    ac.guid,
		Metrics: make(map[string]Metric),
		agent:   a,
	}
	body.Components = append(body.Components, c)
}

// clear empties out all metrics held by the agent. This should be called after a payload has been successfully sent to
// reset all components to a pristine state.
func (a *Agent) clear() {
	for _, c := range a.body.Components {
		// Allocate a new Metrics map and reuse the old map's length as its capacity.
		c.Metrics = make(map[string]Metric, len(c.Metrics))
		c.Duration.Duration = 0 // Should be zero, but clear() says it'll make it pristine, so zero it anyway.
		c.start = time.Time{}
	}
}

// getPayload returns a JSON payload as a byte slice to send to NewRelic as its POSTed body. The resulting JSON does not
// include components without metrics.
func (a *Agent) getPayload(w io.Writer, from time.Time, compressed bool) (err error) {
	// Make a copy of the body and exclude components without metrics.
	body := *a.body
	body.Components = make([]*Component, 0, len(body.Components))
	for _, com := range a.body.Components {
		if len(com.Metrics) == 0 || com.start.IsZero() {
			continue
		}

		dupe := *com
		dupe.Duration.Duration = from.Sub(com.start)
		if dupe.Duration.Duration < 0 {
			// Metrics from the future aren't allowed.
			dupe.Duration.Duration = 0
		}

		body.Components = append(body.Components, &dupe)
	}

	if len(body.Components) == 0 {
		return mkerr(errNoMetrics, nil)
	}

	if compressed {
		zipWriter := gzip.NewWriter(w)
		defer func() {
			if err == nil {
				err = zipWriter.Close()
			}
		}()
		w = zipWriter
	}
	encoder := json.NewEncoder(w)
	return encoder.Encode(body)
}
