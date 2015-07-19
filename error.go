package skunk

// Error is any error internal to skunk or an error encapsulated by skunk.
type Error struct {
	Msg  string // A message describing the error
	Code int    // A unique number identifying the particular error
	Err  error  // Any inner error
}

func (e *Error) Error() string {
	m := "skunk: " + e.Msg
	if e.Err != nil {
		m = m + ": " + e.Err.Error()
	}
	return m
}

// mkerr returns a new Error for the given error code and accompanying inner error (may be nil).
func mkerr(code int, err error) error {
	return &Error{errMessages[code], code, err}
}

// errMessages is a map of all known error messages
var errMessages = map[int]string{
	// Public
	ErrNameTooLong:   "component name is too long. component names must be <= 32 characters",
	ErrNoName:        "component name is empty",
	ErrNoGUID:        "component GUID is empty",
	ErrNoAPIKey:      "no API key given",
	ErrNoHost:        "agent host is empty",
	ErrNoVersion:     "agent version is empty",
	ErrNotRunning:    "agent is not running",
	ErrNilOpReceived: "received a nil Op",
	ErrEmptyPayload:  "payload was empty",
	ErrBadPayload:    "malformed payload sent",
	ErrForbidden:     "API key was not accepted",
	ErrBadRequest:    "malformed request",
	ErrBodyTooLarge:  "too many components and/or metrics in body",
	ErrEncodingJSON:  "encountered an error in encoding a JSON payload",
	// Private
	errNoMetrics:    "nothing to send",
	errMustRetry:    "must retry this request",
	errShuttingDown: "agent is shutting down",
}

func iserr(err error, code int) bool {
	serr, ok := err.(*Error)
	return ok && serr.Code == code
}

const (
	// Component errors

	ErrNameTooLong int = 1 + iota
	ErrNoName
	ErrNoGUID

	// Initialization errors

	ErrNoAPIKey
	ErrNoHost
	ErrNoVersion

	// Consistency errors

	ErrNotRunning
	ErrNilOpReceived
	ErrEmptyPayload

	// Request construction / POST errors

	ErrBadPayload
	ErrForbidden
	ErrBadRequest
	ErrBodyTooLarge
	ErrEncodingJSON

	// Private errors

	// errNoMetrics is returned by getPayload when there are no metrics to send. This is a non-fatal error that just
	// means the send should be skipped for lack of data.
	errNoMetrics
	// errMustRetry is returned by sendRequest when the response is a 50x error and we should retry the send in a
	// minute.
	errMustRetry
	// errShuttingDown means the agent is shutting down right now. The runloop must exit immediately.
	errShuttingDown
)
