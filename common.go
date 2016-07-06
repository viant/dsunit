package dsunit

var errorStatus = "error"
var okStatus = "ok"

//TestSchema constant test:// - it is used as shortcut for the test base directory.
var TestSchema = "test://"

type dsUnitError struct {
	error string
}

func (e dsUnitError) Error() string {
	return e.error
}

func (e *Response) hasError() bool {
	return e.Status == errorStatus
}

func newErrorResponse(err error) *Response {
	return &Response{Status: errorStatus, Message: err.Error()}
}

func newOkResponse(message string) *Response {
	return &Response{Status: okStatus, Message: message}
}
