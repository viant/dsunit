/*
 *
 *
 * Copyright 2012-2016 Viant.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 *
 */

// Package dsunit -
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
