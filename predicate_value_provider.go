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
package dsunit

import (
	"fmt"
	"github.com/viant/dsc"
	"time"
	"github.com/viant/toolbox"
)

type betweenPredicateValueProvider struct{}


func (p *betweenPredicateValueProvider) Get(context toolbox.Context, arguments ... interface{}) (interface{}, error) {
	if len(arguments) != 2 {
		return nil, dsUnitError{fmt.Sprintf("Expected 2 arguments with between predicate but had %v", len(arguments))}
	}
	predicate := dsc.NewBetweenPredicate(arguments[0], arguments[1])
	return &predicate, nil
}



func newBetweenPredicateValueProvider() toolbox.ValueProvider {
	var result toolbox.ValueProvider = &betweenPredicateValueProvider{}
	return result
}


type withinSecPredicateValueProvider struct {}


func (p *withinSecPredicateValueProvider) Get(context toolbox.Context, arguments ... interface{}) (interface{}, error) {
	if len(arguments) != 3 {
		return nil, dsUnitError{fmt.Sprintf("Expected 3 arguments <ds:within_sec [timestamp, delta, dateFormat]>  predicate, but had %v", len(arguments))}
	}

	if arguments[0] == "now" {
		arguments[0] = time.Now()
	}
	dateFormat := toolbox.AsString(arguments[2])
	dateLayout := toolbox.DateFormatToLayout(dateFormat)
	targetTime := toolbox.AsTime(arguments[0], dateLayout)
	if targetTime == nil {
		return nil, fmt.Errorf("Unable convert %v to time.Time", arguments[0])
	}
	delta := toolbox.AsInt(arguments[1])
	predicate := NewWithinPredicate(*targetTime, delta, dateLayout)
	return &predicate, nil
}


func newWithinSecPredicateValueProvider() toolbox.ValueProvider {
	var result toolbox.ValueProvider = &withinSecPredicateValueProvider{}
	return result
}