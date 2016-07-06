package dsunit

import (
	"fmt"
	"time"

	"github.com/viant/dsc"
	"github.com/viant/toolbox"
)

type betweenPredicateValueProvider struct{}

func (p *betweenPredicateValueProvider) Get(context toolbox.Context, arguments ...interface{}) (interface{}, error) {
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

type withinSecPredicateValueProvider struct{}

func (p *withinSecPredicateValueProvider) Get(context toolbox.Context, arguments ...interface{}) (interface{}, error) {
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
