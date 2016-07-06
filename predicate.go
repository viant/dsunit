package dsunit

import (
	"fmt"
	"time"

	"github.com/viant/toolbox"
)

type withinSecPredicate struct {
	baseTime       time.Time
	deltaInSeconds int
	dateLayout     string
}

//Apply returns true if passed in time is within deltaInSeconds from baseTime
func (p *withinSecPredicate) Apply(value interface{}) bool {
	timeValue := toolbox.AsTime(value, p.dateLayout)
	if timeValue == nil {
		return false
	}
	difference := int(p.baseTime.Unix() - timeValue.Unix())
	if p.deltaInSeconds >= 0 {
		return difference >= 0 && difference <= int(p.deltaInSeconds)
	}
	return difference <= 0 && difference >= int(p.deltaInSeconds)
}

func (p *withinSecPredicate) ToString() string {
	return fmt.Sprintf(" %v within %v s", p.baseTime, p.deltaInSeconds)
}

//NewWithinPredicate returns new NewWithinPredicate predicate, it takes base time, delta in second, and dateLayout
func NewWithinPredicate(baseTime time.Time, deltaInSeconds int, dateLayout string) toolbox.Predicate {
	return &withinSecPredicate{
		baseTime:       baseTime,
		deltaInSeconds: deltaInSeconds,
		dateLayout:     dateLayout,
	}
}
