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
	"time"
	"fmt"
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
		baseTime: baseTime,
		deltaInSeconds:deltaInSeconds,
		dateLayout:dateLayout,
	}
}
