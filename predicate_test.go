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
package dsunit_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/viant/dsunit"
)

func TestWithinPredicate(t *testing.T) {
	targetTime := time.Unix(1465009041, 0)
	predicate := dsunit.NewWithinPredicate(targetTime, -2, "")
	timeValue := time.Unix(1465009042, 0)
	assert.True(t, predicate.Apply(timeValue))

	timeValue = time.Unix(1465009044, 0)
	assert.False(t, predicate.Apply(timeValue))
}
