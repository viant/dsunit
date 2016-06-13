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
	"os"
	"testing"
)

const dbUserEnvKey = "DB_TEST_USER"
const dbPasswordEnvKey = "DB_TEST_PASSWORD"

//SkipTestIfNeeded  skips tests if DB_TEST_USER evnironment variable is not set , note that DB_TEST_USER, DB_TEST_PASSWORD needs to be set with valid user datastore credential to run the tests.
func SkipTestIfNeeded(t *testing.T) bool {
	if _, found := os.LookupEnv(dbUserEnvKey); found {
		return false
	}
	t.Skipf("Set the following enviroment variables: %v, %v", dbUserEnvKey, dbPasswordEnvKey)
	return true
}
