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
