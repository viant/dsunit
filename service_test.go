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

	_ "github.com/mattn/go-sqlite3"
	"github.com/viant/dsunit"
)

func init() {
	//dsunit.UseRemoteTestServer("http://localhost:8071")
}

func TestSetup(t *testing.T) {
	dsunit.InitDatastoreFromURL(t, "test://test/datastore_init.json")
	dsunit.ExecuteScriptFromURL(t, "test://test/script_request.json")
	dsunit.PrepareDatastore(t, "bar_test")
	//business test logic comes here
	dsunit.ExpectDatasets(t, "bar_test", dsunit.SnapshotDatasetCheckPolicy)
}

func TestTest1p(t *testing.T) {
	dsunit.InitDatastoreFromURL(t, "test://test/datastore_init.json")
	dsunit.ExecuteScriptFromURL(t, "test://test/script_request.json")

	dsunit.PrepareDatastoreFor(t, "bar_test", "test://test/", "test1")
	//business test logic comes here
	dsunit.ExpectDatasetFor(t, "bar_test", dsunit.SnapshotDatasetCheckPolicy, "test://test/", "test1")
}

func TestDatasetMapping(t *testing.T) {
	dsunit.InitDatastoreFromURL(t, "test://test/datastore_init.json")
	dsunit.ExecuteScriptFromURL(t, "test://test/script_request.json")
	dsunit.PrepareDatastoreFor(t, "bar_test", "test://test/", "mapping")
	dsunit.ExpectDatasetFor(t, "bar_test", dsunit.SnapshotDatasetCheckPolicy, "test://test/", "mapping")
}
