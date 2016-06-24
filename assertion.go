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

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/viant/toolbox"
)

const (
	//ViolationTypeInvalidRowCount 'InvalidRowCount' constant used to report rows discrepancy between expected and actual dataset table.
	ViolationTypeInvalidRowCount = "InvalidRowCount"
	//ViolationTypeMissingActualRow 'AssertionMissingActualRow' constant used to report missing actual row.
	ViolationTypeMissingActualRow = "AssertionMissingActualRow"
	//ViolationTypeRowNotEqual 'AssertionRowNotEqual; constant used to report difference in values in the expected and actual row.
	ViolationTypeRowNotEqual = "AssertionRowNotEqual"
)

type assertViolations struct {
	violations []AssertViolation
}

func (v *assertViolations) Violations() []AssertViolation {
	return v.violations
}

func (v *assertViolations) HasViolations() bool {
	return len(v.violations) > 0
}

func (v *assertViolations) String() string {
	result := ""
	var aggregate = make(map[string](*[]AssertViolation))
	var aggregateKey = make([]string, 0)
	var ok bool
	for _, violation := range v.violations {
		key := violation.Datastore + "." + violation.Table
		var casesForThisKey *[]AssertViolation
		if casesForThisKey, ok = aggregate[key]; !ok {
			var newViolations = make([]AssertViolation, 0)
			casesForThisKey = &newViolations
			aggregate[key] = casesForThisKey
			aggregateKey = append(aggregateKey, key)
		}
		(*casesForThisKey) = append(*casesForThisKey, violation)
	}

	for _, key := range aggregateKey {
		result = result + "--  " + key + " --\n\t"
		var previousViolationType = ""

		violations, _ := aggregate[key]

		for _, item := range *violations {
			var violation = item
			if violation.Type == ViolationTypeInvalidRowCount {
				result = result + fmt.Sprintf("expected %v rows but had %v\n\t", violation.Expected, violation.Actual)
			}
			if violation.Type == ViolationTypeMissingActualRow {
				if previousViolationType != violation.Type {
					result = result + "The following rows were missing:\n\t\t"
					previousViolationType = violation.Type
				}
				result = result + fmt.Sprintf("%v :%v \n\t\t", violation.Key, violation.Expected)

			}
			if violation.Type == ViolationTypeRowNotEqual {
				if previousViolationType != violation.Type {
					result = result + "\n\tThe following rows were different:\n\t\t"
					previousViolationType = violation.Type
				}
				result = result + fmt.Sprintf("%v :fmt: %v !=  actual: %v \n\t\t", violation.Key, violation.Expected, violation.Actual)
			}
		}

	}
	return result
}

func newAssertViolations(violations []AssertViolation) AssertViolations {
	return &assertViolations{violations: violations}
}

//DatasetTester represent a dataset tester.
type DatasetTester struct {
	dateLayout string
}

//Assert compares expected and actual dataset, it reports and violations as result.
func (t DatasetTester) Assert(datastore string, expected, actual *Dataset) []AssertViolation {
	var result = make([]AssertViolation, 0)
	violation := t.assertRowCount(datastore, expected, actual)
	if violation != nil {
		result = append(result, *violation)
	}

	violations := t.assertRows(datastore, expected, actual)
	if len(violations) > 0 {
		result = append(result, violations...)
	}
	return result
}

func (t DatasetTester) assertRowCount(datastore string, expected, actual *Dataset) *AssertViolation {
	if len(actual.Rows) != len(expected.Rows) {
		return &AssertViolation{
			Datastore: datastore,
			Table:     expected.TableDescriptor.Table,
			Type:      ViolationTypeInvalidRowCount,
			Expected:  toolbox.AsString(len(expected.Rows)),
			Actual:    toolbox.AsString(len(actual.Rows)),
			Source:    "",
		}
	}
	return nil
}

func (t DatasetTester) assertRows(datastore string, expected, actual *Dataset) []AssertViolation {
	var result = make([]AssertViolation, 0)
	actualRows := indexDataset(*actual)
	expectedRows := indexDataset(*expected)
	expectedKeys := toolbox.SortStrings(toolbox.MapKeysToStringSlice(expectedRows))
	for _, key := range expectedKeys {
		expectedRow := expectedRows[key]
		actualRow, ok := actualRows[key]
		if !ok {
			result = append(result, AssertViolation{
				Datastore: datastore,
				Type:      ViolationTypeMissingActualRow,
				Table:     expected.TableDescriptor.Table,
				Key:       key,
				Expected:  expectedRow.String(),
				Actual:    "",
				Source:    expectedRow.Source,
			})
			continue
		}

		violated := t.assertRow(datastore, key, expected, expectedRow, actualRow)
		if len(violated) > 0 {
			result = append(result, violated...)
		}

	}
	return result
}

func (t DatasetTester) assertRow(datastore, key string, expectedDataset *Dataset, expected, actual Row) []AssertViolation {
	var result = make([]AssertViolation, 0)
	expectedDiff, actualDiff := "", ""
	var sortedColumns = toolbox.SortStrings(expected.Columns())
	for _, column := range sortedColumns {
		expectedValue := expected.Value(column)
		var actualValue interface{}
		if actual.HasColumn(column) {
			actualValue = actual.Value(column)
		} else {
			actualValue = nil
		}

		if expectedValue == nil && actualValue == nil {
			continue
		}
		quote := ""
		if reflect.TypeOf(expectedValue).Kind() == reflect.Ptr {
			expectedValue = reflect.ValueOf(expectedValue).Elem().Interface()
		}

		if reflect.ValueOf(expectedValue).Kind() == reflect.String {
			quote = "\""
		}

		if !isEqual(expectedValue, actualValue, t.dateLayout) {
			if len(expectedDiff) > 0 {
				expectedDiff = expectedDiff + ","
				actualDiff = actualDiff + ","
			}
			expectedDiff = fmt.Sprintf("%v%v:%v%v%v", expectedDiff, column, quote, expectedValue, quote)
			actualDiff = fmt.Sprintf("%v%v:%v%v%v", actualDiff, column, quote, actualValue, quote)
		}
	}
	if len(expectedDiff) > 0 {
		result = append(result, AssertViolation{
			Datastore: datastore,
			Type:      ViolationTypeRowNotEqual,
			Table:     expectedDataset.TableDescriptor.Table,
			Key:       key,
			Expected:  "{" + expectedDiff + "}",
			Actual:    "{" + actualDiff + "}",
			Source:    expected.Source,
		})
	}
	return result
}

func isFloatEqual(floatValue interface{}, value interface{}) bool {
	if toolbox.IsString(value) || toolbox.IsInt(value) {
		floatAsText := toolbox.AsString(floatValue)
		valueAsText := toolbox.AsString(value)
		if floatAsText == valueAsText {
			return true
		}
		valueAsFloat, err := strconv.ParseFloat(valueAsText, reflect.TypeOf(floatValue).Bits())
		if err == nil {
			return valueAsFloat == floatValue
		}

	}
	return false
}

func isBooleanEqual(boolValue interface{}, value interface{}) bool {
	actualBool := toolbox.AsString(value) == "1"
	return boolValue == actualBool
}

func isTimeEqual(timeValue interface{}, value interface{}) bool {
	timeValueAsTime := timeValue.(time.Time)
	if toolbox.IsString(value) {
		valueAsText := value.(string)
		timeValueAsText := timeValueAsTime.String()
		if len(valueAsText) < len(timeValueAsText) {
			timeValueAsText = timeValueAsText[0:len(valueAsText)]
		}
		if timeValueAsText == valueAsText {
			return true
		}
		if toolbox.CanConvertToFloat(value) {
			unixTimestamp := int(toolbox.AsFloat(value))
			actualTime := time.Unix(int64(unixTimestamp), 0)
			return actualTime.Equal(timeValueAsTime)

		}

	}
	return false
}

func isMapEqual(expected, actual interface{}, dateLayout string) bool {
	var expectedMap = make(map[string]interface{})
	toolbox.ProcessMap(expected, func(key, value interface{}) bool {
		expectedMap[toolbox.AsString(key)] = value
		return true
	})

	var actualMap = make(map[string]interface{})
	toolbox.ProcessMap(actual, func(key, value interface{}) bool {
		actualMap[toolbox.AsString(key)] = value
		return true
	})

	for expectedKey, expectedValue := range expectedMap {
		if actualValue, found := actualMap[expectedKey]; found {
			isEqual := isEqual(expectedValue, actualValue, dateLayout)
			if !isEqual {
				return false
			}
		}
	}
	return true
}

func isSliceEqual(expected, actual interface{}, dateLayout string) bool {
	var expectedSlice = make([]interface{}, 0)
	toolbox.ProcessSlice(expected, func(item interface{}) bool {
		expectedSlice = append(expectedSlice, item)
		return true
	})
	var actualSlice = make([]interface{}, 0)
	toolbox.ProcessSlice(actual, func(item interface{}) bool {
		actualSlice = append(actualSlice, item)
		return true
	})
	if len(actualSlice) != len(expectedSlice) {

		return false
	}
	for i := range expectedSlice {
		isEqual := isEqual(expectedSlice[i], actualSlice[i], dateLayout)
		if !isEqual {
			return false
		}
	}
	return true
}

func isEqual(expected, actual interface{}, dateLayout string) bool {
	if expected == nil || actual == nil {
		return expected == actual
	}

	actualType := reflect.TypeOf(actual)
	if actualType == nil {
		return false
	}

	expectedValue := reflect.ValueOf(expected)
	expectedType := expectedValue.Type()
	if reflect.DeepEqual(expected, actual) {
		return true
	}

	switch value := expected.(type) {
	case bool:
		if toolbox.IsInt(actual) {
			return isBooleanEqual(expected, actual)
		} else if toolbox.IsString(actual) {
			if actualBool, err := strconv.ParseBool(toolbox.AsString(actual)); err == nil {
				return actualBool == value
			}
		}
	case int, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		if toolbox.IsBool(actual) {
			return isBooleanEqual(actual, expected)
		} else if toolbox.IsFloat(actual) {

			return isFloatEqual(actual, expected)
		} else if toolbox.IsString(actual) {
			actualFloatValue, _ := strconv.ParseFloat(toolbox.AsString(actual), 64)
			return isFloatEqual(actualFloatValue, expected)
		}

	case float32, float64:
		if toolbox.IsString(actual) || toolbox.IsInt(actual) {
			return isFloatEqual(expected, actual)
		}
	case string:

		if toolbox.IsFloat(actual) {
			return isFloatEqual(actual, expected)
		} else if toolbox.IsTime(actual) {
			return isTimeEqual(actual, expected)
		} else if toolbox.IsString(actual) {

			if expectedValue == actual {
				return true
			}
			expectedTime, err := toolbox.ParseTime(value, dateLayout)
			if err == nil {
				return isTimeEqual(expectedTime, actual)
			}
		}
	case time.Time:
		if toolbox.IsString(actual) {
			return isTimeEqual(expected, actual)
		}

	case toolbox.Predicate:
		return value.Apply(actual)

	case *toolbox.Predicate:
		return (*value).Apply(actual)

	case reflect.Value:
		expectedValue, converted := expected.(reflect.Value)
		if converted {
			if value.Interface() == expectedValue.Interface() {
				return true
			}
		}
	}

	if expectedType.Kind() == reflect.Ptr {
		expectedType = expectedType.Elem()
		expected = reflect.ValueOf(expected).Elem().Interface()
	}
	if actualType.Kind() == reflect.Ptr {
		actualType = actualType.Elem()
		actual = reflect.ValueOf(actual).Elem().Interface()
	}

	if expectedType.Kind() == reflect.Map {
		return isMapEqual(expected, actual, dateLayout)
	}

	if expectedType.Kind() == reflect.Slice || expectedType.Kind() == reflect.Array {
		return isSliceEqual(expected, actual, dateLayout)
	}

	if expectedValue.IsValid() && expectedType.ConvertibleTo(actualType) {
		expected = expectedValue.Convert(actualType).Interface()
	}
	if reflect.DeepEqual(expected, actual) {
		return true
	}

	return false

}

func indexDataset(dataset Dataset) map[string]Row {
	var result = make(map[string]Row)
	toolbox.IndexSlice(dataset.Rows, result, func(row Row) string {
		var pkValues = make([]string, 0)
		toolbox.TransformSlice(dataset.PkColumns, &pkValues, func(pkColumn string) string {
			return row.ValueAsString(pkColumn)
		})
		return strings.Join(pkValues, ",")
	})
	return result
}
