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

	"github.com/stretchr/testify/assert"
	"github.com/viant/dsc"
	"github.com/viant/dsunit"
)

func TestTransform(t *testing.T) {
	transformer := dsunit.DatasetTransformer{}

	mapping := &dsunit.DatasetMapping{
		Table: "order_line",
		Columns: []dsunit.DatasetColumn{
			dsunit.DatasetColumn{
				Name:         "id",
				DefaultValue: "<ds:seq[\"order_line\"]>",
				Required:     true,
			},
			dsunit.DatasetColumn{
				Name: "seq",
			},
			dsunit.DatasetColumn{
				Name: "quantity",
			},
			dsunit.DatasetColumn{
				Name:         "create_time",
				DefaultValue: "<ds:current_timestamp>",
			},
			dsunit.DatasetColumn{
				Name: "product_price",
			},
			dsunit.DatasetColumn{
				Name: "product_id",
			},
		},
		Associations: []dsunit.DatasetMapping{
			{
				Table: "products",
				Columns: []dsunit.DatasetColumn{
					dsunit.DatasetColumn{
						Name:       "id",
						Required:   true,
						FromColumn: "product_id",
					},
					dsunit.DatasetColumn{
						Name:       "name",
						FromColumn: "product_name",
					},
					dsunit.DatasetColumn{
						Name:       "price",
						FromColumn: "product_price",
					},
				},
			},
		},
	}

	registry := dsc.NewTableDescriptorRegistry()
	registry.Register(&dsc.TableDescriptor{Table: "order_line", Autoincrement: true, PkColumns: []string{"id"}})
	registry.Register(&dsc.TableDescriptor{Table: "products", PkColumns: []string{"id"}})

	sourceDataset := &dsunit.Dataset{
		TableDescriptor: *registry.Get("order_line"),
		Rows: []dsunit.Row{
			dsunit.Row{
				Values: map[string]interface{}{
					"seq":           1,
					"quantity":      3,
					"product_id":    10,
					"product_price": 12.3,
					"product_name":  "abc",
				},
			},
			dsunit.Row{
				Values: map[string]interface{}{
					"seq":           2,
					"quantity":      5,
					"product_id":    11,
					"product_price": 42.3,
					"product_name":  "xyz",
				},
			},
			dsunit.Row{
				Values: map[string]interface{}{
					"seq":           2,
					"quantity":      5,
					"product_price": 42.3,
					"product_name":  "xyz",
				},
			},
		},
	}
	datasets := transformer.Transform("abc", sourceDataset, mapping, registry)
	assert.Equal(t, 2, len(datasets.Datasets))

	{
		dataset := datasets.Datasets[0]
		assert.Equal(t, "order_line", dataset.Table)
		assert.Equal(t, 3, len(dataset.Rows))

		assert.Equal(t, 6, len(dataset.Rows[0].Values), "should have all columns set for dataset")
		assert.Equal(t, "<ds:seq[\"order_line\"]>", dataset.Rows[0].Values["id"], "default value should be set")
		assert.Equal(t, 12.3, dataset.Rows[0].Values["product_price"], "value should be set")
	}
	{
		dataset := datasets.Datasets[1]
		assert.Equal(t, "products", dataset.Table)
		assert.Equal(t, 2, len(dataset.Rows), "product_id not present in the third row")
		assert.Equal(t, 3, len(dataset.Rows[0].Values), "should have all columns set for dataset")
		assert.Equal(t, 10, dataset.Rows[0].Values["id"], "should set value from column")

	}

}
