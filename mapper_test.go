package dsunit_test

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/dsunit"
	"testing"
)

func TestService_Map(t *testing.T) {

	service := dsunit.NewMapper()
	assert.False(t, service.Has("V_ORDER"))
	service.Add(&dsunit.Mapping{
		Name: "V_ORDER",
		MappingTable: &dsunit.MappingTable{
			Table: "Order",
			Columns: []*dsunit.MappingColumn{
				{
					Name:       "ID",
					Required:   true,
					Unique:     true,
					FromColumn: "ORDER_ID",
				},
				{
					Name: "NAME",
				},
				{
					Name: "STATUS",
				},
			},
			Associations: []*dsunit.MappingTable{
				{
					Table: "OrderLineItem",
					Columns: []*dsunit.MappingColumn{
						{
							Name:       "ID",
							Required:   true,
							Unique:     true,
							FromColumn: "LINE_ITEM_ID",
						},
						{
							Name:       "ORDER_ID",
							Required:   true,
							FromColumn: "ORDER_ID",
						},
						{
							Name: "PRODUCT",
						},
						{
							Name: "QUANTITY",
						},
						{
							Name: "PRICE",
						},
					},
				},
			},
		},
	})
	assert.True(t, service.Has("V_ORDER"))

	assert.EqualValues(t, 0, len(service.Map(dsunit.NewDataset("ABC"))))

	{
		vOrder := dsunit.NewDataset("V_ORDER",
			map[string]interface{}{
				"Z": 1,
			},
			map[string]interface{}{
				"LINE_ITEM_ID": 1,
				"NAME":         "X Order",
				"ORDER_ID":     1,
				"PRODUCT":      "p1",
				"QUANTITY":     1,
				"PRICE":        3,
			},
			map[string]interface{}{
				"LINE_ITEM_ID": 2,
				"ORDER_ID":     1,
				"STATUS":       1,
				"PRODUCT":      "p2",
				"QUANTITY":     3,
				"PRICE":        4,
			},
		)

		datasets := service.Map(vOrder)
		assert.EqualValues(t, 2, len(datasets))
		{
			dataset := datasets[0]
			assert.NotNil(t, dataset)
			assert.EqualValues(t, "Order", dataset.Table)
			assert.EqualValues(t, 1, len(dataset.Records))
			assert.EqualValues(t, map[string]interface{}{
				"ID":     1,
				"NAME":   "X Order",
				"STATUS": 1,
			}, dataset.Records[0])
		}
		{
			dataset := datasets[1]
			assert.NotNil(t, dataset)
			assert.EqualValues(t, "OrderLineItem", dataset.Table)
			assert.EqualValues(t, 2, len(dataset.Records))
			assert.EqualValues(t, map[string]interface{}{
				"ID":       1,
				"ORDER_ID": 1,
				"PRODUCT":  "p1",
				"QUANTITY": 1,
				"PRICE":    3,
			}, dataset.Records[0])
			assert.EqualValues(t, map[string]interface{}{
				"ID":       2,
				"ORDER_ID": 1,
				"PRODUCT":  "p2",
				"QUANTITY": 3,
				"PRICE":    4,
			}, dataset.Records[1])
		}
	}
}
