package dsunit_test
//
//import (
//	"testing"
//
//	"github.com/stretchr/testify/assert"
//	"github.com/viant/dsc"
//	"github.com/viant/dsunit"
//)
//
//func TestTransform(t *testing.T) {
//	transformer := dsunit.DatasetTransformer{}
//
//	mapping := &dsunit.DatasetMapping{
//		Table: "order_line",
//		Columns: []*dsunit.DatasetColumn{
//			{
//				Name:         "id",
//				DefaultValue: "<ds:seq[\"order_line\"]>",
//				Required:     true,
//			},
//			{
//				Name: "seq",
//			},
//			{
//				Name: "quantity",
//			},
//			{
//				Name:         "create_time",
//				DefaultValue: "<ds:current_timestamp>",
//			},
//			{
//				Name: "product_price",
//			},
//			{
//				Name: "product_id",
//			},
//		},
//		Associations: []*dsunit.DatasetMapping{
//			{
//				Table: "products",
//				Columns: []*dsunit.DatasetColumn{
//					{
//						Name:       "id",
//						Required:   true,
//						FromColumn: "product_id",
//					},
//					{
//						Name:       "name",
//						FromColumn: "product_name",
//					},
//					{
//						Name:       "price",
//						FromColumn: "product_price",
//					},
//				},
//			},
//		},
//	}
//
//	registry := dsc.NewTableDescriptorRegistry()
//	registry.Register(&dsc.TableDescriptor{Table: "order_line", Autoincrement: true, PkColumns: []string{"id"}})
//	registry.Register(&dsc.TableDescriptor{Table: "products", PkColumns: []string{"id"}})
//
//	sourceDataset := &dsunit.Dataset{
//		TableDescriptor: registry.Get("order_line"),
//		Rows: []*dsunit.Record{
//			{
//				Values: map[string]interface{}{
//					"seq":           1,
//					"quantity":      3,
//					"product_id":    10,
//					"product_price": 12.3,
//					"product_name":  "abc",
//				},
//			},
//			{
//				Values: map[string]interface{}{
//					"seq":           2,
//					"quantity":      5,
//					"product_id":    11,
//					"product_price": 42.3,
//					"product_name":  "xyz",
//				},
//			},
//			{
//				Values: map[string]interface{}{
//					"seq":           2,
//					"quantity":      5,
//					"product_price": 42.3,
//					"product_name":  "xyz",
//				},
//			},
//		},
//	}
//	datasets := transformer.Transform("abc", sourceDataset, mapping, registry)
//	assert.Equal(t, 2, len(datasets.DatastoreDatasets))
//
//	{
//		dataset := datasets.DatastoreDatasets[0]
//		assert.Equal(t, "order_line", dataset.Table)
//		assert.Equal(t, 3, len(dataset.Rows))
//
//		assert.Equal(t, 6, len(dataset.Rows[0].Values), "should have all columns set for dataset")
//		assert.Equal(t, "<ds:seq[\"order_line\"]>", dataset.Rows[0].Values["id"], "default value should be set")
//		assert.Equal(t, 12.3, dataset.Rows[0].Values["product_price"], "value should be set")
//	}
//	{
//		dataset := datasets.DatastoreDatasets[1]
//		assert.Equal(t, "products", dataset.Table)
//		assert.Equal(t, 2, len(dataset.Rows), "product_id not present in the third row")
//		assert.Equal(t, 3, len(dataset.Rows[0].Values), "should have all columns set for dataset")
//		assert.Equal(t, 10, dataset.Rows[0].Values["id"], "should set value from column")
//
//	}
//
//}
