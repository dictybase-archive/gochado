package chado

import (
	"testing"

	. "github.com/dictybase/testchado/matchers"
	. "github.com/onsi/gomega"
)

const (
	ont = "gene_ontology_association"
	acv = "annotation extension terms"
	adb = "dictyBase"
)

func TestGpadUpdateSqlite(t *testing.T) {
	RegisterTestingT(t)
	//Setup
	setup := setUpSqliteTest()
	chado := setup.chado
	dbh := chado.DBHandle()
	sqlite := NewChadoSqlite(dbh, setup.parser, ont, acv, adb)
	// Get the changed(new/updated) entries in another staging tables
	dbh.MustExec(setup.parser.GetSection("insert_latest_goa_from_staging"), ont)
	// Load the first test data in chado schema
	sqlite.RunBulkInserts()
	// Load the second test data(with updated entries) in staging schema
	LoadUpdatedGpadStagingSqlite(dbh, setup.rice, setup.parser)
	// Get the changed(new/updated) entries in another staging tables
	dbh.MustExec(setup.parser.GetSection("insert_latest_goa_from_staging"), ont)
	// Tag the updatable records
	sqlite.MarkUpdatable()
	//Teardown
	defer chado.DropSchema()

	// Total entries that will be transfered to chado
	Expect("SELECT COUNT(*) FROM temp_gpad_new").Should(HaveCount(5))
	// Check the number of new entries
	Expect("SELECT COUNT(*) FROM temp_gpad_new where is_update = 0").Should(HaveCount(1))
	// Check the number of updatable entries
	Expect("SELECT COUNT(*) FROM temp_gpad_new WHERE is_update = 1").Should(HaveCount(4))

	// Now update the records, here we go by delete and insert
	// It will be qualifier, withfrom and extra pubs
	type tstr struct {
		block string
		count int
	}
	tbl := map[string]int{
		"delete_feature_cvtermprop_qualifier": 3,
		"delete_feature_cvtermprop_withfrom":  3,
		"delete_feature_cvterm_pub":           0,
	}
}
