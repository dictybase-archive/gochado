package chado

import (
	"fmt"
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

	// Check the number of updatable entries
	var ct int
	_ = dbh.QueryRowx("SELECT COUNT(*) FROM temp_gpad_new").Scan(&ct)
	fmt.Println(ct)
	Expect("SELECT COUNT(*) FROM temp_gpad_new WHERE is_update = 1").Should(HaveCount(4))
}
