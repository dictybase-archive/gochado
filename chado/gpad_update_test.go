package chado

import (
	"testing"

	. "github.com/dictybase/testchado/matchers"
	"github.com/jmoiron/sqlx"
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
	p := setup.parser
	sqlite := NewChadoSqlite(dbh, p, ont, acv, adb)
	// Get the changed(new/updated) entries in another staging tables
	dbh.MustExec(p.GetSection("insert_latest_goa_from_staging"), ont)
	// Load the first test data in chado schema
	sqlite.RunBulkInserts()
	// Load the second test data(with updated entries) in staging schema
	LoadUpdatedGpadStagingSqlite(dbh, setup.rice, p)
	// Get the changed(new/updated) entries in another staging tables
	dbh.MustExec(p.GetSection("insert_latest_goa_from_staging"), ont)
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

	tbl := map[string]int{
		"delete_feature_cvtermprop_qualifier": 4,
		"delete_feature_cvtermprop_withfrom":  4,
	}
	for k, v := range tbl {
		runRegularGpadExpl(dbh, p.GetSection(k), v, ont)
	}
	runRegularGpadImpl(dbh, p.GetSection("delete_feature_cvterm_pub"), 0)

	// Run bulk insert again to insert new record
	sqlite.RunBulkInserts()
	Expect("SELECT COUNT(*) FROM feature_cvterm").Should(HaveCount(14))

	//check for rest of the counts
	tbl2 := map[string]int{
		"qualifier": 10,
		"with":      2,
		"date":      14,
		"source":    14,
		"pub":       1,
	}
	runCvtImplExplCounts(dbh, ont, tbl2)

	// Now the bulk update
	sqlite.RunBulkUpdates()
	//check for counts
	tbl2 = map[string]int{
		//"qualifier": 14,
		//"with":      7,
		//"date":      14,
		//"source":    14,
		"pub": 3,
	}
	runCvtImplExplCounts(dbh, ont, tbl2)
}

func runRegularGpadImpl(dbh *sqlx.DB, section string, expected int) {
	res, err := dbh.Exec(section)
	Expect(err).ShouldNot(HaveOccurred())
	rc, err := res.RowsAffected()
	Expect(err).ShouldNot(HaveOccurred())
	Expect(int(rc)).Should(Equal(expected))
}
func runRegularGpadExpl(dbh *sqlx.DB, section string, expected int, cv string) {
	res, err := dbh.Exec(section, cv)
	Expect(err).ShouldNot(HaveOccurred())
	rc, err := res.RowsAffected()
	Expect(err).ShouldNot(HaveOccurred())
	Expect(int(rc)).Should(Equal(expected))
}

func runCvtImplExplCounts(dbh *sqlx.DB, ont string, tbl map[string]int) {
	q := `
SELECT COUNT(*) FROM feature_cvtermprop
WHERE type_id = (
SELECT cvterm_id FROM cvterm
JOIN cv ON cv.cv_id = cvterm.cv_id
WHERE cv.name = $1
AND cvterm.name = $2
)
`
	for _, term := range []string{"qualifier", "date", "source", "with"} {
		if _, ok := tbl[term]; ok {
			var ct int
			err := dbh.QueryRowx(q, ont, term).Scan(&ct)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(ct).Should(Equal(tbl[term]))
		}
	}
	if _, ok := tbl["pub"]; ok {
		var ct int
		err := dbh.QueryRowx("SELECT COUNT(*) FROM feature_cvterm_pub").Scan(&ct)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(ct).Should(Equal(tbl["pub"]))
	}
}
