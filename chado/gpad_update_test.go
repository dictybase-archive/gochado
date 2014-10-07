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
	//Teardown
	defer chado.DropSchema()

	dbh := chado.DBHandle()
	p := setup.parser
	sqlite := NewChadoSqlite(dbh, p, ont, acv, adb)
	// Get the first set of changed(new/updated) entries in another staging tables
	dbh.MustExec(p.GetSection("insert_latest_goa_from_staging"), ont)
	// Load the first test data in chado schema
	sqlite.RunBulkInserts()
	// Load the second test data(with updated entries) in staging schema
	LoadUpdatedGpadStagingSqlite(dbh, setup.rice, p)
	// Get the second set of changed(new/updated) entries in another staging tables
	dbh.MustExec(p.GetSection("insert_latest_goa_from_staging"), ont)
	// Tag the updatable records
	sqlite.MarkUpdatable()

	// Total entries that will be transfered to chado
	Expect("SELECT COUNT(*) FROM temp_gpad_new").Should(HaveCount(7))
	// Check the number of new entries
	Expect("SELECT COUNT(*) FROM temp_gpad_new where is_update = 0").Should(HaveCount(1))
	// Check the number of updatable entries
	Expect("SELECT COUNT(*) FROM temp_gpad_new WHERE is_update = 1").Should(HaveCount(6))

	tbl := map[string]int{
		"delete_feature_cvtermprop_qualifier": 4,
		"delete_feature_cvtermprop_withfrom":  4,
	}
	for k, v := range tbl {
		runRegularGpadExpl(dbh, p.GetSection(k), v, ont)
	}
	runRegularGpadImpl(dbh, p.GetSection("delete_feature_cvterm_pub"), 0)

	// Run bulk insert again to insert new record(s)
	sqlite.RunBulkInserts()
	var ct int
	err := dbh.QueryRowx("SELECT COUNT(*) FROM feature_cvterm").Scan(&ct)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(ct).Should(Equal(16))

	//check for rest of the counts
	tbl2 := map[string]int{
		"qualifier": 12,
		"with":      2,
		"date":      16,
		"source":    16,
		"pub":       1,
	}
	runCvtImplExplCounts(dbh, ont, tbl2)

	// Now the bulk update
	sqlite.RunBulkUpdates()
	////check for counts
	tbl3 := map[string]int{
		"qualifier": 18,
		"with":      8,
		"date":      16,
		"source":    16,
		"pub":       3,
	}
	runCvtImplExplCounts(dbh, ont, tbl3)
	// check for updating the date field
	dq := `
	SELECT fcvprop.value FROM feature_cvtermprop fcvprop
	JOIN feature_cvterm fcvt ON
	fcvt.feature_cvterm_id = fcvprop.feature_cvterm_id
	JOIN feature ON feature.feature_id = fcvt.feature_id
	JOIN cvterm ON cvterm.cvterm_id = fcvprop.type_id
	JOIN cv ON cv.cv_id = cvterm.cv_id
	WHERE feature.uniquename = $1
	AND
	cvterm.name = 'date'
	AND cv.name = $2
	`
	var dt string
	err = dbh.QueryRowx(dq, "DDB_G0272003", ont).Scan(&dt)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(dt).Should(Equal("20140229"))
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
