package chado

import (
	"testing"

	. "github.com/dictybase/testchado/matchers"
	. "github.com/onsi/gomega"
)

func TestGpadChadoSqlite(t *testing.T) {
	RegisterTestingT(t)
	//Setup
	setup := setUpSqliteTest()
	chado := setup.chado
	p := setup.parser
	ont := "gene_ontology_association"
	//Teardown
	defer chado.DropSchema()

	dbh := chado.DBHandle()

	//check for all changed gpad records
	_, err := dbh.Exec(p.GetSection("insert_latest_goa_from_staging"), ont)
	Expect(err).ShouldNot(HaveOccurred())
	Expect("SELECT COUNT(*) FROM temp_gpad_new").Should(HaveCount(12))

	//check for gpad count in chado
	var count int
	err = dbh.QueryRowx(p.GetSection("count_all_gpads_from_chado"), ont).Scan(&count)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(count).Should(Equal(0))
	type gpad struct {
		DbId       string `db:"dbid"`
		GoId       string
		Evcode     string
		AssignedBy string `db:"value"`
	}
	gp := []gpad{}
	err = dbh.Select(&gp, p.GetSection("select_all_gpads_from_chado"), ont)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(gp).Should(HaveLen(0))

	//insert new records
	dbh.MustExec(p.GetSection("insert_feature_cvterm"))
	Expect("SELECT COUNT(*) FROM feature_cvterm").Should(HaveCount(10))
	dbh.MustExec(p.GetSection("insert_feature_cvtermprop_evcode"))
	Expect("SELECT COUNT(*) FROM feature_cvtermprop").Should(HaveCount(10))

	q := `
	SELECT COUNT(*) FROM feature_cvtermprop
	WHERE type_id = (
	SELECT cvterm_id FROM cvterm
	JOIN cv ON cv.cv_id = cvterm.cv_id
	WHERE cv.name = 'gene_ontology_association'
	AND cvterm.name = $1
	)
	`
	m := make(map[string]interface{})
	m["params"] = append(make([]interface{}, 0), "qualifier")
	m["count"] = 10
	dbh.MustExec(p.GetSection("insert_feature_cvtermprop_qualifier"), ont)
	Expect(q).Should(HaveNameCount(m))

	m["params"] = append(make([]interface{}, 0), "date")
	dbh.MustExec(p.GetSection("insert_feature_cvtermprop_date"), ont)
	Expect(q).Should(HaveNameCount(m))

	m["params"] = append(make([]interface{}, 0), "source")
	dbh.MustExec(p.GetSection("insert_feature_cvtermprop_assigned_by"), ont)
	Expect(q).Should(HaveNameCount(m))

	m["params"] = append(make([]interface{}, 0), "with")
	m["count"] = 6
	dbh.MustExec(p.GetSection("insert_feature_cvtermprop_withfrom"), ont)
	Expect(q).Should(HaveNameCount(m))

	dbh.MustExec(p.GetSection("insert_feature_cvterm_pub_reference"))
	Expect("SELECT COUNT(*) FROM feature_cvterm_pub").Should(HaveCount(1))
}

func TestGpadChadoSqliteBulk(t *testing.T) {
	RegisterTestingT(t)
	//Setup
	setup := setUpSqliteTest()
	chado := setup.chado
	dbh := chado.DBHandle()
	ont := "gene_ontology_association"
	acv := "annotation extension terms"
	adb := "dictyBase"
	//Teardown
	defer chado.DropSchema()

	sqlite := NewChadoSqlite(dbh, setup.parser, ont, acv, adb)
	sqlite.BulkLoad()
	Expect("SELECT COUNT(*) FROM temp_gpad_new").Should(HaveCount(12))
	Expect("SELECT COUNT(*) FROM feature_cvterm").Should(HaveCount(13))
	eq := `
SELECT COUNT(*)  FROM feature_cvtermprop
JOIN cvterm ON cvterm.cvterm_id = feature_cvtermprop.type_id
JOIN cv ON cv.cv_id = cvterm.cv_id
WHERE cv.name = "eco"
`
	Expect(eq).Should(HaveCount(13))
	tbl := map[string]int{
		"qualifier": 13,
		"date":      13,
		"source":    13,
		"with":      6,
		"pub":       1,
	}
	runAnonCvtImplExplCounts(ont, tbl)
}
