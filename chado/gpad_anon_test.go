package chado

import (
	"database/sql"
	"strconv"
	"testing"

	"github.com/dictybase/gochado"
	. "github.com/dictybase/testchado/matchers"
	"github.com/jmoiron/sqlx"
	. "github.com/onsi/gomega"
)

func TestAnonCvtChadoSqlite(t *testing.T) {
	RegisterTestingT(t)
	//Setup
	setup := setUpSqliteTest()
	chado := setup.chado
	p := setup.parser
	dbh := chado.DBHandle()
	//Teardown
	defer chado.DropSchema()

	//check for all changed gpad records
	_, err := dbh.Exec(p.GetSection("insert_latest_goa_from_staging"), ont)
	Expect(err).ShouldNot(HaveOccurred())

	// Create all anon cvterms
	type anon struct {
		Name         string
		Digest       string
		Id           string
		Db           string
		Relationship string
	}

	an := []anon{}
	err = dbh.Select(&an, p.GetSection("select_anon_cvterm"))
	Expect(err).ShouldNot(HaveOccurred())
	Expect(an).Should(HaveLen(3))
	for _, a := range an {
		var ct int
		// check if the record already exist
		err := dbh.QueryRowx(p.GetSection("count_anon_cvterm_from_chado"), acv, "dictyBase", a.Name).Scan(&ct)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(ct).Should(Equal(0))
		q := p.GetSection("update_temp_with_anon_cvterm")
		_, err = dbh.Exec(q, a.Name, a.Digest, a.Id, a.Db, a.Relationship)
		Expect(err).ShouldNot(HaveOccurred())
	}
	// insert anon cvterms in chado
	_, err = dbh.Exec(p.GetSection("insert_anon_cvterm_in_dbxref"), "dictyBase")
	Expect(err).ShouldNot(HaveOccurred())
	_, err = dbh.Exec(p.GetSection("insert_anon_cvterm"), acv, "dictyBase")
	Expect(err).ShouldNot(HaveOccurred())
	for _, a := range an {
		Expect(chado).Should(HaveCvterm(a.Name))
		Expect(chado).Should(HaveDbxref("dictyBase:" + a.Name))
	}

	// insert database/sequence identifers that comes in the identifier
	// part of annotation extensions
	_ = dbh.MustExec(p.GetSection("insert_anon_cvterm_db_identifier"))
	_ = dbh.MustExec(p.GetSection("insert_anon_cvterm_dbxref_identifier"))

	//insert anon cvterms relationships
	runAnonCvtRelationShip(dbh, p, acv)
	// insert anon cvtermprop for database/sequence identifies
	runAnonCvprop(dbh, p, acv)
	//insert all gpad entries expect the extensions
	doRegularGpadInserts(dbh, p, ont)
	// insert new feature_cvterm with anon terms
	runAnonCvtInserts(dbh, p, acv)
	// insert implicit and explicit columns
	runAnonCvtImplExplInserts(dbh, p, acv, ont)
	// check for total count after all insertions
	tbl := map[string]int{
		"qualifier": 13,
		"date":      13,
		"source":    13,
		"with":      6,
		"pub":       1,
	}
	runAnonCvtImplExplCounts(ont, tbl)
}

func runAnonCvtImplExplInserts(dbh *sqlx.DB, p *gochado.SqlParser, acv string, ont string) {
	tbl := [][]string{
		[]string{"insert_anon_feature_cvtermprop_evcode", acv, "", "3"},
		[]string{"insert_anon_feature_cvtermprop_qualifier", ont, acv, "3"},
		[]string{"insert_anon_feature_cvtermprop_date", ont, acv, "3"},
		[]string{"insert_anon_feature_cvtermprop_withfrom", ont, acv, "0"},
		[]string{"insert_anon_feature_cvtermprop_assigned_by", ont, acv, "3"},
		[]string{"insert_anon_feature_cvterm_pub_reference", acv, "", "0"},
	}

	for _, entry := range tbl {
		var res sql.Result
		var err error
		if len(entry[2]) == 0 {
			res, err = dbh.Exec(p.GetSection(entry[0]), entry[1])
		} else {
			res, err = dbh.Exec(p.GetSection(entry[0]), entry[1], entry[2])
		}
		Expect(err).ShouldNot(HaveOccurred())
		rc, err := res.RowsAffected()
		Expect(err).ShouldNot(HaveOccurred())
		count, err := strconv.Atoi(entry[3])
		Expect(err).ShouldNot(HaveOccurred())
		Expect(int(rc)).Should(Equal(count))
	}

}

func runAnonCvtInserts(dbh *sqlx.DB, p *gochado.SqlParser, acv string) {
	res, err := dbh.Exec(p.GetSection("insert_anon_feature_cvterm"), acv)
	Expect(err).ShouldNot(HaveOccurred())
	rc, err := res.RowsAffected()
	Expect(err).ShouldNot(HaveOccurred())
	Expect(int(rc)).Should(Equal(3))
}

func doRegularGpadInserts(dbh *sqlx.DB, p *gochado.SqlParser, ont string) {
	// Now fill up the feature_cvterm
	dbh.MustExec(p.GetSection("insert_feature_cvterm"))
	// Evidence code
	dbh.MustExec(p.GetSection("insert_feature_cvtermprop_evcode"), 0)
	// Extra references
	dbh.MustExec(p.GetSection("insert_feature_cvterm_pub_reference"), 0)
	sections := []string{
		"feature_cvtermprop_qualifier",
		"feature_cvtermprop_date",
		"feature_cvtermprop_assigned_by",
		"feature_cvtermprop_withfrom",
	}
	for _, s := range sections {
		s = "insert_" + s
		dbh.MustExec(p.GetSection(s)+";", ont, 0)
	}
}

func runAnonCvprop(dbh *sqlx.DB, p *gochado.SqlParser, acv string) {
	res, err := dbh.Exec(p.GetSection("insert_anon_cvtermprop_extension"), acv)
	Expect(err).ShouldNot(HaveOccurred())
	rc, err := res.RowsAffected()
	Expect(err).ShouldNot(HaveOccurred())
	Expect(int(rc)).Should(Equal(2))

	q1 := `
	SELECT cvtermprop.value FROM cvtermprop
	JOIN cvterm ON cvterm.cvterm_id = cvtermprop.type_id
	JOIN cv ON cv.cv_id = cvterm.cv_id
	WHERE cvterm.name = $1
	AND cv.name = 'go/extensions/gorel'
	`
	var val string
	err = dbh.QueryRowx(q1, "has_regulation_target").Scan(&val)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(val).Should(Equal("UniProtKB:Q54BD4"))

	var val2 string
	err = dbh.QueryRowx(q1, "in_presence_of").Scan(&val2)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(val2).Should(Equal("CHEBI:64672"))
}

func runAnonCvtRelationShip(dbh *sqlx.DB, p *gochado.SqlParser, acv string) {
	_, err := dbh.Exec(p.GetSection("insert_anon_cvterm_rel_original"), "ro", acv)
	Expect(err).ShouldNot(HaveOccurred())
	_, err = dbh.Exec(p.GetSection("insert_anon_cvterm_rel_extension"), acv)
	Expect(err).ShouldNot(HaveOccurred())

	q1 := `
	SELECT anon_cvterm name FROM temp_gpad_extension
	WHERE anon_cvterm IS NOT NULL
	AND relationship = $1
	`
	q2 := `
	SELECT COUNT(*) FROM cvterm_relationship
	JOIN cvterm subject ON
	subject.cvterm_id = cvterm_relationship.subject_id
	JOIN cvterm relation ON
	relation.cvterm_id = cvterm_relationship.type_id
	JOIN cv ON
	cv.cv_id = subject.cv_id
	WHERE subject.name = $1
	AND cv.name = $2
	AND relation.name = $3
	`

	tbl := map[string]int{
		"exists_during":         1,
		"has_regulation_target": 0,
		"in_presence_of":        0,
	}
	for k, v := range tbl {
		var aterm string
		err = dbh.QueryRowx(q1, k).Scan(&aterm)
		Expect(err).ShouldNot(HaveOccurred())
		// is_a relationship with original go term
		var c1 int
		err = dbh.QueryRowx(q2, aterm, acv, "is_a").Scan(&c1)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(c1).Should(Equal(1))
		// relationship with extension identifier
		var c2 int
		err = dbh.QueryRowx(q2, aterm, acv, k).Scan(&c2)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(c2).Should(Equal(v))
	}
}

func runAnonCvtImplExplCounts(ont string, tbl map[string]int) {
	q := `
SELECT COUNT(*) FROM feature_cvtermprop
WHERE type_id = (
SELECT cvterm_id FROM cvterm
JOIN cv ON cv.cv_id = cvterm.cv_id
WHERE cv.name = $1
AND cvterm.name = $2
)
`
	m := make(map[string]interface{})
	m["params"] = append(make([]interface{}, 0), ont, "qualifier")
	m["count"] = tbl["qualifier"]
	Expect(q).Should(HaveNameCount(m))

	m["params"] = append(make([]interface{}, 0), ont, "date")
	m["count"] = tbl["date"]
	Expect(q).Should(HaveNameCount(m))

	m["params"] = append(make([]interface{}, 0), ont, "source")
	m["count"] = tbl["source"]
	Expect(q).Should(HaveNameCount(m))

	m["params"] = append(make([]interface{}, 0), ont, "with")
	m["count"] = tbl["with"]
	Expect(q).Should(HaveNameCount(m))
	Expect("SELECT COUNT(*) FROM feature_cvterm_pub").Should(HaveCount(tbl["pub"]))
}
