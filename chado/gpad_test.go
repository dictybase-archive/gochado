package chado

import (
	"bytes"
	"database/sql"
	"encoding/gob"
	"log"
	"strconv"
	"testing"

	"github.com/GeertJohan/go.rice"
	"github.com/dictybase/gochado"
	"github.com/dictybase/gochado/staging"
	"github.com/dictybase/testchado"
	. "github.com/dictybase/testchado/matchers"
	"github.com/jmoiron/sqlx"
	. "github.com/onsi/gomega"
)

type chadoTest struct {
	rice   *rice.Box
	chado  *testchado.Sqlite
	parser *gochado.SqlParser
}

// Load fixtures for testing GPAD loading. It loads the following fixtures
//  Gene ids
//	Reference/Publications
//	Ontology terms/ids
//	Various ontology terms under gene_ontology_association namespace
//  Cv and db namespace for anonymous cvterms
func LoadGpadChadoFixtureSqlite(chado testchado.DBManager, b *rice.Box) {
	//get the gob file
	r, err := b.Open("fixture.gob")
	defer r.Close()
	if err != nil {
		log.Fatal("Could not get gob file fixture.gob")

	}
	// Now decode and get the data
	dec := gob.NewDecoder(r)
	var genes []string
	err = dec.Decode(&genes)
	if err != nil {
		log.Fatal(err)
	}
	f := gochado.NewGpadFixtureLoader(chado)
	// Gene ids
	_ = f.LoadGenes(genes)

	var gorefs []string
	err = dec.Decode(&gorefs)
	if err != nil {
		log.Fatal(err)
	}
	// Pub ids
	_ = f.LoadPubIds(gorefs)

	var goids map[string][]string
	err = dec.Decode(&goids)
	if err != nil {
		log.Fatal(err)
	}
	// Load GO ids
	_ = f.LoadGoIds(goids)
	_ = f.LoadMiscCvterms("gene_ontology_association")

	// Cv and db namespace for anonymous cvterms
	f.LoadAnonNamespaces()
	// Extension cvterms
	var cvtslice []map[string]string
	err = dec.Decode(&cvtslice)
	if err != nil {
		log.Fatal(err)
	}
	_ = f.LoadCvterms(cvtslice)

	// Dbxrefs
	var xrefs []string
	err = dec.Decode(&xrefs)
	if err != nil {
		log.Fatalf("decoding err in xref %s", err)
	}
	_ = f.LoadDbxrefs(xrefs)

	// Relationship cvterms
	var rslice []map[string]string
	err = dec.Decode(&rslice)
	if err != nil {
		log.Fatalf("unable to decode %s\n", err)
	}
	_ = f.LoadCvterms(rslice)
}

// Loads GPAD test file to staging tables
func LoadGpadStagingSqlite(chado testchado.DBManager, b *rice.Box, sql string) {
	// test struct creation and table handling
	staging := staging.NewStagingSqlite(chado.DBHandle(), gochado.NewSqlParserFromString(sql))
	staging.CreateTables()

	// test data buffering
	gpstr, err := b.String("test.gpad")
	if err != nil {
		log.Fatal(err)
	}
	buff := bytes.NewBufferString(gpstr)
	for {
		line, err := buff.ReadString('\n')
		if err != nil {
			break
		}
		staging.AddDataRow(line)
	}
	//bulkload testing
	staging.BulkLoad()
}

func setUpSqliteTest() *chadoTest {
	chado := testchado.NewSQLiteManager()
	RegisterDBHandler(chado)
	chado.DeploySchema()
	chado.LoadPresetFixture("eco")
	b := rice.MustFindBox("../data")
	str, err := b.String("sqlite_gpad.ini")
	if err != nil {
		log.Fatalf("could not open file sqlite_gpad.ini from rice box error:%s", err)
	}
	// Loads test gpad file in staging
	LoadGpadStagingSqlite(chado, b, str)
	// Loads fixtures needed for testing in chado
	LoadGpadChadoFixtureSqlite(chado, b)

	return &chadoTest{
		rice:   b,
		chado:  chado,
		parser: gochado.NewSqlParserFromString(str),
	}
}

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
	//Teardown
	defer chado.DropSchema()

	sqlite := NewChadoSqlite(dbh, setup.parser, ont)
	sqlite.BulkLoad()
	Expect("SELECT COUNT(*) FROM temp_gpad_new").Should(HaveCount(12))
	Expect("SELECT COUNT(*) FROM feature_cvterm").Should(HaveCount(10))
	eq := `
SELECT COUNT(*)  FROM feature_cvtermprop
JOIN cvterm ON cvterm.cvterm_id = feature_cvtermprop.type_id
JOIN cv ON cv.cv_id = cvterm.cv_id
WHERE cv.name = "eco"
`
	Expect(eq).Should(HaveCount(10))

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
	m["count"] = 10
	Expect(q).Should(HaveNameCount(m))

	m["params"] = append(make([]interface{}, 0), ont, "date")
	Expect(q).Should(HaveNameCount(m))

	m["params"] = append(make([]interface{}, 0), ont, "source")
	Expect(q).Should(HaveNameCount(m))

	m["params"] = append(make([]interface{}, 0), ont, "with")
	m["count"] = 6
	Expect(q).Should(HaveNameCount(m))
	Expect("SELECT COUNT(*) FROM feature_cvterm_pub").Should(HaveCount(1))
}

func TestAnonCvtChadoSqlite(t *testing.T) {
	RegisterTestingT(t)
	//Setup
	setup := setUpSqliteTest()
	chado := setup.chado
	p := setup.parser
	ont := "gene_ontology_association"
	acv := "annotation extension terms"
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
		q := p.GetSection("update_temp_with_anon_cvterm")
		_, err := dbh.Exec(q, a.Name, a.Digest, a.Id, a.Db, a.Relationship)
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
	_, err = dbh.Exec(p.GetSection("insert_anon_cvterm_rel_original"), "ro", acv)
	Expect(err).ShouldNot(HaveOccurred())
	_, err = dbh.Exec(p.GetSection("insert_anon_cvterm_rel_extension"), acv)
	Expect(err).ShouldNot(HaveOccurred())
	runAnonCvtRelationShip(dbh, "exists_during", 1)
	runAnonCvtRelationShip(dbh, "has_regulation_target", 0)
	runAnonCvtRelationShip(dbh, "in_presence_of", 0)

	// insert anon cvtermprop for database/sequence identifies
	res, err := dbh.Exec(p.GetSection("insert_anon_cvtermprop_extension"), acv)
	Expect(err).ShouldNot(HaveOccurred())
	rc, err := res.RowsAffected()
	Expect(err).ShouldNot(HaveOccurred())
	Expect(int(rc)).Should(Equal(2))
	runAnonCvprop(dbh)

	//insert all gpad entries expect the extensions
	doRegularGpadInserts(dbh, p, ont)
	// insert new feature_cvterm with anon terms
	runAnonCvtInserts(dbh, p, acv)
	// insert implicit and explicit columns
	runAnonCvtImplExplInserts(dbh, p, acv)
}

func runAnonCvtImplExplInserts(dbh *sqlx.DB, p *gochado.SqlParser, acv string) {
	//ont := "gene_ontology_association"
	tbl := [][]string{
		[]string{"insert_feature_cvtermprop_evcode", acv, "", "2"},
		//[]string{"insert_anon_feature_cvtermprop_qualifier", ont, acv, "2"},
		//[]string{"insert_feature_cvtermprop_date", ont, acv, "2"},
		//[]string{"insert_feature_cvtermprop_withfrom", ont, acv, "2"},
		//[]string{"insert_feature_cvtermprop_assigned_by", ont, acv, "2"},
		//[]string{"insert_feature_cvterm_pub_reference", acv, "", "2"},
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
	dbh.MustExec(p.GetSection("insert_feature_cvtermprop_evcode"))
	// Extra references
	dbh.MustExec(p.GetSection("insert_feature_cvterm_pub_reference"))
	sections := []string{
		"feature_cvtermprop_qualifier",
		"feature_cvtermprop_date",
		"feature_cvtermprop_assigned_by",
		"feature_cvtermprop_withfrom",
	}
	for _, s := range sections {
		s = "insert_" + s
		dbh.MustExec(p.GetSection(s)+";", ont)
	}
}

func runAnonCvprop(dbh *sqlx.DB) {
	q1 := `
	SELECT cvtermprop.value FROM cvtermprop
	JOIN cvterm ON cvterm.cvterm_id = cvtermprop.type_id
	JOIN cv ON cv.cv_id = cvterm.cv_id
	WHERE cvterm.name = $1
	AND cv.name = 'go/extensions/gorel'
	`
	var val string
	err := dbh.QueryRowx(q1, "has_regulation_target").Scan(&val)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(val).Should(Equal("UniProtKB:Q54BD4"))

	var val2 string
	err = dbh.QueryRowx(q1, "in_presence_of").Scan(&val2)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(val2).Should(Equal("CHEBI:64672"))
}

func runAnonCvtRelationShip(dbh *sqlx.DB, rel string, expected int) {

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
	acv := "annotation extension terms"
	var aterm string
	err := dbh.QueryRowx(q1, rel).Scan(&aterm)
	Expect(err).ShouldNot(HaveOccurred())
	// is_a relationship with original go term
	var c1 int
	err = dbh.QueryRowx(q2, aterm, acv, "is_a").Scan(&c1)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(c1).Should(Equal(1))
	// relationship with extension identifier
	var c2 int
	err = dbh.QueryRowx(q2, aterm, acv, rel).Scan(&c2)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(c2).Should(Equal(expected))
}
