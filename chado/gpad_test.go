package chado

import (
	"bytes"
	"encoding/gob"
	"log"
	"os"
	"testing"

	"github.com/GeertJohan/go.rice"
	"github.com/dictybase/gochado"
	"github.com/dictybase/gochado/staging"
	"github.com/dictybase/testchado"
	. "github.com/dictybase/testchado/matchers"
	"github.com/jmoiron/sqlx"
	"github.com/olekukonko/tablewriter"
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
	_ = f.LoadExtnCvterms(cvtslice)
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

func printPubTable(dbh *sqlx.DB) {
	type pubtable struct {
		Id         string `db:"pub_id"`
		Place      string `db:"pubplace"`
		Uniquename string
	}
	pt := []pubtable{}
	err := dbh.Select(&pt, "SELECT uniquename, pub_id, pubplace FROM pub")
	if err != nil {
		log.Fatal(err)
	}
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Pubid", "Uniquename", "Pubplace"})
	for _, rec := range pt {
		table.Append([]string{rec.Id, rec.Uniquename, rec.Place})
	}
	table.Render()
}
