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

// Load fixtures for testing GPAD loading. It loads the following fixtures
//  Gene ids
//	Reference/Publications
//	Ontology terms/ids
//	Various ontology terms under gene_ontology_association namespace
func LoadGpadChadoFixtureSqlite(chado testchado.DBManager, t *testing.T, b *rice.Box) {
	//get the gob file
	r, err := b.Open("fixture.gob")
	defer r.Close()
	if err != nil {
		t.Error("Could not get gob file fixture.gob")
	}
	// Now decode and get the data
	dec := gob.NewDecoder(r)
	var genes []string
	err = dec.Decode(&genes)
	if err != nil {
		t.Error(err)
	}
	f := gochado.NewGpadFixtureLoader(chado)
	_ = f.LoadGenes(genes)

	var gorefs []string
	err = dec.Decode(&gorefs)
	if err != nil {
		t.Error(err)
	}
	_ = f.LoadPubIds(gorefs)

	var goids map[string][]string
	err = dec.Decode(&goids)
	if err != nil {
		t.Error(err)
	}
	_ = f.LoadGoIds(goids)
	_ = f.LoadMiscCvterms("gene_ontology_association")
}

// Loads GPAD test file to staging tables
func LoadGpadStagingSqlite(chado testchado.DBManager, t *testing.T, b *rice.Box) {
	// test struct creation and table handling
	str, err := b.String("sqlite_gpad.ini")
	if err != nil {
		t.Errorf("could not open file sqlite_gpad.ini from rice box error:%s", err)
	}
	staging := staging.NewStagingSqlite(chado.DBHandle(), gochado.NewSqlParserFromString(str))
	staging.CreateTables()

	// test data buffering
	gpstr, err := b.String("test.gpad")
	if err != nil {
		t.Error(err)
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

func TestGpadChadoSqlite(t *testing.T) {
	RegisterTestingT(t)
	chado := testchado.NewSQLiteManager()
	RegisterDBHandler(chado)
	chado.DeploySchema()
	chado.LoadPresetFixture("eco")
	//Setup
	b := rice.MustFindBox("../data")
	LoadGpadStagingSqlite(chado, t, b)
	LoadGpadChadoFixtureSqlite(chado, t, b)

	//Teardown
	defer chado.DropSchema()

	dbh := chado.DBHandle()
	str, err := b.String("sqlite_gpad.ini")
	if err != nil {
		t.Errorf("could not open file sqlite_gpad.ini from rice box error:%s", err)
	}
	p := gochado.NewSqlParserFromString(str)
	type entries struct{ Counter int }
	e := entries{}
	err = dbh.Get(&e, p.GetSection("select_latest_goa_count_chado"), "Dictyostelium", "disocideum")
	if err != nil {
		t.Errorf("should have run the query error: %s", err)
	}

	grecord := 0
	if e.Counter > 0 {
		type lt struct{ Latest int }
		l := lt{}
		err = dbh.Get(&l, p.GetSection("select_latest_goa_bydate_chado"), "Dictyostelium", "disocideum")
		if err != nil {
			t.Errorf("should have run the query error: %s", err)
		}
		grecord = l.Latest
	}

	dbh.MustExec(p.GetSection("insert_latest_goa_from_staging"), grecord)
	Expect("SELECT COUNT(*) FROM temp_gpad_new").Should(HaveCount(12))
	// Check if goid is present in dbxref
	type gpad struct {
		Id       string
		Goid     string
		Pubid    string `db:"publication_id"`
		Pubplace string
		Evcode   string `db:"evidence_code"`
	}
	g := []gpad{}
	err = dbh.Select(&g, "SELECT id, goid, publication_id, pubplace, evidence_code FROM temp_gpad_new")
	if err != nil {
		t.Errorf("error in fetching rows from temp_gpad_new %s\n", err)
	}
	type xref struct {
		Id string `db:"dbxref_id"`
	}
	type feat struct {
		Id string `db:"feature_id"`
	}
	type pub struct {
		Id string `db:"pub_id"`
	}
	xr := xref{}
	f := feat{}
	pb := pub{}
	dbquery := `
        SELECT dbxref.dbxref_id FROM dbxref
        JOIN db ON db.db_id = dbxref.db_id
        JOIN cvterm ON dbxref.dbxref_id = cvterm.dbxref_id
        JOIN cv ON cv.cv_id = cvterm.cv_id
        WHERE dbxref.accession = $1
        AND db.name = $2
        AND cv.name IN("biological_process", "molecular_function", "cellular_component")
    `
	evquery := `
        SELECT dbxref.dbxref_id FROM dbxref
        JOIN db ON db.db_id = dbxref.db_id
        JOIN cvterm ON dbxref.dbxref_id = cvterm.dbxref_id
        JOIN cv ON cv.cv_id = cvterm.cv_id
        WHERE dbxref.accession = $1
        AND db.name = $2
        AND cv.name = "eco"
    `
	// make sure all dbxrefs, db, cv, cvterms and publication records are present
	for _, r := range g {
		err := dbh.Get(&xr, dbquery, r.Goid, "GO")
		if err != nil {
			t.Errorf("unable to fetch row for dbxref id %s error: %s", r.Goid, err)
		}
		err = dbh.Get(&f, "SELECT feature_id FROM feature WHERE uniquename = $1", r.Id)
		if err != nil {
			t.Errorf("unable to fetch row for feature id %s error: %s", r.Id, err)
		}
		err = dbh.Get(&pb, "SELECT pub_id FROM pub WHERE uniquename = $1 AND pubplace = $2", r.Pubid, r.Pubplace)
		if err != nil {
			t.Errorf("unable to fetch row for pubplace:%s and publication id:%s error: %s", r.Pubplace, r.Pubid, err)
		}
		err = dbh.Get(&xr, evquery, r.Evcode, "ECO")
		if err != nil {
			t.Errorf("unable to fetch row for eco dbxref id %s error: %s", r.Evcode, err)
		}
	}

	dbh.MustExec(p.GetSection("insert_feature_cvterm"))
	Expect("SELECT COUNT(*) FROM feature_cvterm").Should(HaveCount(12))
	dbh.MustExec(p.GetSection("insert_feature_cvtermprop_evcode"))
	Expect("SELECT COUNT(*) FROM feature_cvtermprop").Should(HaveCount(12))

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
	m["count"] = 12
	dbh.MustExec(p.GetSection("insert_feature_cvtermprop_qualifier"))
	Expect(q).Should(HaveNameCount(m))

	m["params"] = append(make([]interface{}, 0), "date")
	dbh.MustExec(p.GetSection("insert_feature_cvtermprop_date"))
	Expect(q).Should(HaveNameCount(m))

	m["params"] = append(make([]interface{}, 0), "source")
	dbh.MustExec(p.GetSection("insert_feature_cvtermprop_assigned_by"))
	Expect(q).Should(HaveNameCount(m))

	m["params"] = append(make([]interface{}, 0), "with")
	m["count"] = 5
	dbh.MustExec(p.GetSection("insert_feature_cvtermprop_withfrom"))
	Expect(q).Should(HaveNameCount(m))

	dbh.MustExec(p.GetSection("insert_feature_cvterm_pub_reference"))
	Expect("SELECT COUNT(*) FROM feature_cvterm_pub").Should(HaveCount(1))
}

func TestGpadChadoSqliteBulk(t *testing.T) {
	RegisterTestingT(t)
	chado := testchado.NewSQLiteManager()
	RegisterDBHandler(chado)
	chado.DeploySchema()
	chado.LoadPresetFixture("eco")
	//Setup
	b := rice.MustFindBox("../data")
	LoadGpadStagingSqlite(chado, t, b)
	LoadGpadChadoFixtureSqlite(chado, t, b)
	//Teardown
	defer chado.DropSchema()

	dbh := chado.DBHandle()
	str, err := b.String("sqlite_gpad.ini")
	if err != nil {
		t.Errorf("could not open file sqlite_gpad.ini from rice box error:%s", err)
	}
	p := gochado.NewSqlParserFromString(str)
	sqlite := NewChadoSqlite(dbh, p, &gochado.Organism{Genus: "Dictyostelium", Species: "discoideum"})
	sqlite.BulkLoad()
	Expect("SELECT COUNT(*) FROM temp_gpad_new").Should(HaveCount(12))
	Expect("SELECT COUNT(*) FROM feature_cvterm").Should(HaveCount(12))
	eq := `
    SELECT COUNT(*)  FROM feature_cvtermprop
    JOIN cvterm ON cvterm.cvterm_id = feature_cvtermprop.type_id
    JOIN cv ON cv.cv_id = cvterm.cv_id
    WHERE cv.name = "eco"
    `
	Expect(eq).Should(HaveCount(12))

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
	m["count"] = 12
	Expect(q).Should(HaveNameCount(m))

	m["params"] = append(make([]interface{}, 0), "date")
	Expect(q).Should(HaveNameCount(m))

	m["params"] = append(make([]interface{}, 0), "source")
	Expect(q).Should(HaveNameCount(m))

	m["params"] = append(make([]interface{}, 0), "with")
	m["count"] = 5
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
