package staging

import (
	"bytes"
	"log"
	"reflect"
	"testing"

	"github.com/GeertJohan/go.rice"
	"github.com/dictybase/gochado"
	"github.com/dictybase/testchado"
	. "github.com/onsi/gomega"
	"gopkg.in/dictybase/testchado.v1"
)

type stagingTest struct {
	rice    *rice.Box
	staging *Sqlite
	chado   *testchado.Sqlite
}

func SetUpSqliteTest() *stagingTest {
	chado := testchado.NewSQLiteManager()
	chado.DeploySchema()
	r, err := rice.FindBox("../data")
	if err != nil {
		log.Fatalf("could not open rice box error: %s", err)
	}
	str, err := r.String("sqlite_gpad.ini")
	if err != nil {
		log.Fatalf("could not open file sqlite_gpad.ini from rice box error:%s", err)
	}
	staging := NewStagingSqlite(chado.DBHandle(), gochado.NewSqlParserFromString(str))
	staging.CreateTables()
	return &stagingTest{
		rice:    r,
		staging: staging,
		chado:   chado,
	}
}

func SetUpSqliteBulkTest() *stagingTest {
	st := SetUpSqliteTest()
	gpstr, err := st.rice.String("test.gpad")
	if err != nil {
		log.Fatal(err)
	}
	buff := bytes.NewBufferString(gpstr)
	for {
		line, err := buff.ReadString('\n')
		if err != nil {
			break
		}
		st.staging.AddDataRow(line)
	}
	return st
}

func TestGpadStagingSqliteTblBuffer(t *testing.T) {
	// Set up for testing
	RegisterTestingT(t)
	st := SetUpSqliteTest()
	staging := st.staging
	r := st.rice
	dbh := st.chado.DBHandle()
	defer st.chado.DropSchema()

	// test struct creation and table handling
	ln := len(staging.sections)
	if ln != 5 {
		t.Errorf("Expecting 5 entries got %d", ln)
	}
	for _, sec := range staging.tables {
		row := dbh.QueryRowx("SELECT name FROM sqlite_temp_master WHERE type = 'table' AND name = $1", sec)
		var tbl string
		err := row.Scan(&tbl)
		if err != nil {
			t.Errorf("Could not retrieve temp table %s: %s", sec, err)
		}
		if tbl != sec {
			t.Errorf("should have retrieved table %s", sec)
		}
	}

	// test data buffering
	gpstr, err := r.String("test.gpad")
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
	if len(staging.buckets) != 5 {
		t.Errorf("should have 5 buckets got %d", len(staging.buckets))
	}
	for _, name := range []string{"gpad", "gpad_reference", "gpad_withfrom", "gpad_extension"} {
		if _, ok := staging.buckets[name]; !ok {
			t.Errorf("bucket %s do not exist", name)
		}
	}
	if staging.buckets["gpad"].Count() != 11 {
		t.Errorf("should have %d data row under %s key", 11, "gpad")
	}
	if staging.buckets["gpad_reference"].Count() != 1 {
		t.Errorf("got %d data row expected %d under %s key", staging.buckets["gpad_reference"].Count(), 1, "gpad_reference")
	}
	if staging.buckets["gpad_withfrom"].Count() != 5 {
		t.Errorf("got %d data row expected %d under %s key", staging.buckets["gpad_withfrom"].Count(), 5, "gpad_withfrom")
	}
}

func TestGpadStagingSqliteBulkCount(t *testing.T) {
	// Set up for testing
	RegisterTestingT(t)
	st := SetUpSqliteBulkTest()
	dbh := st.chado.DBHandle()
	defer st.chado.DropSchema()

	//bulkload testing
	st.staging.BulkLoad()
	type entries struct{ Counter int }
	e := entries{}
	err := dbh.Get(&e, "SELECT COUNT(*) counter FROM temp_gpad")
	if err != nil {
		t.Errorf("should have executed the query %s", err)
	}
	//test by matching total entries in tables
	if e.Counter != 11 {
		t.Errorf("expected %d got %d", 11, e.Counter)
	}
	err = dbh.Get(&e, "SELECT COUNT(*) counter FROM temp_gpad_reference")
	if err != nil {
		t.Errorf("should have executed the query %s", err)
	}
	if e.Counter != 1 {
		t.Errorf("expected %d got %d", 1, e.Counter)
	}
	err = dbh.Get(&e, "SELECT COUNT(*) counter FROM temp_gpad_withfrom")
	if err != nil {
		t.Errorf("should have executed the query %s", err)
	}
	if e.Counter != 5 {
		t.Errorf("expected %d got %d", 5, e.Counter)
	}
	err = dbh.Get(&e, "SELECT COUNT(*) counter FROM temp_gpad_extension")
	if err != nil {
		t.Errorf("should have executed the query %s", err)
	}
	if e.Counter != 1 {
		t.Errorf("expected %d got %d", 1, e.Counter)
	}
}

func TestGpadStagingSqliteBulkIndividual(t *testing.T) {
	// Set up for testing
	RegisterTestingT(t)
	st := SetUpSqliteBulkTest()
	chado := st.chado
	dbh := chado.DBHandle()
	defer chado.DropSchema()
	st.staging.BulkLoad()

	//test individual row
	type gpad struct {
		Qualifier string
		Pubid     string `db:"publication_id"`
		Pubplace  string `db:"pubplace"`
		Evidence  string `db:"evidence_code"`
		Assigned  string `db:"assigned_by"`
		Date      string `db:"date_curated"`
	}
	g := gpad{}
	err := dbh.Get(&g, "SELECT qualifier, publication_id, pubplace, evidence_code, assigned_by, date_curated FROM temp_gpad where id = ?", "DDB_G0272003")
	if err != nil {
		t.Errorf("should have executed the query %s", err)
	}
	el := reflect.ValueOf(&g).Elem()
	for k, v := range map[string]string{"Qualifier": "enables", "Pubid": "0000002", "Pubplace": "GO_REF", "Evidence": "0000256", "Assigned": "InterPro", "Date": "20140222"} {
		sv := el.FieldByName(k).String()
		if sv != v {
			t.Errorf("Expected %s Got %s\n", sv, v)
		}
	}

	type gdigest struct{ Digest string }
	type gref struct {
		Pubid    string `db:"publication_id"`
		Pubplace string `db:"pubplace"`
	}
	gd := gdigest{}
	err = dbh.Get(&gd, "SELECT digest FROM temp_gpad WHERE id = $1", "DDB_G0278727")
	if err != nil {
		t.Errorf("should have executed the query %s", err)
	}

	//gpad_reference
	gr := gref{}
	err = dbh.Get(&gr, "SELECT publication_id, pubplace FROM temp_gpad_reference WHERE digest = $1", gd.Digest)
	if err != nil {
		t.Errorf("should have executed the query %s", err)
	}
	if gr.Pubid != "0000033" {
		t.Errorf("expected %s got %s", "0000033", gr.Pubid)
	}
	if gr.Pubplace != "GO_REF" {
		t.Errorf("expected %s got %s", "GO_REF", gr.Pubplace)
	}

	// gpad_withfrom
	err = dbh.Get(&gd, "SELECT digest FROM temp_gpad WHERE id = $1 AND evidence_code = $2", "DDB_G0272004", "0000318")
	if err != nil {
		t.Errorf("should have executed the query %s", err)
	}
	type gwithfrom struct{ Withfrom string }
	gw := gwithfrom{}
	err = dbh.Get(&gw, "SELECT withfrom FROM temp_gpad_withfrom WHERE digest = $1", gd.Digest)
	if err != nil {
		t.Errorf("should have executed the query %s", err)
	}
	if gw.Withfrom != "PANTHER:PTN000012953" {
		t.Errorf("expected %s got %s", "PANTHER:PTN000012953", gw.Withfrom)
	}

	//gpad_extension
	q := `SELECT tgext.relationship, tgext.db, tgext.id FROM temp_gpad_extension
	tgext JOIN temp_gpad ON tgext.digest = temp_gpad.digest
	WHERE temp_gpad.id = $1
	`
	type gext struct {
		Relationship string
		Db           string
		Id           string
	}
	ge := gext{}
	err = dbh.Get(&ge, q, "DDB_G0286189")
	if err != nil {
		t.Errorf("should have executed the query %s", err)
	}
	if ge.Relationship != "exists_during" {
		t.Errorf("expected %s got %s", "exists", ge.Relationship)
	}
}
