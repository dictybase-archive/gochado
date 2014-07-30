package staging

import (
	"bytes"
	"log"
	"reflect"
	"testing"

	"github.com/GeertJohan/go.rice"
	"github.com/dictybase/gochado"
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
	st.staging.BulkLoad()
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
	Expect(staging.sections).Should(HaveLen(5))
	for _, sec := range staging.tables {
		row := dbh.QueryRowx("SELECT name FROM sqlite_temp_master WHERE type = 'table' AND name = $1", sec)
		var tbl string
		err := row.Scan(&tbl)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(tbl).Should(Equal(sec))
	}

	// test data buffering
	gpstr, err := r.String("test.gpad")
	Expect(err).ShouldNot(HaveOccurred())
	buff := bytes.NewBufferString(gpstr)
	for {
		line, err := buff.ReadString('\n')
		if err != nil {
			break
		}
		staging.AddDataRow(line)
	}
	Expect(staging.buckets).Should(HaveLen(5))
	for _, name := range []string{"gpad", "gpad_reference", "gpad_withfrom", "gpad_extension"} {
		Expect(staging.buckets).Should(HaveKey(name))
	}
	Expect(staging.buckets["gpad"].Count()).To(Equal(12))
	Expect(staging.buckets["gpad_reference"].Count()).To(Equal(1))
	Expect(staging.buckets["gpad_withfrom"].Count()).To(Equal(5))
	Expect(staging.buckets["gpad_extension"].Count()).To(Equal(3))
}

func TestGpadStagingSqliteBulkCount(t *testing.T) {
	// Set up for testing
	RegisterTestingT(t)
	st := SetUpSqliteBulkTest()
	dbh := st.chado.DBHandle()
	defer st.chado.DropSchema()

	type entries struct{ Counter int }
	e := entries{}
	err := dbh.Get(&e, "SELECT COUNT(*) counter FROM temp_gpad")
	Expect(err).ShouldNot(HaveOccurred())
	//test by matching total entries in tables
	Expect(e.Counter).Should(Equal(12))

	err = dbh.Get(&e, "SELECT COUNT(*) counter FROM temp_gpad_reference")
	Expect(err).ShouldNot(HaveOccurred())
	Expect(e.Counter).Should(Equal(1))

	err = dbh.Get(&e, "SELECT COUNT(*) counter FROM temp_gpad_withfrom")
	Expect(err).ShouldNot(HaveOccurred())
	Expect(e.Counter).Should(Equal(5))

	err = dbh.Get(&e, "SELECT COUNT(*) counter FROM temp_gpad_extension")
	Expect(err).ShouldNot(HaveOccurred())
	Expect(e.Counter).Should(Equal(3))
}

func TestGpadStagingSqliteBulkIndividual(t *testing.T) {
	// Set up for testing
	RegisterTestingT(t)
	st := SetUpSqliteBulkTest()
	chado := st.chado
	dbh := chado.DBHandle()
	defer chado.DropSchema()

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
	Expect(err).ShouldNot(HaveOccurred())
	el := reflect.ValueOf(&g).Elem()
	for k, v := range map[string]string{"Qualifier": "enables", "Pubid": "0000002", "Pubplace": "GO_REF", "Evidence": "0000256", "Assigned": "InterPro", "Date": "20140222"} {
		sv := el.FieldByName(k).String()
		Expect(sv).Should(Equal(v))
	}

	type gdigest struct{ Digest string }
	type gref struct {
		Pubid    string `db:"publication_id"`
		Pubplace string `db:"pubplace"`
	}
	gd := gdigest{}
	err = dbh.Get(&gd, "SELECT digest FROM temp_gpad WHERE id = $1", "DDB_G0278727")
	Expect(err).ShouldNot(HaveOccurred())

	//gpad_reference
	gr := gref{}
	err = dbh.Get(&gr, "SELECT publication_id, pubplace FROM temp_gpad_reference WHERE digest = $1", gd.Digest)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(gr.Pubid).Should(Equal("0000033"))
	Expect(gr.Pubplace).Should(Equal("GO_REF"))

	// gpad_withfrom
	err = dbh.Get(&gd, "SELECT digest FROM temp_gpad WHERE id = $1 AND evidence_code = $2", "DDB_G0272004", "0000318")
	Expect(err).ShouldNot(HaveOccurred())
	type gwithfrom struct{ Withfrom string }
	gw := gwithfrom{}
	err = dbh.Get(&gw, "SELECT withfrom FROM temp_gpad_withfrom WHERE digest = $1", gd.Digest)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(gw.Withfrom).Should(Equal("PANTHER:PTN000012953"))

	//gpad_extension
	q := `SELECT tgext.relationship, tgext.db, tgext.id FROM temp_gpad_extension
	tgext JOIN temp_gpad ON tgext.digest = temp_gpad.digest
	WHERE temp_gpad.id = $1
	`
	type gext struct {
		Relationship string
		Db           string
		Id           string
		Digest       string
	}
	ge := gext{}
	err = dbh.Get(&ge, q, "DDB_G0286189")
	Expect(err).ShouldNot(HaveOccurred())
	Expect(ge.Relationship).Should(Equal("exists_during"))
	Expect(ge.Db).Should(Equal("GO"))

	q2 := `SELECT tgext.relationship, tgext.db, tgext.id FROM temp_gpad_extension
	tgext JOIN temp_gpad ON tgext.digest = temp_gpad.digest
	WHERE temp_gpad.id = $1 AND tgext.db = $2
	`
	err = dbh.Get(&ge, q2, "DDB_G0285321", "UniProtKB")
	Expect(err).ShouldNot(HaveOccurred())
	Expect(ge.Relationship).Should(Equal("has_regulation_target"))
	Expect(ge.Id).Should(Equal("Q54BD4"))
	err = dbh.Get(&ge, q2, "DDB_G0285321", "CHEBI")
	Expect(err).ShouldNot(HaveOccurred())
	Expect(ge.Relationship).Should(Equal("in_presence_of"))
	Expect(ge.Id).Should(Equal("64672"))
}
