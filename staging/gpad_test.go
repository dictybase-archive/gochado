package staging

import (
    "bytes"
    "github.com/GeertJohan/go.rice"
    "github.com/dictybase/gochado"
    "github.com/dictybase/testchado"
    . "github.com/onsi/gomega"
    "reflect"
    "testing"
)

func TestSqlite(t *testing.T) {
    RegisterTestingT(t)
    chado := testchado.NewSQLiteManager()
    chado.DeploySchema()
    defer chado.DropSchema()

    dbh := chado.DBHandle()

    // test struct creation and table handling
    r, err := rice.FindBox("../data")
    if err != nil {
        t.Errorf("could not open rice box error: %s", err)
    }
    str, err := r.String("sqlite_gpad.ini")
    if err != nil {
        t.Errorf("could not open file sqlite_gpad.ini from rice box error:%s", err)
    }
    staging := NewStagingSqlite(dbh, gochado.NewSqlParserFromString(str))
    ln := len(staging.sections)
    if ln != 3 {
        t.Errorf("Expecting 3 entries got %d", ln)
    }
    staging.CreateTables()
    for _, sec := range staging.tables {
        row := dbh.QueryRowx("SELECT name FROM sqlite_temp_master WHERE type = 'table' AND name = $1", sec)
        var tbl string
        err = row.Scan(&tbl)
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
    if len(staging.buckets) != 3 {
        t.Errorf("should have three buckets got %d", len(staging.buckets))
    }
    for _, name := range []string{"gpad", "gpad_reference", "gpad_withfrom"} {
        if _, ok := staging.buckets[name]; !ok {
            t.Errorf("bucket %s do not exist", name)
        }
    }
    if staging.buckets["gpad"].Count() != 10 {
        t.Errorf("should have %d data row under %s key", 10, "gpad")
    }
    if staging.buckets["gpad_reference"].Count() != 1 {
        t.Errorf("got %d data row expected %d under %s key", staging.buckets["gpad_reference"].Count(), 1, "gpad_reference")
    }
    if staging.buckets["gpad_withfrom"].Count() != 5 {
        t.Errorf("got %d data row expected %d under %s key", staging.buckets["gpad_withfrom"].Count(), 5, "gpad_withfrom")
    }

    //bulkload testing
    staging.BulkLoad()
    type entries struct{ Counter int }
    e := entries{}
    err = dbh.Get(&e, "SELECT COUNT(*) counter FROM temp_gpad")
    if err != nil {
        t.Errorf("should have executed the query %s", err)
    }
    //test by matching total entries in tables
    if e.Counter != 10 {
        t.Errorf("expected %d got %d", 10, e.Counter)
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
    err = dbh.Get(&g, "SELECT qualifier, publication_id, pubplace, evidence_code, assigned_by, date_curated FROM temp_gpad where id = ?", "DDB_G0272003")
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
}
