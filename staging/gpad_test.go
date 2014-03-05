package staging

import (
    "bytes"
    "github.com/GeertJohan/go.rice"
    "github.com/dictybase/gochado"
    "github.com/dictybase/testchado"
    . "github.com/onsi/gomega"
    "testing"
)

func TestSqlite(t *testing.T) {
    RegisterTestingT(t)
    chado := testchado.NewSQLiteManager()
    chado.DeploySchema()
    defer chado.DropSchema()

    dbh := chado.DBHandle()
    r, err := rice.FindBox("../data")
    if err != nil {
        t.Errorf("could not open rice box error: %s", err)
    }
    str, err := r.String("sqlite_gpad.ini")
    if err != nil {
        t.Errorf("could not open file sqlite_gpad.ini from rice box error:%s", err)
    }
    staging := NewSqlite(dbh, gochado.NewSqlParserFromString(str))
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
        t.Errorf("should have %d data row under %s key", 9, "gpad")
    }
    if staging.buckets["gpad_reference"].Count() != 1 {
        t.Errorf("got %d data row expected %d under %s key", staging.buckets["gpad_reference"].Count(), 1, "gpad_reference")
    }
}
