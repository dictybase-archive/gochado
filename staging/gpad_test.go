package staging

import (
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
    for sec := range staging.buckets {
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
}
