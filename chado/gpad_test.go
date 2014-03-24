package chado

import (
    "bytes"
    "encoding/gob"
    "github.com/GeertJohan/go.rice"
    "github.com/dictybase/gochado"
    "github.com/dictybase/gochado/staging"
    "github.com/dictybase/testchado"
    . "github.com/dictybase/testchado/matchers"
    . "github.com/onsi/gomega"
    "testing"
)

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
    //Teardown
    defer chado.DropSchema()
    //Setup
    b := rice.MustFindBox("../data")
    LoadGpadStagingSqlite(chado, t, b)
    LoadGpadChadoFixtureSqlite(chado, t, b)

    str, err := b.String("sqlite_gpad.ini")
    if err != nil {
        t.Errorf("could not open file sqlite_gpad.ini from rice box error:%s", err)
    }
    p := gochado.NewSqlParserFromString(str)
    dbh := chado.DBHandle()
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
    dbh.Execf(p.GetSection("insert_latest_goa_from_staging"), grecord)
    Expect("SELECT COUNT(*) FROM temp_gpad_new").Should(HaveCount(10))
    dbh.Execf(p.GetSection("insert_feature_cvterm"))
    Expect("SELECT COUNT(*) FROM feature_cvterm").Should(HaveCount(10))
}
