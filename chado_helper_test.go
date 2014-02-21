package gochado

import (
    "github.com/dictybase/testchado"
    . "github.com/dictybase/testchado/matchers"
    . "github.com/onsi/gomega"
    "testing"
)

func TestFindOrCreateDbId(t *testing.T) {
    RegisterTestingT(t)
    chado := testchado.NewDBManager()
    chado.DeploySchema()
    _ = chado.LoadDefaultFixture()
    defer chado.DropSchema()

    helper := NewChadoHelper(chado.DBHandle())
    dbid, err := helper.FindOrCreateDbId("gochado")
    if err != nil {
        t.Error(err)
    }
    dbid2, err2 := helper.FindOrCreateDbId("gochado")
    if err2 != nil {
        t.Error(err2)
    }
    if dbid != dbid2 {
        t.Error("expected %d got %d", dbid, dbid2)
    }
}

func TestFindOrCreateCvId(t *testing.T) {
    RegisterTestingT(t)
    chado := testchado.NewDBManager()
    chado.DeploySchema()
    _ = chado.LoadDefaultFixture()
    defer chado.DropSchema()

    helper := NewChadoHelper(chado.DBHandle())
    cvid, err := helper.FindOrCreateCvId("sequence")
    if err != nil {
        t.Errorf("should have found cv sequence error: %s", err)
    }
    if cvid != 3 {
        t.Error("should have matched the cv id")
    }

    cvid2, err2 := helper.FindOrCreateCvId("tc")
    if err2 != nil {
        t.Errorf("should have created cv tc error: %s", err2)
    }
    if cvid2 != 4 {
        t.Errorf("Expected cvid 4 got %d", cvid2)
    }
}

func TestFindOrCreateCvtermId(t *testing.T) {
    RegisterTestingT(t)
    chado := testchado.NewDBManager()
    chado.DeploySchema()
    _ = chado.LoadDefaultFixture()
    defer chado.DropSchema()

    helper := NewChadoHelper(chado.DBHandle())
    id, err := helper.FindCvtermId("sequence", "gene")
    if err != nil {
        t.Errorf("should have found cvterm id: %s", err)
    }

    id2, err2 := helper.FindCvtermId("sequence", "gene")
    if id != id2 {
        t.Errorf("first id %d and second id %d should have matched: %s", id, id2, err2)
    }

    p := make(map[string]string)
    p["cv"] = "gochado"
    p["cvterm"] = "gochadoterm"
    p["dbxref"] = "GC:59939"
    _, err = helper.CreateCvtermId(p)
    if err != nil {
        t.Error(err)
    }
    Expect(chado).Should(HaveCvterm(p["cvterm"]))
    Expect(chado).Should(HaveDbxref(p["dbxref"]))

    p["cv"] = "seinfeld"
    p["cvterm"] = "The chinese restaurant"
    p["dbxref"] = "Todd Gag"
    _, err = helper.CreateCvtermId(p)
    if err != nil {
        t.Error(err)
    }
    Expect(chado).Should(HaveDbxref("internal:Todd Gag"))
}

func TestNormalizeId(t *testing.T) {
    chado := testchado.NewDBManager()
    chado.DeploySchema()
    helper := NewChadoHelper(chado.DBHandle())
    dbid, acc, err := helper.NormaLizeId("GC:53843934")
    if err != nil {
        t.Error(err)
    }
    if acc != "53843934" {
        t.Errorf("expected %s Got %s", "53843934", acc)
    }
    dbid2, _ := helper.FindOrCreateDbId("GC")
    if dbid2 != dbid {
        t.Errorf("Expected %d Got %d", dbid, dbid2)
    }
    dbid, _, err = helper.NormaLizeId("DDB8594")
    if err != nil {
        t.Error(err)
    }
    dbid2, _ = helper.FindOrCreateDbId("internal")
    if dbid != dbid2 {
        t.Errorf("Expected %d Got %d", dbid, dbid2)
    }
}
