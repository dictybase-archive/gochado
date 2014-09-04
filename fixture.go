package gochado

import (
	"log"
	"strings"

	"github.com/jinzhu/gorm"
	"gopkg.in/dictybase/testchado.v1"
)

type GpadFixtureLoader struct {
	gorm   *gorm.DB
	helper *ChadoHelper
}

func NewGpadFixtureLoader(tc testchado.DBManager) *GpadFixtureLoader {
	gorm := tc.GormHandle()
	gorm.LogMode(false)
	return &GpadFixtureLoader{gorm: gorm, helper: NewChadoHelper(tc.DBHandle())}
}

func (f *GpadFixtureLoader) LoadGenes(genes []string) []Feature {
	gorm := f.gorm
	var cvterm Cvterm
	gorm.Where("name = ?", "gene").First(&cvterm)
	var org Organism
	gorm.Where("genus = ? AND species = ?", "Dictyostelium", "discoiduem").First(&org)

	features := make([]Feature, 0)
	for _, n := range genes {
		f := Feature{Uniquename: n, OrganismId: org.OrganismId, TypeId: cvterm.CvtermId}
		gorm.Save(&f)
		features = append(features, f)
	}
	return features
}

func (f *GpadFixtureLoader) LoadGoIds(ids map[string][]string) []Cvterm {
	gorm := f.gorm
	var db Db
	gorm.Where(&Db{Name: "GO"}).FirstOrInit(&db)
	if gorm.NewRecord(db) {
		gorm.Save(&db)
	}
	terms := make([]Cvterm, 0)
	for id, info := range ids {
		_, xref, err := f.helper.NormaLizeId(id)
		if err != nil {
			log.Fatal(err)
		}
		var cv Cv
		gorm.Where(&Cv{Name: info[0]}).FirstOrInit(&cv)
		if gorm.NewRecord(cv) {
			gorm.Save(&cv)
		}
		cvterm := Cvterm{
			Name:   info[1],
			CvId:   cv.CvId,
			Dbxref: Dbxref{Accession: xref, DbId: db.DbId},
		}
		gorm.Save(&cvterm)
		terms = append(terms, cvterm)
	}
	return terms
}

func (f *GpadFixtureLoader) LoadPubIds(ids []string) []Pub {
	gorm := f.gorm
	h := f.helper
	params := map[string]string{
		"cv":     "Pub",
		"cvterm": "publication",
		"dbxref": "publication",
	}
	tid, err := h.CreateCvtermId(params)
	if err != nil {
		log.Fatal(err)
	}
	var pid string
	pubplace := "GPAD"
	pubs := make([]Pub, 0)
	for _, id := range ids {
		pid = id
		if strings.Contains(id, ":") {
			out := strings.SplitN(id, ":", 2)
			pubplace = out[0]
			if pubplace == "PMID" {
				pubplace = "PubMed"
			}
			pid = out[1]
		}
		p := Pub{Uniquename: pid, Pubplace: pubplace, TypeId: int64(tid)}
		gorm.Save(&p)
		pubs = append(pubs, p)
	}
	return pubs
}

func (f *GpadFixtureLoader) LoadMiscCvterms(cv string) []Cvterm {
	h := f.helper
	gorm := f.gorm
	cvterms := make([]Cvterm, 0)
	for _, cvterm := range []string{"date", "source", "with", "qualifier"} {
		id, err := h.CreateCvtermId(map[string]string{
			"cv":     cv,
			"cvterm": cvterm,
			"dbxref": cvterm,
		})
		if err != nil {
			log.Fatal(err)
		}
		var t Cvterm
		gorm.Where("cvterm_id = ?", id).First(&t)
		cvterms = append(cvterms, t)
	}
	return cvterms
}
