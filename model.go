package gochado

import (
    "time"
)

type Dbxref struct {
    DbxrefId    int64 `primary_key:"dbxref_id"`
    Accession   string
    Version     string
    Description string
    //Foreign key
    DbId int64
    // Embedded struct belongs_to relation
    Db  Db
    //has_many relations
    FeatureDbxrefs []FeatureDbxref
}

type Db struct {
    DbId        int64 `primary_key:"db_id"`
    Name        string
    Description string
    Urlprefix   string
    Url         string
    // has_many relations
    Dbxrefs []Dbxref
}

type Cv struct {
    CvId       int64 `primary_key:"cv_id"`
    Name       string
    Definition string
    Cvterms    []Cvterm
}

type Cvterm struct {
    CvtermId           int64 `primary_key:"cvterm_id"`
    Name               string
    Definition         string
    IsObsolete         bool
    IsRelationshiptype bool

    //Foreign keys
    CvId     int64
    Cv       Cv
    DbxrefId int64
    Dbxref   Dbxref
}

type Organism struct {
    OrganismId   int64 `primary_key:"organism_id"`
    Abbreviation string
    Genus        string
    Species      string
    CommonName   string
    Comment      string
    Features     []Feature
}

type Feature struct {
    FeatureId        int64 `primary_key:"feature_id"`
    Name             string
    Uniquename       string
    Residues         string
    Seqlen           int64
    Md5checksum      string
    IsAnalysis       bool
    IsObsolete       bool
    Timeaccessioned  time.Time
    Timelastmodified time.Time

    //foreign keys
    OrganismId int64
    Organism   Organism
    DbxrefId   int64
    Dbxref     Dbxref
    TypeId     int64
    Type       Cvterm

    //has_many relations
    FeatureCvterms []FeatureCvterm
    FeatureDbxrefs []FeatureDbxref
}

type Pub struct {
    PubId       int64 `primary_key:"pub_id"`
    Title       string
    Volumttitle string
    Volume      string
    SeriesName  string
    Issue       string
    Pyear       string
    Pages       string
    Miniref     string
    Uniquename  string
    Publisher   string
    Pubplace    string
    IsObsolete  bool
    //foreign key
    TypeId int64
    Type   Cvterm
}

type FeatureCvterm struct {
    FeatureCvtermId int64 `primary_key:"feature_cvterm_id"`
    IsNot           bool
    Rank            int64
    //foreign keys
    FeatureId int64
    Feature   Feature
    CvtermId  int64
    Cvterm    Cvterm
    PubId     int64
    Pub       Pub
}

type FeatureDbxref struct {
    FeatureDbxrefId int64 `primary_key:"feature_dbxref_id"`
    FeatureId       int64
    DbxrefId        int64
    Feature         Feature
    Dbxref          Dbxref
}
