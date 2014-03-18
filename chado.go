package gochado

// Interface for making a chado loader from staging tables
type ChadoLoader interface {
    // To prepare involved chado tables for bulk load, such as
    // disabling indexes and/or foreign keys if needed
    AlterTables()
    // Primarilly to complement the AlterTables method, put back the chaged tables
    // in its pristine states. Could also be used to re-calcualate statistics
    // on tables that has inserts after bulk load.
    ResetTables()
    // Actual data loading by running a series of sql statements that transfers
    // data from staging tables.
    BulkLoad()
}
