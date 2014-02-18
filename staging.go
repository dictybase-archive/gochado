package gochado

// Interface for making a loader for staging tables
type StagingLoader interface {
    // Add a row of unprocessed data to the staging cache
    AddDataRow(string)
    // Create temporary staging tables
    CreateTables()
    // Drop the staging tables
    DropTables()
    // Alteration in the staging tables, for example creating indexes
    AlterTables()
    // Bulk upload data from staging cache to the staging tables
    BulkLoad()
}
