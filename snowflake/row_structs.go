package snowflake

import (
	"database/sql"
	"time"
)

type showDatabaseRow struct {
	createdOn     time.Time
	name          string
	isDefault     string
	isCurrent     string
	origin        string
	owner         string
	comment       string
	options       string
	retentionTime string
}

type showSchemaRow struct {
	createdOn     time.Time
	name          string
	isDefault     string
	isCurrent     string
	databaseName  string
	owner         string
	comment       string
	options       string
	retentionTime string
}

type showTableRow struct {
	createdOn     time.Time
	name          string
	databaseName  string
	schemaName    string
	kind          string
	comment       string
	clusterBy     string
	rows          int
	bytes         int
	owner         string
	retentionTime string
}

type descTableRow struct {
	colName      string
	colType      string
	kind         string
	isNullable   string
	defaultValue sql.NullString
	isPrimaryKey string
	isUniqueKey  string
	check        sql.NullString
	expression   sql.NullString
	comment      sql.NullString
}

type showColumnsRow struct {
	tableName     string
	schemaName    string
	columnName    string
	dataType      map[string]interface{}
	isNullable    string
	defaultValue  string
	kind          string
	expression    string
	comment       string
	databaseName  string
	autoincrement string
}
