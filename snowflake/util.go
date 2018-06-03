package snowflake

import (
	"database/sql"
	"fmt"
)

/*
showStatementExists is a helper function to check whether a Snowflake object
exists or not. In the context of the Snowflake terraform provider, before
modifying any object we want to know that one and only one object exists. While
by default identifiers are case insensitive, (see
https://docs.snowflake.net/manuals/sql-reference/identifiers-syntax.html)
identifiers can be made case sensitive by wrapping them in double quotes. For
instance, it's possible to issue these queries:

create database "foo";
create database "FOO";

And get two results for the following
show databases like 'foo';
*/
func showStatementExists(db *sql.DB, statement string) (bool, error) {
	rows, err := db.Query(statement)
	if err != nil {
		return true, err
	}
	defer rows.Close()
	index := 0
	for rows.Next() {
		index++
	}
	if index == 0 {
		return false, nil
	}
	if index == 1 {
		return true, nil
	}
	if index > 1 {
		return true, fmt.Errorf("More than 1 row returned for \"%s\"", statement)
	}
	return true, fmt.Errorf("this should never happen")
}

func sqlObjExists(db *sql.DB, objectType string, name string, inClause string) (bool, error) {
	statement := fmt.Sprintf("SHOW %s LIKE '%s' in %s", objectType, name, inClause)
	return showStatementExists(db, statement)
}

func showDatabase(db *sql.DB, name string) (showDatabaseRow, error) {
	var r showDatabaseRow
	// This verifies that one and only one database exists
	exists, err := sqlObjExists(db, "databases", name, "account")
	if err != nil {
		return r, err
	}
	if exists == false {
		return r, fmt.Errorf("Database %s does not exist", name)
	}
	statement := fmt.Sprintf("SHOW DATABASES LIKE '%s'", name)
	rows, err := db.Query(statement)
	if err != nil {
		return r, err
	}
	defer rows.Close()
	for rows.Next() {
		if err := rows.Scan(
			&r.createdOn,
			&r.name,
			&r.isDefault,
			&r.isCurrent,
			&r.origin,
			&r.owner,
			&r.comment,
			&r.options,
			&r.retentionTime,
		); err != nil {
			return r, err
		}
	}
	return r, nil
}

func showSchema(db *sql.DB, databaseName string, name string) (showSchemaRow, error) {
	var r showSchemaRow
	// This verifies that one and only one database exists
	exists, err := sqlObjExists(db, "schemas", name, databaseName)
	if err != nil {
		return r, err
	}
	if exists == false {
		return r, fmt.Errorf("Schema %s.%s does not exist", databaseName, name)
	}
	statement := fmt.Sprintf("SHOW SCHEMAS LIKE '%s' in %s", name, databaseName)
	rows, err := db.Query(statement)
	if err != nil {
		return r, err
	}
	defer rows.Close()
	for rows.Next() {
		if err := rows.Scan(
			&r.createdOn,
			&r.name,
			&r.isDefault,
			&r.isCurrent,
			&r.databaseName,
			&r.owner,
			&r.comment,
			&r.options,
			&r.retentionTime,
		); err != nil {
			return r, err
		}
	}
	return r, nil
}

func readTable(db *sql.DB, database string, schema string, name string) (infoSchemaTable, error) {
	var r infoSchemaTable
	exists, err := sqlObjExists(db, "tables", name, fmt.Sprintf("%s.%s", database, schema))
	if err != nil {
		return r, err
	}
	if exists == false {
		return r, fmt.Errorf("Table %s.%s.%s does not exist", database, schema, name)
	}
	statement := fmt.Sprintf("SELECT * from %s.information_schema.tables where table_name = '%s' and table_schema = '%s'", database, name, schema)
	rows, err := db.Query(statement)
	if err != nil {
		return r, err
	}
	defer rows.Close()
	for rows.Next() {
		if err := rows.Scan(
			&r.tableCatalog,
			&r.tableSchema,
			&r.tableName,
			&r.tableOwner,
			&r.tableType,
			&r.isTransient,
			&r.clusteringKey,
			&r.rowCount,
			&r.bytes,
			&r.retentionTime,
			&r.selfReferencingColumnName,
			&r.referenceGeneration,
			&r.userDefinedTypeColumn,
			&r.userDefinedTypeSchema,
			&r.userDefinedTypeName,
			&r.isInsertableInto,
			&r.isTyped,
			&r.commitAction,
			&r.created,
			&r.lastAltered,
			&r.comment,
		); err != nil {
			return r, err
		}
	}
	return r, nil
}

func showTable(db *sql.DB, databaseName string, schemaName string, name string) (showTableRow, error) {
	var r showTableRow
	// This verifies that one and only one database exists
	exists, err := sqlObjExists(db, "tables", name, fmt.Sprintf("%s.%s", databaseName, schemaName))
	if err != nil {
		return r, err
	}
	if exists == false {
		return r, fmt.Errorf("Table %s.%s.%s does not exist", databaseName, schemaName, name)
	}
	statement := fmt.Sprintf("SHOW TABLES LIKE '%s' in %s.%s", name, databaseName, schemaName)
	rows, err := db.Query(statement)
	if err != nil {
		return r, err
	}
	defer rows.Close()
	for rows.Next() {
		if err := rows.Scan(
			&r.createdOn,
			&r.name,
			&r.databaseName,
			&r.schemaName,
			&r.kind,
			&r.comment,
			&r.clusterBy,
			&r.rows,
			&r.bytes,
			&r.owner,
			&r.retentionTime,
		); err != nil {
			return r, err
		}
	}
	return r, nil
}

func descTable(db *sql.DB, databaseName string, schemaName string, name string) ([]descTableRow, error) {
	var columnInfo []descTableRow
	// This verifies that one and only one database exists
	exists, err := sqlObjExists(db, "tables", name, fmt.Sprintf("%s.%s", databaseName, schemaName))
	if err != nil {
		return columnInfo, err
	}
	if exists == false {
		return columnInfo, fmt.Errorf("Table %s.%s.%s does not exist", databaseName, schemaName, name)
	}
	statement := fmt.Sprintf("DESC TABLE %s.%s.%s", databaseName, schemaName, name)
	rows, err := db.Query(statement)
	if err != nil {
		return columnInfo, err
	}
	defer rows.Close()
	for rows.Next() {
		var r descTableRow
		if err := rows.Scan(
			&r.colName,
			&r.colType,
			&r.kind,
			&r.isNullable,
			&r.defaultValue,
			&r.isPrimaryKey,
			&r.isUniqueKey,
			&r.check,
			&r.expression,
			&r.comment,
		); err != nil {
			return columnInfo, err
		}
		columnInfo = append(columnInfo, r)
	}
	return columnInfo, nil
}

func showPipe(db *sql.DB, database string, schema string, name string) (showPipeRow, error) {
	var r showPipeRow
	// This verifies that one and only one database exists
	exists, err := sqlObjExists(db, "pipes", name, fmt.Sprintf("%s.%s", database, schema))
	if err != nil {
		return r, err
	}
	if exists == false {
		return r, fmt.Errorf("Pipe %s.%s.%s does not exist", database, schema, name)
	}
	statement := fmt.Sprintf("SHOW PIPES LIKE '%s' in %s.%s", name, database, schema)
	rows, err := db.Query(statement)
	if err != nil {
		return r, err
	}
	defer rows.Close()
	for rows.Next() {
		if err := rows.Scan(
			&r.createdOn,
			&r.name,
			&r.databaseName,
			&r.schemaName,
			&r.definition,
			&r.owner,
			&r.notificationChannel,
			&r.comment,
		); err != nil {
			return r, err
		}
	}
	return r, nil
}
