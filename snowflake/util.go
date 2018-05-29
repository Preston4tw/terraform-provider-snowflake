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
func showStatementExists(databaseHandle *sql.DB, statement string) (bool, error) {
	rows, err := databaseHandle.Query(statement)
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

func databaseExists(databaseHandle *sql.DB, databaseName string) (bool, error) {
	statement := fmt.Sprintf("SHOW DATABASES LIKE '%s'", databaseName)
	return showStatementExists(databaseHandle, statement)
}

func schemaExists(databaseHandle *sql.DB, databaseName string, schemaName string) (bool, error) {
	statement := fmt.Sprintf("SHOW SCHEMAS LIKE '%s' in %s", schemaName, databaseName)
	return showStatementExists(databaseHandle, statement)
}

func tableExists(databaseHandle *sql.DB, databaseName string, schemaName string, tableName string) (bool, error) {
	statement := fmt.Sprintf("SHOW TABLES LIKE '%s' in %s.%s", tableName, databaseName, schemaName)
	return showStatementExists(databaseHandle, statement)
}

func showDatabase(databaseHandle *sql.DB, databaseName string) (showDatabaseRow, error) {
	var r showDatabaseRow
	// This verifies that one and only one database exists
	exists, err := databaseExists(databaseHandle, databaseName)
	if err != nil {
		return r, err
	}
	if exists == false {
		return r, fmt.Errorf("Database %s does not exist", databaseName)
	}
	statement := fmt.Sprintf("SHOW DATABASES LIKE '%s'", databaseName)
	rows, err := databaseHandle.Query(statement)
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

func showSchema(databaseHandle *sql.DB, databaseName string, schemaName string) (showSchemaRow, error) {
	var r showSchemaRow
	// This verifies that one and only one database exists
	exists, err := schemaExists(databaseHandle, databaseName, schemaName)
	if err != nil {
		return r, err
	}
	if exists == false {
		return r, fmt.Errorf("Schema %s.%s does not exist", databaseName, schemaName)
	}
	statement := fmt.Sprintf("SHOW SCHEMAS LIKE '%s' in %s", schemaName, databaseName)
	rows, err := databaseHandle.Query(statement)
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

func showTable(databaseHandle *sql.DB, databaseName string, schemaName string, tableName string) (showTableRow, error) {
	var r showTableRow
	// This verifies that one and only one database exists
	exists, err := tableExists(databaseHandle, databaseName, schemaName, tableName)
	if err != nil {
		return r, err
	}
	if exists == false {
		return r, fmt.Errorf("Table %s.%s.%s does not exist", databaseName, schemaName, tableName)
	}
	statement := fmt.Sprintf("SHOW TABLES LIKE '%s' in %s.%s", tableName, databaseName, schemaName)
	rows, err := databaseHandle.Query(statement)
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

func descTable(databaseHandle *sql.DB, databaseName string, schemaName string, tableName string) ([]descTableRow, error) {
	var columnInfo []descTableRow
	// This verifies that one and only one database exists
	exists, err := tableExists(databaseHandle, databaseName, schemaName, tableName)
	if err != nil {
		return columnInfo, err
	}
	if exists == false {
		return columnInfo, fmt.Errorf("Table %s.%s.%s does not exist", databaseName, schemaName, tableName)
	}
	statement := fmt.Sprintf("DESC TABLE %s.%s.%s", databaseName, schemaName, tableName)
	rows, err := databaseHandle.Query(statement)
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
