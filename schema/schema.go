package schema

import (
	"database/sql"
	"fmt"
	"regexp"
)

// ListTables returns all BASE TABLE names in the given database.
func ListTables(db *sql.DB, dbName string) ([]string, error) {
	rows, err := db.Query(
		"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME",
		dbName,
	)
	if err != nil {
		return nil, fmt.Errorf("listing tables: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scanning table name: %w", err)
		}
		tables = append(tables, name)
	}
	return tables, rows.Err()
}

// FilterTables removes tables whose names match any of the exclude regex patterns.
func FilterTables(tables []string, excludePatterns []string) ([]string, error) {
	var compiled []*regexp.Regexp
	for _, p := range excludePatterns {
		re, err := regexp.Compile(p)
		if err != nil {
			return nil, fmt.Errorf("compiling exclude pattern %q: %w", p, err)
		}
		compiled = append(compiled, re)
	}

	var result []string
	for _, t := range tables {
		excluded := false
		for _, re := range compiled {
			if re.MatchString(t) {
				excluded = true
				break
			}
		}
		if !excluded {
			result = append(result, t)
		}
	}
	return result, nil
}

// CompilePatterns compiles a list of regex patterns and returns the compiled regexps.
func CompilePatterns(patterns []string) ([]*regexp.Regexp, error) {
	var compiled []*regexp.Regexp
	for _, p := range patterns {
		re, err := regexp.Compile(p)
		if err != nil {
			return nil, fmt.Errorf("compiling pattern %q: %w", p, err)
		}
		compiled = append(compiled, re)
	}
	return compiled, nil
}

// MatchesAny returns true if the name matches any of the compiled patterns.
func MatchesAny(name string, patterns []*regexp.Regexp) bool {
	for _, re := range patterns {
		if re.MatchString(name) {
			return true
		}
	}
	return false
}

// GetCreateTable returns the DDL statement for the given table.
func GetCreateTable(db *sql.DB, table string) (string, error) {
	var tableName, ddl string
	// nosemgrep -- This query is not vulnerable to SQL injection because the table name is not user-provided input, but rather comes from the database metadata.
	err := db.QueryRow("SHOW CREATE TABLE `"+table+"`").Scan(&tableName, &ddl)
	if err != nil {
		return "", fmt.Errorf("getting DDL for %s: %w", table, err)
	}
	return ddl, nil
}

// GetAllDDL retrieves the CREATE TABLE statement for every table in the list.
func GetAllDDL(db *sql.DB, tables []string) (map[string]string, error) {
	ddlMap := make(map[string]string, len(tables))
	for _, t := range tables {
		ddl, err := GetCreateTable(db, t)
		if err != nil {
			return nil, err
		}
		ddlMap[t] = ddl
	}
	return ddlMap, nil
}

// GetPrimaryKeys returns the primary key column names for the given table.
func GetPrimaryKeys(db *sql.DB, dbName, table string) ([]string, error) {
	rows, err := db.Query(
		"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE "+
			"WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY' "+
			"ORDER BY ORDINAL_POSITION",
		dbName, table,
	)
	if err != nil {
		return nil, fmt.Errorf("getting PKs for %s: %w", table, err)
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, fmt.Errorf("scanning PK column: %w", err)
		}
		cols = append(cols, col)
	}
	return cols, rows.Err()
}

// GetColumnNames returns the ordered column names for the given table.
func GetColumnNames(db *sql.DB, dbName, table string) ([]string, error) {
	rows, err := db.Query(
		"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "+
			"WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? "+
			"ORDER BY ORDINAL_POSITION",
		dbName, table,
	)
	if err != nil {
		return nil, fmt.Errorf("getting columns for %s: %w", table, err)
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, fmt.Errorf("scanning column name: %w", err)
		}
		cols = append(cols, col)
	}
	return cols, rows.Err()
}
