package main

import (
	"cdump/config"
	"cdump/db"
	"cdump/dump"
	"cdump/schema"
	"log"
	"os"
)

const (
	userTable  = "Anagrafica"
	userColumn = "username"
)

func main() {
	cfgPath := "config.yaml"
	if len(os.Args) > 1 {
		cfgPath = os.Args[1]
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// ---- Connect to databases ----
	log.Println("Connecting to origin database...")
	originDB, err := db.Connect(cfg.Origin)
	if err != nil {
		log.Fatalf("Failed to connect to origin: %v", err)
	}
	defer originDB.Close()

	// ---- List and filter tables ----
	log.Println("Listing tables in origin database...")
	allTables, err := schema.ListTables(originDB, cfg.Origin.Database)
	if err != nil {
		log.Fatalf("Failed to list tables: %v", err)
	}
	log.Printf("Found %d tables in origin", len(allTables))

	tables, err := schema.FilterTables(allTables, cfg.ExcludeTables)
	if err != nil {
		log.Fatalf("Failed to filter tables: %v", err)
	}
	log.Printf("After excluding patterns: %d tables remain", len(tables))

	emptyPatterns, err := schema.CompilePatterns(cfg.EmptyTables)
	if err != nil {
		log.Fatalf("Failed to compile emptyTables patterns: %v", err)
	}

	// ---- Extract DDL ----
	log.Println("Extracting DDL for all tables...")
	ddlMap, err := schema.GetAllDDL(originDB, tables)
	if err != nil {
		log.Fatalf("Failed to get DDL: %v", err)
	}

	// ---- Build FK graph and sort ----
	log.Println("Analyzing foreign key relationships...")
	g, err := schema.BuildGraph(originDB, cfg.Origin.Database, tables)
	if err != nil {
		log.Fatalf("Failed to build FK graph: %v", err)
	}
	log.Printf("Found %d foreign key relationships", len(g.Edges))

	// Add soft (undeclared) relationships from config.
	for _, sr := range cfg.SoftRelationships {
		g.AddEdge(schema.FKEdge{
			FromTable:  sr.FromTable,
			FromColumn: sr.FromColumn,
			ToTable:    sr.ToTable,
			ToColumn:   sr.ToColumn,
		})
	}
	if len(cfg.SoftRelationships) > 0 {
		log.Printf("Added %d soft relationships (total edges: %d)", len(cfg.SoftRelationships), len(g.Edges))
	}

	sortedTables := schema.TopologicalSort(g)

	// ---- Write schema.sql ----
	schemaPath := "schema.sql"
	log.Printf("Writing %s...", schemaPath)
	if err := dump.WriteSchemaSQL(schemaPath, sortedTables, ddlMap); err != nil {
		log.Fatalf("Failed to write schema.sql: %v", err)
	}
	log.Printf("schema.sql written with %d tables", len(tables))

	// ---- Write data.sql ----
	dataPath := "data.sql"
	log.Printf("Writing %s...", dataPath)
	if err := dump.WriteDataSQL(dataPath, originDB, cfg.Origin.Database, sortedTables, g, userTable, userColumn, cfg.KeepUsernames, cfg.ExcludeFields, cfg.ReplaceFields, emptyPatterns); err != nil {
		log.Fatalf("Failed to write data.sql: %v", err)
	}
	log.Println("data.sql written successfully")

	log.Println("Done!")
}
