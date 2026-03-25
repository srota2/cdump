package schema

import (
	"database/sql"
	"fmt"
	"log"
)

// FKEdge represents a foreign key relationship: FromTable.FromColumn -> ToTable.ToColumn.
type FKEdge struct {
	FromTable  string
	FromColumn string
	ToTable    string
	ToColumn   string
}

// Graph holds the set of tables and their foreign key relationships.
type Graph struct {
	Tables  []string
	Edges   []FKEdge
	Forward map[string][]FKEdge // table -> edges where this table references others
	Reverse map[string][]FKEdge // table -> edges where others reference this table
}

// BuildGraph queries FK metadata and constructs the relationship graph.
// Only edges between tables in the provided list are included.
func BuildGraph(db *sql.DB, dbName string, tables []string) (*Graph, error) {
	tableSet := make(map[string]bool, len(tables))
	for _, t := range tables {
		tableSet[t] = true
	}

	rows, err := db.Query(
		"SELECT TABLE_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME "+
			"FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE "+
			"WHERE TABLE_SCHEMA = ? AND REFERENCED_TABLE_SCHEMA = ? AND REFERENCED_TABLE_NAME IS NOT NULL",
		dbName, dbName,
	)
	if err != nil {
		return nil, fmt.Errorf("querying FK metadata: %w", err)
	}
	defer rows.Close()

	g := &Graph{
		Tables:  tables,
		Forward: make(map[string][]FKEdge),
		Reverse: make(map[string][]FKEdge),
	}

	for rows.Next() {
		var e FKEdge
		if err := rows.Scan(&e.FromTable, &e.FromColumn, &e.ToTable, &e.ToColumn); err != nil {
			return nil, fmt.Errorf("scanning FK row: %w", err)
		}
		if !tableSet[e.FromTable] || !tableSet[e.ToTable] {
			continue
		}
		g.Edges = append(g.Edges, e)
		g.Forward[e.FromTable] = append(g.Forward[e.FromTable], e)
		g.Reverse[e.ToTable] = append(g.Reverse[e.ToTable], e)
	}
	return g, rows.Err()
}

// DetectCycles returns all strongly connected components with more than one node (Tarjan's algorithm).
func DetectCycles(g *Graph) [][]string {
	index := 0
	stack := []string{}
	onStack := make(map[string]bool)
	indices := make(map[string]int)
	lowlinks := make(map[string]int)
	var sccs [][]string

	// Build adjacency from Forward edges (deduplicated by target table).
	adj := make(map[string]map[string]bool)
	for _, t := range g.Tables {
		adj[t] = make(map[string]bool)
	}
	for _, e := range g.Edges {
		adj[e.FromTable][e.ToTable] = true
	}

	var strongConnect func(v string)
	strongConnect = func(v string) {
		indices[v] = index
		lowlinks[v] = index
		index++
		stack = append(stack, v)
		onStack[v] = true

		for w := range adj[v] {
			if _, visited := indices[w]; !visited {
				strongConnect(w)
				if lowlinks[w] < lowlinks[v] {
					lowlinks[v] = lowlinks[w]
				}
			} else if onStack[w] {
				if indices[w] < lowlinks[v] {
					lowlinks[v] = indices[w]
				}
			}
		}

		if lowlinks[v] == indices[v] {
			var scc []string
			for {
				w := stack[len(stack)-1]
				stack = stack[:len(stack)-1]
				onStack[w] = false
				scc = append(scc, w)
				if w == v {
					break
				}
			}
			if len(scc) > 1 {
				sccs = append(sccs, scc)
			}
		}
	}

	for _, t := range g.Tables {
		if _, visited := indices[t]; !visited {
			strongConnect(t)
		}
	}
	return sccs
}

// TopologicalSort returns a topological ordering of tables (dependencies first).
// Cycles (SCCs) are collapsed: tables in the same SCC are grouped together.
func TopologicalSort(g *Graph) []string {
	sccs := DetectCycles(g)

	// Map each table to its SCC representative (first element), or itself if not in any SCC.
	sccOf := make(map[string]string)
	sccMembers := make(map[string][]string)
	for _, scc := range sccs {
		rep := scc[0]
		log.Printf("WARNING: circular FK detected among tables: %v", scc)
		for _, t := range scc {
			sccOf[t] = rep
		}
		sccMembers[rep] = scc
	}

	// Build a deduplicated node set (using SCC representatives).
	nodeSet := make(map[string]bool)
	var nodes []string
	for _, t := range g.Tables {
		n := t
		if rep, ok := sccOf[t]; ok {
			n = rep
		}
		if !nodeSet[n] {
			nodeSet[n] = true
			nodes = append(nodes, n)
		}
	}

	// Build collapsed adjacency and in-degree.
	adj := make(map[string]map[string]bool)
	inDeg := make(map[string]int)
	for _, n := range nodes {
		adj[n] = make(map[string]bool)
		inDeg[n] = 0
	}
	for _, e := range g.Edges {
		from := e.FromTable
		to := e.ToTable
		if rep, ok := sccOf[from]; ok {
			from = rep
		}
		if rep, ok := sccOf[to]; ok {
			to = rep
		}
		if from == to {
			continue // self-edge within SCC
		}
		if !adj[from][to] {
			adj[from][to] = true
			inDeg[to]++
		}
	}

	// Kahn's algorithm.
	var queue []string
	for _, n := range nodes {
		if inDeg[n] == 0 {
			queue = append(queue, n)
		}
	}

	var sorted []string
	for len(queue) > 0 {
		n := queue[0]
		queue = queue[1:]
		sorted = append(sorted, n)
		for m := range adj[n] {
			inDeg[m]--
			if inDeg[m] == 0 {
				queue = append(queue, m)
			}
		}
	}

	// Expand SCC representatives back to their members.
	var result []string
	for _, n := range sorted {
		if members, ok := sccMembers[n]; ok {
			result = append(result, members...)
		} else {
			result = append(result, n)
		}
	}

	// In our graph, edges go from -> to meaning "from references to".
	// Kahn's processes nodes with in-degree 0 first (leaf tables, not referenced by anyone).
	// We want referenced tables (dependencies) first, so reverse.
	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}

	return result
}

// FindDependentTables does a BFS from rootTable through the Reverse adjacency
// (tables that reference rootTable, tables that reference those, etc.).
// Returns a map of table -> BFS level (0 = rootTable itself).
func FindDependentTables(g *Graph, rootTable string) map[string]int {
	levels := map[string]int{rootTable: 0}
	queue := []string{rootTable}

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		currentLevel := levels[current]

		for _, edge := range g.Reverse[current] {
			if _, seen := levels[edge.FromTable]; !seen {
				levels[edge.FromTable] = currentLevel + 1
				queue = append(queue, edge.FromTable)
			}
		}
	}
	return levels
}

// EdgesFromTo returns all FK edges from fromTable to toTable.
func (g *Graph) EdgesFromTo(fromTable, toTable string) []FKEdge {
	var result []FKEdge
	for _, e := range g.Forward[fromTable] {
		if e.ToTable == toTable {
			result = append(result, e)
		}
	}
	return result
}

// AddEdge adds a single edge (e.g. a soft/undeclared FK) to the graph.
// The edge is only added if both tables are in the graph's table set.
func (g *Graph) AddEdge(e FKEdge) {
	tableSet := make(map[string]bool, len(g.Tables))
	for _, t := range g.Tables {
		tableSet[t] = true
	}
	if !tableSet[e.FromTable] || !tableSet[e.ToTable] {
		return
	}
	g.Edges = append(g.Edges, e)
	g.Forward[e.FromTable] = append(g.Forward[e.FromTable], e)
	g.Reverse[e.ToTable] = append(g.Reverse[e.ToTable], e)
}
