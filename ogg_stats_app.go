package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"sync" // Add sync package for mutex

	"github.com/gin-gonic/gin"

	_ "github.com/mattn/go-sqlite3"
)

var (
	db      *sql.DB
	logger  *log.Logger
	dbMutex sync.Mutex // Add mutex for database synchronization
)

// tableStats represents statistics for a single table
type tableStats struct {
	inserts, updates, deletes    int
	deltaIns, deltaUpd, deltaDel int
}

var (
	version   string
	buildDate string
)

func main() {
	fmt.Printf("GoldenGate Stats Utility v%s (%s) Copyright (c) 2025, CDL(139-8001-3650). All rights reserved.\n", version, buildDate)

	// Flags
	paramsFlag := flag.String("params", "", "Comma-separated GoldenGate parameter files (supports PDB format)")
	intervalFlag := flag.Int("interval", 5, "Stats collection interval in minutes")
	portFlag := flag.Int("port", 8080, "Web server port")
	freshFlag := flag.Bool("fresh", false, "Truncate all stats tables (use when extract restarts)")
	webOnlyFlag := flag.Bool("webonly", false, "Start web UI only; do not collect stats")
	flag.Parse()

	// Backward compatibility: allow positional arg as params if -params not set
	paramsInput := *paramsFlag
	if paramsInput == "" && len(flag.Args()) > 0 {
		paramsInput = flag.Args()[0]
	}

	if strings.TrimSpace(paramsInput) == "" && !*webOnlyFlag {
		fmt.Println("Usage: oggstats -params <file1.prm[,file2.prm,...]> [-interval N] [-port P] [-fresh] [-webonly]")
		os.Exit(1)
	}

	var paramFiles []string
	for _, p := range strings.Split(paramsInput, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			paramFiles = append(paramFiles, p)
		}
	}
	if len(paramFiles) == 0 && !*webOnlyFlag {
		fmt.Println("No valid parameter files provided")
		os.Exit(1)
	}

	tickerInterval := *intervalFlag
	webPort := *portFlag
	freshStart := *freshFlag
	webOnly := *webOnlyFlag

	// open log file first
	logFile, err := os.OpenFile("oggstats.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer logFile.Close()

	// console + file logging
	mw := io.MultiWriter(os.Stdout, logFile)
	logger = log.New(mw, "", log.LstdFlags)

	// Build extracts and GG home map (skip if web-only)
	extracts := make([]string, 0, len(paramFiles))
	ggHomeByExtract := make(map[string]string)
	if !webOnly {
		for _, pf := range paramFiles {
			extractName := strings.TrimSuffix(filepath.Base(pf), filepath.Ext(pf))
			extracts = append(extracts, extractName)
			ggHome := extractGGHomeFromParamFile(pf)
			ggHomeByExtract[extractName] = ggHome
			logger.Printf("Extract configured: name=%s param=%s gg_home=%s\n", extractName, pf, ggHome)
		}
	}
	logger.Printf("Configuration: ticker_interval=%d minutes, web_port=%d web_only=%v\n", tickerInterval, webPort, webOnly)

	// init sqlite
	db, err = sql.Open("sqlite3", "./ogg_stats.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Test database connection
	err = db.Ping()
	if err != nil {
		log.Fatal("Database ping failed:", err)
	}
	logger.Printf("Database connection established successfully\n")

	initSchema()
	logger.Printf("Database schema initialized\n")

	// If fresh start is requested, truncate all stats tables (skip in web-only)
	if freshStart && !webOnly {
		logger.Printf("Fresh start requested. Truncating all stats tables...\n")
		truncateStatsTables()
		logger.Printf("All stats tables truncated successfully\n")
	}

	// parse each param file once (skip in web-only)
	if !webOnly {
		for _, pf := range paramFiles {
			extractName := strings.TrimSuffix(filepath.Base(pf), filepath.Ext(pf))
			parseParamFile(pf, extractName)
		}
	}

	// Check tables discovered per extract (only when collecting)
	if !webOnly {
		for _, ex := range extracts {
			dbMutex.Lock()
			var tableCount int
			err = db.QueryRow("SELECT COUNT(*) FROM ogg_tables WHERE extract_name = ?", ex).Scan(&tableCount)
			dbMutex.Unlock()
			if err != nil {
				logger.Printf("Error checking table count for %s: %v\n", ex, err)
			} else {
				logger.Printf("Discovered %d tables for extract %s\n", tableCount, ex)
				if tableCount == 0 {
					logger.Printf("No tables found for extract %s. Please check your parameter file.\n", ex)
				}
			}
		}
	}

	// Setup web server
	if webOnly {
		// Choose a default extract from existing DB
		var defaultExtract string
		dbMutex.Lock()
		_ = db.QueryRow("SELECT extract_name FROM ogg_tables GROUP BY extract_name ORDER BY extract_name LIMIT 1").Scan(&defaultExtract)
		dbMutex.Unlock()
		if defaultExtract == "" {
			defaultExtract = "default"
		}
		setupWebServer(defaultExtract, webPort)
		// Block forever in web-only mode
		select {}
	} else {
		// Use first configured extract as default
		setupWebServer(extracts[0], webPort)
	}

	// schedule task with configurable interval (skip in web-only)
	if !webOnly {
		ticker := time.NewTicker(time.Duration(tickerInterval) * time.Minute)
		defer ticker.Stop()

		// run immediately once for all extracts
		for _, ex := range extracts {
			runStats(ex, ggHomeByExtract[ex])
		}

		for range ticker.C {
			for _, ex := range extracts {
				runStats(ex, ggHomeByExtract[ex])
			}
		}
	}
}

// extractGGHomeFromParamFile extracts the GoldenGate home directory from parameter file path
func extractGGHomeFromParamFile(paramFile string) string {
	// Get the absolute path of the parameter file
	absPath, err := filepath.Abs(paramFile)
	if err != nil {
		log.Printf("Error getting absolute path for %s: %v\n", paramFile, err)
		return ""
	}

	// Split the path into components
	pathComponents := strings.Split(absPath, string(filepath.Separator))

	// Look for "dirprm" directory in the path
	for i, component := range pathComponents {
		if component == "dirprm" && i > 0 {
			// Reconstruct the path up to the parent of dirprm
			ggHome := strings.Join(pathComponents[:i], string(filepath.Separator))
			// Add leading slash back
			if !strings.HasPrefix(ggHome, "/") {
				ggHome = "/" + ggHome
			}
			return ggHome
		}
	}

	// If dirprm not found, try to find other common GG directories
	for i, component := range pathComponents {
		if (component == "dirprm" || component == "dirdat" || component == "dirchk" || component == "dirrpt") && i > 0 {
			// Reconstruct the path up to the parent of the GG directory
			ggHome := strings.Join(pathComponents[:i], string(filepath.Separator))
			// Add leading slash back
			if !strings.HasPrefix(ggHome, "/") {
				ggHome = "/" + ggHome
			}
			return ggHome
		}
	}

	// If no GG directory found, return empty string (will use PATH)
	log.Printf("Warning: Could not determine GoldenGate home from parameter file path: %s\n", paramFile)
	return ""
}

// init schema
func initSchema() {
	schema := `
CREATE TABLE IF NOT EXISTS ogg_tables(
    extract_name TEXT,
    pdb_name     TEXT,
    schema_name  TEXT,
    table_name   TEXT,
    PRIMARY KEY (extract_name, pdb_name, schema_name, table_name)
);
CREATE TABLE IF NOT EXISTS ogg_table_stats_snap(
    run_time     DATETIME,
    extract_name TEXT,
    pdb_name     TEXT,
    schema_name  TEXT,
    table_name   TEXT,
    inserts      INTEGER,
    updates      INTEGER,
    deletes      INTEGER,
    PRIMARY KEY (run_time, extract_name, pdb_name, schema_name, table_name)
);
CREATE INDEX IF NOT EXISTS idx_snap_time ON ogg_table_stats_snap(extract_name, run_time);
CREATE INDEX IF NOT EXISTS idx_snap_keys ON ogg_table_stats_snap(extract_name, pdb_name, schema_name, table_name, run_time);
`
	_, err := db.Exec(schema)
	if err != nil {
		log.Fatal(err)
	}
}

// truncateStatsTables truncates all stats tables to reset statistics
func truncateStatsTables() {
	tables := []string{
		"ogg_table_stats_snap",
	}

	for _, table := range tables {
		_, err := db.Exec(fmt.Sprintf("DELETE FROM %s", table))
		if err != nil {
			logger.Printf("Error truncating table %s: %v\n", table, err)
		} else {
			logger.Printf("Truncated table: %s\n", table)
		}
	}
}

// parse param file to extract tables
func parseParamFile(file string, extract string) {
	logger.Printf("Parsing parameter file: %s for extract: %s\n", file, extract)

	f, err := os.Open(file)
	if err != nil {
		logger.Printf("Error opening parameter file %s: %v\n", file, err)
		log.Fatal(err)
	}
	defer f.Close()

	// Support both formats:
	// 1. Oracle CDB/PDB: TABLE pdbname.schema.tablename
	// 2. Traditional: TABLE schema.tablename
	rePDB := regexp.MustCompile(`(?i)TABLE\s+([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)`)
	reTraditional := regexp.MustCompile(`(?i)TABLE\s+([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)`)

	lineCount := 0
	pdbTables := 0
	traditionalTables := 0

	// Lock database for this operation
	dbMutex.Lock()
	defer dbMutex.Unlock()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		lineCount++

		// Try PDB format first
		m := rePDB.FindStringSubmatch(line)
		if len(m) == 4 {
			// PDB format: pdbname.schema.tablename
			_, err := db.Exec(`INSERT OR IGNORE INTO ogg_tables(extract_name,pdb_name,schema_name,table_name) VALUES(?,?,?,?)`,
				extract, m[1], m[2], m[3])
			if err != nil {
				logger.Printf("Error inserting PDB table %s.%s.%s: %v\n", m[1], m[2], m[3], err)
			} else {
				logger.Printf("Discovered PDB table %s.%s.%s for extract %s\n", m[1], m[2], m[3], extract)
				pdbTables++
			}
			continue
		}

		// Try traditional format
		m = reTraditional.FindStringSubmatch(line)
		if len(m) == 3 {
			// Traditional format: schema.tablename (pdb_name will be NULL)
			_, err := db.Exec(`INSERT OR IGNORE INTO ogg_tables(extract_name,pdb_name,schema_name,table_name) VALUES(?,?,?,?)`,
				extract, "", m[1], m[2])
			if err != nil {
				logger.Printf("Error inserting traditional table %s.%s: %v\n", m[1], m[2], err)
			} else {
				logger.Printf("Discovered traditional table %s.%s for extract %s\n", m[1], m[2], extract)
				traditionalTables++
			}
		}
	}

	if err := scanner.Err(); err != nil {
		logger.Printf("Error reading parameter file: %v\n", err)
	}

	logger.Printf("Parameter file parsing complete. Processed %d lines, found %d PDB tables, %d traditional tables\n",
		lineCount, pdbTables, traditionalTables)
}

// run stats for all tables
func runStats(extract string, ggHome string) {
	now := time.Now().Truncate(5 * time.Minute) // round to 5-min intervals
	logger.Printf("==== Running stats for extract=%s at %s ====\n", extract, now.Format(time.RFC3339))

	// Lock database for this entire operation
	dbMutex.Lock()
	defer dbMutex.Unlock()

	// First check if we have any tables for this extract
	var tableCount int
	err := db.QueryRow("SELECT COUNT(*) FROM ogg_tables WHERE extract_name = ?", extract).Scan(&tableCount)
	if err != nil {
		logger.Printf("Error checking table count for extract %s: %v\n", extract, err)
		return
	}

	if tableCount == 0 {
		logger.Printf("No tables found for extract %s, skipping stats collection\n", extract)
		return
	}

	// Get unique schemas from the database
	rows, err := db.Query(`SELECT DISTINCT pdb_name, schema_name FROM ogg_tables WHERE extract_name=? ORDER BY pdb_name, schema_name`, extract)
	if err != nil {
		logger.Printf("Database query error: %v\n", err)
		return
	}
	defer rows.Close()

	// Collect all schema data first
	var schemas []struct {
		pdbName, schema string
	}
	for rows.Next() {
		var pdbName, schema string
		err := rows.Scan(&pdbName, &schema)
		if err != nil {
			logger.Printf("Row scan error: %v\n", err)
			continue
		}
		schemas = append(schemas, struct {
			pdbName, schema string
		}{pdbName, schema})
	}

	// Check for errors from iterating over rows
	if err := rows.Err(); err != nil {
		logger.Printf("Error iterating over rows: %v\n", err)
	}

	logger.Printf("Found %d unique schemas to process\n", len(schemas))

	// Collect all stats in memory first
	statsMap := make(map[string]tableStats)

	// Process each schema sequentially
	for _, s := range schemas {
		schemaKey := fmt.Sprintf("%s|%s", s.pdbName, s.schema)
		logger.Printf("Processing schema: %s\n", schemaKey)

		// Get stats for all tables in this schema
		tableStatsMap := getStatsFromGGSCIBySchema(extract, s.pdbName, s.schema, ggHome)

		// Merge table stats into main stats map
		for tableKey, stats := range tableStatsMap {
			statsMap[tableKey] = stats
		}

		logger.Printf("Collected stats for schema %s: %d tables\n", schemaKey, len(tableStatsMap))
	}

	// Now calculate deltas and write to database sequentially
	logger.Printf("Calculating deltas and writing %d table stats to database...\n", len(statsMap))

	for key, stats := range statsMap {
		parts := strings.Split(key, "|")
		if len(parts) != 4 {
			logger.Printf("Invalid key format: %s\n", key)
			continue
		}

		extractName := parts[0]
		pdbName := parts[1]
		schema := parts[2]
		table := parts[3]

		// Get previous stats for delta calculation
		var prevIns, prevUpd, prevDel int
		row := db.QueryRow(`
			SELECT inserts,updates,deletes FROM ogg_table_stats_snap
			WHERE extract_name=? AND pdb_name=? AND schema_name=? AND table_name=? AND run_time < ?
			ORDER BY run_time DESC LIMIT 1`,
			extractName, pdbName, schema, table, now)
		err = row.Scan(&prevIns, &prevUpd, &prevDel)
		if err != nil && err != sql.ErrNoRows {
			logger.Printf("Previous stats query error: %v\n", err)
			// If we can't get previous stats, use current as delta
			prevIns, prevUpd, prevDel = 0, 0, 0
		}

		deltaIns := stats.inserts - prevIns
		deltaUpd := stats.updates - prevUpd
		deltaDel := stats.deletes - prevDel

		// handle restart case
		if deltaIns < 0 || deltaUpd < 0 || deltaDel < 0 {
			if pdbName != "" {
				logger.Printf("Detected counter reset for %s.%s.%s, taking current as delta\n", pdbName, schema, table)
			} else {
				logger.Printf("Detected counter reset for %s.%s, taking current as delta\n", schema, table)
			}
			deltaIns, deltaUpd, deltaDel = stats.inserts, stats.updates, stats.deletes
		}

		// Update stats with calculated deltas
		stats.deltaIns = deltaIns
		stats.deltaUpd = deltaUpd
		stats.deltaDel = deltaDel
		statsMap[key] = stats

		// Log with PDB info if available
		if pdbName != "" {
			logger.Printf("Collected [%s.%s.%s] C=%d U=%d D=%d\n", pdbName, schema, table, deltaIns, deltaUpd, deltaDel)
		} else {
			logger.Printf("Collected [%s.%s] C=%d U=%d D=%d\n", schema, table, deltaIns, deltaUpd, deltaDel)
		}

		// Ensure this table exists in ogg_tables to prevent orphan stats
		_, err = db.Exec(`INSERT OR IGNORE INTO ogg_tables(extract_name,pdb_name,schema_name,table_name) VALUES(?,?,?,?)`,
			extractName, pdbName, schema, table)
		if err != nil {
			if pdbName != "" {
				logger.Printf("register table error for %s.%s.%s: %v\n", pdbName, schema, table, err)
			} else {
				logger.Printf("register table error for %s.%s: %v\n", schema, table, err)
			}
		}

		// save snapshot (for historical tracking)
		_, err = db.Exec(`INSERT INTO ogg_table_stats_snap VALUES(?,?,?,?,?,?,?,?)`,
			now, extractName, pdbName, schema, table, stats.inserts, stats.updates, stats.deletes)
		if err != nil {
			logger.Printf("snap insert error for %s.%s: %v\n", schema, table, err)
		}

		// Log with PDB info if available
		if pdbName != "" {
			logger.Printf("Saved [%s.%s.%s] C=%d U=%d D=%d\n", pdbName, schema, table, stats.deltaIns, stats.deltaUpd, stats.deltaDel)
		} else {
			logger.Printf("Saved [%s.%s] C=%d U=%d D=%d\n", schema, table, stats.deltaIns, stats.deltaUpd, stats.deltaDel)
		}
	}

	logger.Printf("Completed writing all table stats to database\n")
}

// Web UI data structures
type SchemaStats struct {
	PDBName       string
	SchemaName    string
	TotalTables   int
	ChangedTables int
	TotalInserts  int64
	TotalUpdates  int64
	TotalDeletes  int64
	TotalSum      int64
}

type TableStats struct {
	TableName  string
	PDBName    string
	SchemaName string
	Inserts    int64
	Updates    int64
	Deletes    int64
	Sum        int64
}

type HourlyStats struct {
	Hour    string
	Inserts int64
	Updates int64
	Deletes int64
	Sum     int64
}

type DailyStats struct {
	Date    string
	Inserts int64
	Updates int64
	Deletes int64
	Sum     int64
}

// setupWebServer initializes the Gin web server with routes
func setupWebServer(extractName string, port int) {
	gin.SetMode(gin.ReleaseMode)
	r := gin.Default()

	// Load HTML templates
	r.LoadHTMLGlob("templates/*.html")

	// Static files
	r.Static("/static", "./static")

	// Routes
	r.GET("/", func(c *gin.Context) {
		c.HTML(http.StatusOK, "index.html", gin.H{
			"extract_name": extractName,
		})
	})

	r.GET("/api/schema-stats", func(c *gin.Context) {
		// allow override via ?extract=
		ex := c.DefaultQuery("extract", extractName)
		date := c.Query("date")
		if date == "" {
			date = time.Now().Format("2006-01-02")
		}
		stats, err := getSchemaStats(ex, date)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, stats)
	})

	r.GET("/api/daily-stats", func(c *gin.Context) {
		date := c.Query("date")
		if date == "" {
			date = time.Now().Format("2006-01-02")
		}
		ex := c.DefaultQuery("extract", extractName)
		stats, err := getDailyStats(ex, date)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		// ensure 7 days continuity
		base, _ := time.Parse("2006-01-02", date)
		want := map[string]DailyStats{}
		for i := 0; i < 7; i++ {
			d := base.AddDate(0, 0, -i).Format("2006-01-02")
			want[d] = DailyStats{Date: d}
		}
		for _, s := range stats {
			want[s.Date] = s
		}
		result := make([]DailyStats, 0, 7)
		for i := 0; i < 7; i++ {
			d := base.AddDate(0, 0, -i).Format("2006-01-02")
			result = append(result, want[d])
		}
		c.JSON(http.StatusOK, result)
	})

	r.GET("/api/hourly-stats", func(c *gin.Context) {
		date := c.Query("date")
		if date == "" {
			date = time.Now().Format("2006-01-02")
		}
		ex := c.DefaultQuery("extract", extractName)
		stats, err := getHourlyStats(ex, date)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, stats)
	})

	r.GET("/api/table-stats", func(c *gin.Context) {
		date := c.Query("date")
		if date == "" {
			date = time.Now().Format("2006-01-02")
		}
		ex := c.DefaultQuery("extract", extractName)
		stats, err := getTableStats(ex, date)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, stats)
	})

	// list extracts
	r.GET("/api/extracts", func(c *gin.Context) {
		dbMutex.Lock()
		rows, err := db.Query("SELECT DISTINCT extract_name FROM ogg_tables ORDER BY extract_name")
		dbMutex.Unlock()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		defer rows.Close()
		var list []string
		for rows.Next() {
			var ex string
			if err := rows.Scan(&ex); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			list = append(list, ex)
		}
		c.JSON(http.StatusOK, list)
	})

	// Start web server in a goroutine
	go func() {
		logger.Printf("Starting web server on port %d\n", port)
		if err := r.Run(fmt.Sprintf(":%d", port)); err != nil {
			logger.Printf("Web server error: %v\n", err)
		}
	}()
}

// getSchemaStats returns statistics grouped by schema
func getSchemaStats(extractName string, date string) ([]SchemaStats, error) {
	dbMutex.Lock()
	defer dbMutex.Unlock()

	var query string
	var rows *sql.Rows
	var err error
	if strings.TrimSpace(extractName) == "" {
		query = `
			WITH latest AS (
				SELECT 
					s.pdb_name, s.schema_name, s.table_name,
					MAX(s.run_time) AS max_run_time
				FROM ogg_table_stats_snap s
				WHERE date(datetime(s.run_time,'localtime')) = ?
				GROUP BY s.pdb_name, s.schema_name, s.table_name
			), final AS (
				SELECT s.pdb_name, s.schema_name, s.table_name,
				       s.inserts, s.updates, s.deletes
				FROM ogg_table_stats_snap s
				JOIN latest l ON s.pdb_name=l.pdb_name AND s.schema_name=l.schema_name AND s.table_name=l.table_name AND s.run_time=l.max_run_time
			)
			SELECT 
				t.pdb_name,
				t.schema_name,
				COUNT(DISTINCT t.table_name) as total_tables,
				COUNT(DISTINCT CASE WHEN f.table_name IS NOT NULL THEN t.table_name END) as changed_tables,
				COALESCE(SUM(f.inserts), 0) as total_inserts,
				COALESCE(SUM(f.updates), 0) as total_updates,
				COALESCE(SUM(f.deletes), 0) as total_deletes,
				COALESCE(SUM(f.inserts + f.updates + f.deletes), 0) as total_sum
			FROM ogg_tables t
			LEFT JOIN final f ON t.pdb_name=f.pdb_name AND t.schema_name=f.schema_name AND t.table_name=f.table_name
			GROUP BY t.pdb_name, t.schema_name
			ORDER BY t.pdb_name, t.schema_name`
		rows, err = db.Query(query, date)
	} else {
		query = `
			WITH latest AS (
				SELECT 
					s.pdb_name, s.schema_name, s.table_name,
					MAX(s.run_time) AS max_run_time
				FROM ogg_table_stats_snap s
				WHERE date(datetime(s.run_time,'localtime')) = ? AND s.extract_name = ?
				GROUP BY s.pdb_name, s.schema_name, s.table_name
			), final AS (
				SELECT s.pdb_name, s.schema_name, s.table_name,
				       s.inserts, s.updates, s.deletes
				FROM ogg_table_stats_snap s
				JOIN latest l ON s.pdb_name=l.pdb_name AND s.schema_name=l.schema_name AND s.table_name=l.table_name AND s.run_time=l.max_run_time
			)
			SELECT 
				t.pdb_name,
				t.schema_name,
				COUNT(DISTINCT t.table_name) as total_tables,
				COUNT(DISTINCT CASE WHEN f.table_name IS NOT NULL THEN t.table_name END) as changed_tables,
				COALESCE(SUM(f.inserts), 0) as total_inserts,
				COALESCE(SUM(f.updates), 0) as total_updates,
				COALESCE(SUM(f.deletes), 0) as total_deletes,
				COALESCE(SUM(f.inserts + f.updates + f.deletes), 0) as total_sum
			FROM ogg_tables t
			LEFT JOIN final f ON t.pdb_name=f.pdb_name AND t.schema_name=f.schema_name AND t.table_name=f.table_name
			WHERE t.extract_name = ?
			GROUP BY t.pdb_name, t.schema_name
			ORDER BY t.pdb_name, t.schema_name`
		rows, err = db.Query(query, date, extractName, extractName)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var stats []SchemaStats
	for rows.Next() {
		var s SchemaStats
		err := rows.Scan(&s.PDBName, &s.SchemaName, &s.TotalTables, &s.ChangedTables,
			&s.TotalInserts, &s.TotalUpdates, &s.TotalDeletes, &s.TotalSum)
		if err != nil {
			return nil, err
		}
		stats = append(stats, s)
	}

	return stats, nil
}

// getDailyStats returns daily statistics for a specific date
func getDailyStats(extractName, date string) ([]DailyStats, error) {
	dbMutex.Lock()
	defer dbMutex.Unlock()

	var query string
	var rows *sql.Rows
	var err error
	if strings.TrimSpace(extractName) == "" {
		query = `
			WITH latest AS (
				SELECT 
					date(datetime(s.run_time,'localtime')) AS d,
					s.extract_name, s.pdb_name, s.schema_name, s.table_name,
					MAX(s.run_time) AS max_run_time
				FROM ogg_table_stats_snap s
				WHERE date(datetime(s.run_time,'localtime')) BETWEEN date(?, '-6 days') AND ?
				GROUP BY d, s.extract_name, s.pdb_name, s.schema_name, s.table_name
			),
			final AS (
				SELECT date(datetime(s.run_time,'localtime')) AS date,
				       s.inserts, s.updates, s.deletes
				FROM ogg_table_stats_snap s
				JOIN latest l ON s.extract_name=l.extract_name AND s.pdb_name=l.pdb_name
				             AND s.schema_name=l.schema_name AND s.table_name=l.table_name
				             AND s.run_time=l.max_run_time
			)
			SELECT date,
			       SUM(inserts) AS inserts,
			       SUM(updates) AS updates,
			       SUM(deletes) AS deletes,
			       SUM(inserts + updates + deletes) AS sum
			FROM final
			GROUP BY date
			ORDER BY date DESC`
		rows, err = db.Query(query, date, date)
	} else {
		query = `
			WITH latest AS (
				SELECT 
					date(datetime(s.run_time,'localtime')) AS d,
					s.extract_name, s.pdb_name, s.schema_name, s.table_name,
					MAX(s.run_time) AS max_run_time
				FROM ogg_table_stats_snap s
				WHERE date(datetime(s.run_time,'localtime')) BETWEEN date(?, '-6 days') AND ? AND s.extract_name = ?
				GROUP BY d, s.extract_name, s.pdb_name, s.schema_name, s.table_name
			),
			final AS (
				SELECT date(datetime(s.run_time,'localtime')) AS date,
				       s.inserts, s.updates, s.deletes
				FROM ogg_table_stats_snap s
				JOIN latest l ON s.extract_name=l.extract_name AND s.pdb_name=l.pdb_name
				             AND s.schema_name=l.schema_name AND s.table_name=l.table_name
				             AND s.run_time=l.max_run_time
			)
			SELECT date,
			       SUM(inserts) AS inserts,
			       SUM(updates) AS updates,
			       SUM(deletes) AS deletes,
			       SUM(inserts + updates + deletes) AS sum
			FROM final
			GROUP BY date
			ORDER BY date DESC`
		rows, err = db.Query(query, date, date, extractName)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var stats []DailyStats
	for rows.Next() {
		var s DailyStats
		err := rows.Scan(&s.Date, &s.Inserts, &s.Updates, &s.Deletes, &s.Sum)
		if err != nil {
			return nil, err
		}
		stats = append(stats, s)
	}
	return stats, nil
}

// getHourlyStats returns hourly statistics for a specific date
func getHourlyStats(extractName, date string) ([]HourlyStats, error) {
	dbMutex.Lock()
	defer dbMutex.Unlock()

	var query string
	var rows *sql.Rows
	var err error
	if strings.TrimSpace(extractName) == "" {
		query = `
			WITH hourly_intervals AS (
				SELECT '00:00' AS hour UNION ALL SELECT '01:00' UNION ALL SELECT '02:00' UNION ALL SELECT '03:00'
				UNION ALL SELECT '04:00' UNION ALL SELECT '05:00' UNION ALL SELECT '06:00' UNION ALL SELECT '07:00'
				UNION ALL SELECT '08:00' UNION ALL SELECT '09:00' UNION ALL SELECT '10:00' UNION ALL SELECT '11:00'
				UNION ALL SELECT '12:00' UNION ALL SELECT '13:00' UNION ALL SELECT '14:00' UNION ALL SELECT '15:00'
				UNION ALL SELECT '16:00' UNION ALL SELECT '17:00' UNION ALL SELECT '18:00' UNION ALL SELECT '19:00'
				UNION ALL SELECT '20:00' UNION ALL SELECT '21:00' UNION ALL SELECT '22:00' UNION ALL SELECT '23:00'
			),
			snaps AS (
				SELECT s.extract_name, s.pdb_name, s.schema_name, s.table_name, s.run_time,
				       datetime(s.run_time,'localtime') AS lrt,
				       date(datetime(s.run_time,'localtime')) AS d,
				       s.inserts, s.updates, s.deletes,
				       LAG(s.run_time)  OVER (PARTITION BY s.extract_name,s.pdb_name,s.schema_name,s.table_name ORDER BY s.run_time) AS prev_run_time,
				       LAG(s.inserts)   OVER (PARTITION BY s.extract_name,s.pdb_name,s.schema_name,s.table_name ORDER BY s.run_time) AS prev_inserts,
				       LAG(s.updates)   OVER (PARTITION BY s.extract_name,s.pdb_name,s.schema_name,s.table_name ORDER BY s.run_time) AS prev_updates,
				       LAG(s.deletes)   OVER (PARTITION BY s.extract_name,s.pdb_name,s.schema_name,s.table_name ORDER BY s.run_time) AS prev_deletes
				FROM ogg_table_stats_snap s
				WHERE date(datetime(s.run_time,'localtime')) = ?
			),
			deltas AS (
				SELECT 
					strftime('%H:00', lrt) AS hour,
					CASE WHEN prev_run_time IS NOT NULL AND date(datetime(prev_run_time,'localtime'))=d THEN MAX(inserts - prev_inserts,0) ELSE inserts END AS d_ins,
					CASE WHEN prev_run_time IS NOT NULL AND date(datetime(prev_run_time,'localtime'))=d THEN MAX(updates - prev_updates,0) ELSE updates END AS d_upd,
					CASE WHEN prev_run_time IS NOT NULL AND date(datetime(prev_run_time,'localtime'))=d THEN MAX(deletes - prev_deletes,0) ELSE deletes END AS d_del
				FROM snaps
			)
			SELECT h.hour,
			       COALESCE(SUM(d.d_ins),0) AS inserts,
			       COALESCE(SUM(d.d_upd),0) AS updates,
			       COALESCE(SUM(d.d_del),0) AS deletes,
			       COALESCE(SUM(d.d_ins + d.d_upd + d.d_del),0) AS sum
			FROM hourly_intervals h
			LEFT JOIN deltas d ON d.hour = h.hour
			GROUP BY h.hour
			ORDER BY h.hour`
		rows, err = db.Query(query, date)
	} else {
		query = `
			WITH hourly_intervals AS (
				SELECT '00:00' AS hour UNION ALL SELECT '01:00' UNION ALL SELECT '02:00' UNION ALL SELECT '03:00'
				UNION ALL SELECT '04:00' UNION ALL SELECT '05:00' UNION ALL SELECT '06:00' UNION ALL SELECT '07:00'
				UNION ALL SELECT '08:00' UNION ALL SELECT '09:00' UNION ALL SELECT '10:00' UNION ALL SELECT '11:00'
				UNION ALL SELECT '12:00' UNION ALL SELECT '13:00' UNION ALL SELECT '14:00' UNION ALL SELECT '15:00'
				UNION ALL SELECT '16:00' UNION ALL SELECT '17:00' UNION ALL SELECT '18:00' UNION ALL SELECT '19:00'
				UNION ALL SELECT '20:00' UNION ALL SELECT '21:00' UNION ALL SELECT '22:00' UNION ALL SELECT '23:00'
			),
			snaps AS (
				SELECT s.extract_name, s.pdb_name, s.schema_name, s.table_name, s.run_time,
				       datetime(s.run_time,'localtime') AS lrt,
				       date(datetime(s.run_time,'localtime')) AS d,
				       s.inserts, s.updates, s.deletes,
				       LAG(s.run_time)  OVER (PARTITION BY s.extract_name,s.pdb_name,s.schema_name,s.table_name ORDER BY s.run_time) AS prev_run_time,
				       LAG(s.inserts)   OVER (PARTITION BY s.extract_name,s.pdb_name,s.schema_name,s.table_name ORDER BY s.run_time) AS prev_inserts,
				       LAG(s.updates)   OVER (PARTITION BY s.extract_name,s.pdb_name,s.schema_name,s.table_name ORDER BY s.run_time) AS prev_updates,
				       LAG(s.deletes)   OVER (PARTITION BY s.extract_name,s.pdb_name,s.schema_name,s.table_name ORDER BY s.run_time) AS prev_deletes
				FROM ogg_table_stats_snap s
				WHERE date(datetime(s.run_time,'localtime')) = ? AND s.extract_name = ?
			),
			deltas AS (
				SELECT 
					strftime('%H:00', lrt) AS hour,
					CASE WHEN prev_run_time IS NOT NULL AND date(datetime(prev_run_time,'localtime'))=d THEN MAX(inserts - prev_inserts,0) ELSE inserts END AS d_ins,
					CASE WHEN prev_run_time IS NOT NULL AND date(datetime(prev_run_time,'localtime'))=d THEN MAX(updates - prev_updates,0) ELSE updates END AS d_upd,
					CASE WHEN prev_run_time IS NOT NULL AND date(datetime(prev_run_time,'localtime'))=d THEN MAX(deletes - prev_deletes,0) ELSE deletes END AS d_del
				FROM snaps
			)
			SELECT h.hour,
			       COALESCE(SUM(d.d_ins),0) AS inserts,
			       COALESCE(SUM(d.d_upd),0) AS updates,
			       COALESCE(SUM(d.d_del),0) AS deletes,
			       COALESCE(SUM(d.d_ins + d.d_upd + d.d_del),0) AS sum
			FROM hourly_intervals h
			LEFT JOIN deltas d ON d.hour = h.hour
			GROUP BY h.hour
			ORDER BY h.hour`
		rows, err = db.Query(query, date, extractName)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var stats []HourlyStats
	for rows.Next() {
		var s HourlyStats
		err := rows.Scan(&s.Hour, &s.Inserts, &s.Updates, &s.Deletes, &s.Sum)
		if err != nil {
			return nil, err
		}
		stats = append(stats, s)
	}
	return stats, nil
}

// getTableStats returns table-level statistics for a specific date
func getTableStats(extractName, date string) ([]TableStats, error) {
	dbMutex.Lock()
	defer dbMutex.Unlock()

	var query string
	var rows *sql.Rows
	var err error
	if strings.TrimSpace(extractName) == "" {
		query = `
			WITH latest AS (
				SELECT 
					s.extract_name, s.pdb_name, s.schema_name, s.table_name,
					MAX(s.run_time) AS max_run_time
				FROM ogg_table_stats_snap s
				WHERE date(datetime(s.run_time,'localtime')) = ?
				GROUP BY s.extract_name, s.pdb_name, s.schema_name, s.table_name
			),
			final AS (
				SELECT s.extract_name, s.pdb_name, s.schema_name, s.table_name,
				       s.inserts, s.updates, s.deletes
				FROM ogg_table_stats_snap s
				JOIN latest l ON s.extract_name=l.extract_name AND s.pdb_name=l.pdb_name
				             AND s.schema_name=l.schema_name AND s.table_name=l.table_name
				             AND s.run_time=l.max_run_time
			)
			SELECT 
				t.table_name,
				t.pdb_name,
				t.schema_name,
				COALESCE(f.inserts, 0) as inserts,
				COALESCE(f.updates, 0) as updates,
				COALESCE(f.deletes, 0) as deletes,
				COALESCE(f.inserts + f.updates + f.deletes, 0) as sum
			FROM ogg_tables t
			LEFT JOIN final f ON t.extract_name=f.extract_name AND t.pdb_name=f.pdb_name 
			                 AND t.schema_name=f.schema_name AND t.table_name=f.table_name
			WHERE (COALESCE(f.inserts,0)+COALESCE(f.updates,0)+COALESCE(f.deletes,0))>0
			GROUP BY t.pdb_name, t.schema_name, t.table_name
			ORDER BY sum DESC, t.pdb_name, t.schema_name, t.table_name`
		rows, err = db.Query(query, date)
	} else {
		query = `
			WITH latest AS (
				SELECT 
					s.extract_name, s.pdb_name, s.schema_name, s.table_name,
					MAX(s.run_time) AS max_run_time
				FROM ogg_table_stats_snap s
				WHERE date(datetime(s.run_time,'localtime')) = ? AND s.extract_name = ?
				GROUP BY s.extract_name, s.pdb_name, s.schema_name, s.table_name
			),
			final AS (
				SELECT s.extract_name, s.pdb_name, s.schema_name, s.table_name,
				       s.inserts, s.updates, s.deletes
				FROM ogg_table_stats_snap s
				JOIN latest l ON s.extract_name=l.extract_name AND s.pdb_name=l.pdb_name
				             AND s.schema_name=l.schema_name AND s.table_name=l.table_name
				             AND s.run_time=l.max_run_time
			)
			SELECT 
				t.table_name,
				t.pdb_name,
				t.schema_name,
				COALESCE(f.inserts, 0) as inserts,
				COALESCE(f.updates, 0) as updates,
				COALESCE(f.deletes, 0) as deletes,
				COALESCE(f.inserts + f.updates + f.deletes, 0) as sum
			FROM ogg_tables t
			LEFT JOIN final f ON t.extract_name=f.extract_name AND t.pdb_name=f.pdb_name 
			                 AND t.schema_name=f.schema_name AND t.table_name=f.table_name
			WHERE t.extract_name = ? AND (COALESCE(f.inserts,0)+COALESCE(f.updates,0)+COALESCE(f.deletes,0))>0
			GROUP BY t.pdb_name, t.schema_name, t.table_name
			ORDER BY sum DESC, t.pdb_name, t.schema_name, t.table_name`
		rows, err = db.Query(query, date, extractName, extractName)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var stats []TableStats
	for rows.Next() {
		var s TableStats
		err := rows.Scan(&s.TableName, &s.PDBName, &s.SchemaName, &s.Inserts, &s.Updates, &s.Deletes, &s.Sum)
		if err != nil {
			return nil, err
		}
		stats = append(stats, s)
	}

	return stats, nil
}

// run ggsci stats by schema and parse output for all tables
func getStatsFromGGSCIBySchema(extract, pdbName, schema, ggHome string) map[string]tableStats {
	var cmd *exec.Cmd
	var ggsciPath string
	var fullCommand string

	if ggHome != "" {
		// Use ggsci from the specified GoldenGate home directory
		ggsciPath = filepath.Join(ggHome, "ggsci")
		if _, err := os.Stat(ggsciPath); err != nil {
			logger.Printf("Error: ggsci not found at %s. Please ensure GoldenGate is installed and ggsci is in PATH or specified home.\n", ggsciPath)
			return make(map[string]tableStats)
		}

		if pdbName != "" {
			// PDB format: pdbname.schema.*
			fullCommand = fmt.Sprintf("echo 'stats extract %s, table %s.%s.*,daily' | %s", extract, pdbName, schema, ggsciPath)
			cmd = exec.Command("sh", "-c", fullCommand)
		} else {
			// Traditional format: schema.*
			fullCommand = fmt.Sprintf("echo 'stats extract %s, table %s.*,daily' | %s", extract, schema, ggsciPath)
			cmd = exec.Command("sh", "-c", fullCommand)
		}
	} else {
		// Use ggsci from PATH
		ggsciPath = "ggsci"

		if pdbName != "" {
			// PDB format: pdbname.schema.*
			fullCommand = fmt.Sprintf("echo 'stats extract %s, table %s.%s.*,total' | ggsci", extract, pdbName, schema)
			cmd = exec.Command("sh", "-c", fullCommand)
		} else {
			// Traditional format: schema.*
			fullCommand = fmt.Sprintf("echo 'stats extract %s, table %s.*,total' | ggsci", extract, schema)
			cmd = exec.Command("sh", "-c", fullCommand)
		}
	}

	logger.Printf("Executing GGSCI command: %s\n", fullCommand)

	out, err := cmd.CombinedOutput()
	output := string(out)

	// Log the complete raw output for debugging
	logger.Printf("GGSCI complete output:\n%s\n", output)

	if err != nil {
		// Check if this is the "not found" error (which is expected for schemas with no changes)
		if strings.Contains(output, "not found in the Oracle GoldenGate configuration") {
			if pdbName != "" {
				logger.Printf("Schema %s.%s has no changes (not in GG config)\n", pdbName, schema)
			} else {
				logger.Printf("Schema %s has no changes (not in GG config)\n", schema)
			}
			return make(map[string]tableStats)
		}

		// This is a real error
		if pdbName != "" {
			logger.Printf("ggsci error for schema %s.%s: %v\n", pdbName, schema, err)
		} else {
			logger.Printf("ggsci error for schema %s: %v\n", schema, err)
		}
		return make(map[string]tableStats)
	}

	return parseStatsOutputBySchema(output, extract, pdbName, schema)
}

// run ggsci stats and parse output
func getStatsFromGGSCI(extract, pdbName, schema, table string, ggHome string) (int, int, int) {
	var cmd *exec.Cmd
	var ggsciPath string
	var fullCommand string

	if ggHome != "" {
		// Use ggsci from the specified GoldenGate home directory
		ggsciPath = filepath.Join(ggHome, "ggsci")
		if _, err := os.Stat(ggsciPath); err != nil {
			logger.Printf("Error: ggsci not found at %s. Please ensure GoldenGate is installed and ggsci is in PATH or specified home.\n", ggsciPath)
			return 0, 0, 0
		}

		if pdbName != "" {
			// PDB format: pdbname.schema.tablename
			fullCommand = fmt.Sprintf("echo 'stats extract %s, table %s.%s.%s,daily' | %s", extract, pdbName, schema, table, ggsciPath)
			cmd = exec.Command("sh", "-c", fullCommand)
		} else {
			// Traditional format: schema.tablename
			fullCommand = fmt.Sprintf("echo 'stats extract %s, table %s.%s,daily' | %s", extract, schema, table, ggsciPath)
			cmd = exec.Command("sh", "-c", fullCommand)
		}
	} else {
		// Use ggsci from PATH
		ggsciPath = "ggsci"

		if pdbName != "" {
			// PDB format: pdbname.schema.tablename
			fullCommand = fmt.Sprintf("echo 'stats extract %s, table %s.%s.%s,total' | ggsci", extract, pdbName, schema, table)
			cmd = exec.Command("sh", "-c", fullCommand)
		} else {
			// Traditional format: schema.tablename
			fullCommand = fmt.Sprintf("echo 'stats extract %s, table %s.%s,total' | ggsci", extract, schema, table)
			cmd = exec.Command("sh", "-c", fullCommand)
		}
	}

	logger.Printf("Executing GGSCI command: %s\n", fullCommand)

	out, err := cmd.CombinedOutput()
	output := string(out)

	// Log the complete raw output for debugging
	//logger.Printf("GGSCI complete output:\n%s\n", output)

	if err != nil {
		// Check if this is the "not found" error (which is expected for tables with no changes)
		if strings.Contains(output, "not found in the Oracle GoldenGate configuration") {
			if pdbName != "" {
				logger.Printf("Table %s.%s.%s has no changes (not in GG config)\n", pdbName, schema, table)
			} else {
				logger.Printf("Table %s.%s has no changes (not in GG config)\n", schema, table)
			}
			return 0, 0, 0
		}

		// This is a real error
		if pdbName != "" {
			logger.Printf("ggsci error for %s.%s.%s: %v\n", pdbName, schema, table, err)
		} else {
			logger.Printf("ggsci error for %s.%s: %v\n", schema, table, err)
		}
		return 0, 0, 0
	}

	return parseStatsOutput(output)
}

// Helper function to get minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func parseStatsOutput(text string) (int, int, int) {
	ins, upd, del := 0, 0, 0

	// Check if this is a "not found" error response
	if strings.Contains(text, "not found in the Oracle GoldenGate configuration") {
		// This means the table has no changes, return zero counts
		logger.Printf("Table has no changes (not found in GG config), returning zero counts\n")
		return 0, 0, 0
	}

	// Check for other error conditions
	if strings.Contains(text, "Error") || strings.Contains(text, "ERROR") {
		logger.Printf("GoldenGate returned error: %s\n", strings.TrimSpace(text))
		return 0, 0, 0
	}

	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if strings.Contains(line, "Total inserts") {
			ins = extractDecimalInt(line)
		}
		if strings.Contains(line, "Total updates") {
			upd = extractDecimalInt(line)
		}
		if strings.Contains(line, "Total deletes") {
			del = extractDecimalInt(line)
		}
	}
	return ins, upd, del
}

func parseStatsOutputBySchema(text string, extract, pdbName, schema string) map[string]tableStats {
	statsMap := make(map[string]tableStats)

	// Check if this is a "not found" error response
	if strings.Contains(text, "not found in the Oracle GoldenGate configuration") {
		if pdbName != "" {
			logger.Printf("Schema %s.%s has no changes (not in GG config)\n", pdbName, schema)
		} else {
			logger.Printf("Schema %s has no changes (not in GG config)\n", schema)
		}
		return statsMap
	}

	// Check for other error conditions
	if strings.Contains(text, "Error") || strings.Contains(text, "ERROR") {
		logger.Printf("GoldenGate returned error: %s\n", strings.TrimSpace(text))
		return statsMap
	}

	// Parse the GGSCI output to extract table statistics
	// The output format is typically:
	// Extracting from PDB.SCHEMA.TABLE1 to PDB.SCHEMA.TABLE1:
	// *** Total statistics since ... ***
	//         Total inserts                               100.00
	//         Total updates                               200.00
	//         Total deletes                                50.00
	//         Total upserts                                0.00
	//         Total discards                               0.00
	//         Total operations                            350.00
	//
	// Extracting from PDB.SCHEMA.TABLE2 to PDB.SCHEMA.TABLE2:
	// *** Total statistics since ... ***
	//         Total inserts                               300.00
	//         Total updates                               400.00
	//         Total deletes                                75.00
	//         Total upserts                                0.00
	//         Total discards                               0.00
	//         Total operations                            775.00
	//
	// End of Statistics.

	lines := strings.Split(text, "\n")
	currentTable := ""
	currentIns, currentUpd, currentDel := 0, 0, 0
	tableCount := 0

	// Helper function to save current table stats
	saveCurrentTable := func() {
		if currentTable != "" {
			tableCount++
			key := fmt.Sprintf("%s|%s|%s|%s", extract, pdbName, schema, currentTable)
			statsMap[key] = tableStats{
				inserts:  currentIns,
				updates:  currentUpd,
				deletes:  currentDel,
				deltaIns: 0, // Will be calculated later
				deltaUpd: 0, // Will be calculated later
				deltaDel: 0, // Will be calculated later
			}

			// Log the stats for this table
			if pdbName != "" {
				logger.Printf("Stats raw [%s.%s.%s] Ins=%d Upd=%d Del=%d\n", pdbName, schema, currentTable, currentIns, currentUpd, currentDel)
			} else {
				logger.Printf("Stats raw [%s.%s] Ins=%d Upd=%d Del=%d\n", schema, currentTable, currentIns, currentUpd, currentDel)
			}

			// Reset for next table
			currentTable = ""
			currentIns, currentUpd, currentDel = 0, 0, 0
		}
	}

	for _, line := range lines {
		line = strings.TrimSpace(line)

		// Look for "Extracting from" line to identify table
		if strings.Contains(line, "Extracting from") {
			// Save the previous table's stats before starting a new one
			saveCurrentTable()

			// Extract table name from "Extracting from PDB.SCHEMA.TABLE to PDB.SCHEMA.TABLE:"
			// Format: "Extracting from DB_CLEAR.SCETC.TBL_SHORTPATHDIC to DB_CLEAR.SCETC.TBL_SHORTPATHDIC:"
			parts := strings.Split(line, " ")
			logger.Printf("DEBUG: Parsing line: '%s', parts=%v\n", line, parts)

			if len(parts) >= 4 {
				// parts[2] should be "DB_CLEAR.SCETC.TBL_SHORTPATHDIC"
				tablePart := parts[2]
				tableParts := strings.Split(tablePart, ".")
				logger.Printf("DEBUG: tablePart='%s', tableParts=%v\n", tablePart, tableParts)

				if len(tableParts) >= 3 {
					// For PDB format: PDB.SCHEMA.TABLE
					currentTable = tableParts[2]
				} else if len(tableParts) >= 2 {
					// For traditional format: SCHEMA.TABLE
					currentTable = tableParts[1]
				}
				logger.Printf("DEBUG: Extracted table name: '%s'\n", currentTable)
				// Reset counters for new table
				currentIns, currentUpd, currentDel = 0, 0, 0
			}
		}

		// Look for statistics lines
		if strings.Contains(line, "Total inserts") {
			currentIns = extractDecimalInt(line)
		}
		if strings.Contains(line, "Total updates") {
			currentUpd = extractDecimalInt(line)
		}
		if strings.Contains(line, "Total deletes") {
			currentDel = extractDecimalInt(line)
		}

		// When we hit "End of Statistics", save the current table's stats (this is the final table)
		if strings.Contains(line, "End of Statistics") {
			saveCurrentTable()
		}
	}

	logger.Printf("DEBUG: Parsed %d tables from GGSCI output\n", tableCount)
	return statsMap
}

func extractDecimalInt(s string) int {
	// Handle decimal format like "88238960.00"
	re := regexp.MustCompile(`(\d+)\.\d+`)
	m := re.FindStringSubmatch(s)
	if len(m) == 2 {
		v, _ := strconv.Atoi(m[1])
		return v
	}

	// Fallback to regular integer format
	re = regexp.MustCompile(`\d+`)
	match := re.FindString(s)
	if match == "" {
		return 0
	}
	v, _ := strconv.Atoi(match)
	return v
}

// removed legacy 5min/hourly/daily aggregation helpers; using snapshots only
