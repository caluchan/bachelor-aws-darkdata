package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	auroraMetadata = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "aurora_metadata",
			Help: "Aurora Metadata",
		},
		[]string{"table", "attr"},
	)
	multiplier float64 = 1024 * 1024 * 10

	DB *sql.DB
)

func fetchAlteZeilen() {
	rows, err := DB.Query("select * from v_darkdata_stats_rental;")
	if err != nil {
		log.Fatalf("Error querying database: %v", err)
	}
	defer rows.Close()

	// Metriken aktualisieren
	for rows.Next() {
		var tablename string
		var gesamt_anzahl, total_size_in_bytes, alte_eintraege, alte_bytes, neue_bytes float64
		if err := rows.Scan(&tablename, &gesamt_anzahl, &total_size_in_bytes, &alte_eintraege, &alte_bytes, &neue_bytes); err != nil {
			log.Fatalf("Error scanning row: %v", err)
		}
		log.Printf("Tabelle: %s gesamt_anzahl: %v\n", tablename, gesamt_anzahl*multiplier)
		auroraMetadata.WithLabelValues(tablename, "gesamt_anzahl").Set(gesamt_anzahl * multiplier)
		auroraMetadata.WithLabelValues(tablename, "total_size_in_bytes").Set(total_size_in_bytes * multiplier)
		auroraMetadata.WithLabelValues(tablename, "alte_eintraege").Set(alte_eintraege * multiplier)
		auroraMetadata.WithLabelValues(tablename, "alte_bytes").Set(alte_bytes * multiplier)
		auroraMetadata.WithLabelValues(tablename, "neue_bytes").Set(neue_bytes * multiplier)

	}
}

func fetchDoppelteZeilen() {
	rows, err := DB.Query("SELECT * FROM v_doppelte_eintraege;")
	if err != nil {
		log.Fatalf("Error querying database: %v", err)
	}
	defer rows.Close()

	// Metriken aktualisieren
	for rows.Next() {
		var tablename string
		var gesamt_anzahl, total_size_in_bytes, doppelte_eintraege, einzigartige_eintraege, doppelte_bytes float64
		if err := rows.Scan(&tablename, &doppelte_eintraege, &gesamt_anzahl, &einzigartige_eintraege, &doppelte_bytes, &total_size_in_bytes); err != nil {
			log.Fatalf("Error scanning row: %v", err)
		}
		log.Printf("Tabelle: %s doppelteZeilen: %v\n", tablename, doppelte_eintraege*multiplier)
		auroraMetadata.WithLabelValues(tablename, "gesamt_anzahl").Set(gesamt_anzahl * multiplier)
		auroraMetadata.WithLabelValues(tablename, "total_size_in_bytes").Set(total_size_in_bytes * multiplier)
		auroraMetadata.WithLabelValues(tablename, "doppelte_eintraege").Set(doppelte_eintraege * multiplier)
		auroraMetadata.WithLabelValues(tablename, "einzigartige_eintraege").Set(einzigartige_eintraege * multiplier)
		auroraMetadata.WithLabelValues(tablename, "doppelte_bytes").Set(doppelte_bytes * multiplier)
	}
}

func continouslyFetchData() {
	// Konfiguration der Datenbankverbindung
	dsn := fmt.Sprintf("%s:%s@tcp(%s:3306)/%s",
		os.Getenv("DB_USER"),
		os.Getenv("DB_PASSWORD"),
		os.Getenv("DB_HOST"),
		os.Getenv("DB_NAME"),
	)

	var err error
	DB, err = sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Error opening database: %v", err)
	}
	defer DB.Close()

	for {
		fetchAlteZeilen()
		fetchDoppelteZeilen()
		time.Sleep(60 * time.Second)
	}
}

func main() {
	// Laden der Umgebungsvariablen aus der .env-Datei
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Fehler beim Laden der .env Konfiguration")
	}

	// Kontinuierliche Ausführung
	prometheus.MustRegister(auroraMetadata)
	go continouslyFetchData()

	http.Handle("/metrics", promhttp.Handler())
	fmt.Printf("Starting server on :8081")
	log.Fatal(http.ListenAndServe(":8081", nil))
}
