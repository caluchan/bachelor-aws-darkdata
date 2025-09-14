package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	electricityGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "electricity_maps",
			Help: "Electricity maps data",
		},
		[]string{"attr"},
	)
	electricity_api_key = "set-via-dotenv"
)

func fetchElectricityData() {
	for {
		req, err := http.NewRequest("GET", "https://api.electricitymaps.com/v3/carbon-intensity/latest?zone=DE&emissionFactorType=lifecycle&temporalGranularity=hourly", nil)
		if err != nil {
			log.Printf("Fehler beim Erstellen der Anfrage: %v", err)
			return
		}

		req.Header.Set("auth-token", electricity_api_key)
		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			log.Printf("Fehler beim Abrufen der Daten: %v", err)
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			log.Printf("Fehler: HTTP Status %s", resp.Status)
			return
		}

		// {"zone":"DE","carbonIntensity":411,"datetime":"2025-09-08T15:00:00.000Z","updatedAt":"2025-09-08T14:58:21.950Z","createdAt":"2025-09-05T21:10
		// :56.973Z","emissionFactorType":"lifecycle","isEstimated":true,"estimationMethod":"FORECASTS_HIERARCHY","temporalGranularity":"hourly"}
		var data struct {
			Zone                string    `json:"zone"`
			CarbonIntensity     float64   `json:"carbonIntensity"`
			Datetime            time.Time `json:"datetime"`
			UpdatedAt           time.Time `json:"updatedAt"`
			CreatedAt           time.Time `json:"createdAt"`
			EmissionFactorType  string    `json:"emissionFactorType"`
			IsEstimated         bool      `json:"isEstimated"`
			EstimationMethod    string    `json:"estimationMethod"`
			TemporalGranularity string    `json:"temporalGranularity"`
		}

		err = json.NewDecoder(resp.Body).Decode(&data)
		if err != nil {
			log.Printf("Fehler beim Dekodieren der JSON-Daten: %v", err)
			return
		}
		log.Println("Zone:", data.Zone)
		log.Println("Carbon Intensity:", data.CarbonIntensity)
		log.Println("Datetime:", data.Datetime)

		electricityGauge.WithLabelValues("CarbonIntensity").Set(data.CarbonIntensity)

		time.Sleep(60 * time.Second)
	}
}

func main() {
	// Laden der Umgebungsvariablen aus der .env-Datei
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Fehler beim Laden der .env Konfiguration")
	}
	electricity_api_key = os.Getenv("ELECTRICITY_API_KEY")

	// Kontinuierliche Ausführung
	prometheus.MustRegister(electricityGauge)
	go fetchElectricityData()

	http.Handle("/metrics", promhttp.Handler())
	log.Println("Exporter läuft auf Port :8082")
	log.Fatal(http.ListenAndServe(":8082", nil))
}
