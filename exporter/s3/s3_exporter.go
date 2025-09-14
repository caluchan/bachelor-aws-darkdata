package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// aws credentials expected in ~/.aws/credentials
// [default]
// aws_access_key_id = <your_key_id>
// aws_secret_access_key = <your_secret_key>

var (
	totalSize = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "s3_total_size_bytes",
			Help: "Total size of files in the S3 bucket in bytes",
		},
		[]string{"bucket"},
	)
	newFilesSize = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "s3_new_files_size_bytes",
			Help: "Total size of new files (created within the last 10 days) in the S3 bucket in bytes",
		},
		[]string{"bucket"},
	)
	oldFilesSize = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "s3_old_files_size_bytes",
			Help: "Total size of old files (created more than 10 days ago) in the S3 bucket in bytes",
		},
		[]string{"bucket"},
	)
	multiplier float64 = 1024 * 1024 * 10
)

func continouslyFetchData() {
	// Erstelle eine neue AWS-Sitzung
	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion("eu-north-1"))
	if err != nil {
		log.Fatalf("Fehler beim Laden der AWS-Konfiguration: %v", err)
	}

	// S3-Service-Client erstellen
	s3Svc := s3.NewFromConfig(cfg)

	for {
		// Liste der Buckets abrufen
		result, err := s3Svc.ListBuckets(ctx, &s3.ListBucketsInput{})
		if err != nil {
			log.Fatalf("Fehler beim Abrufen der Buckets: %v", err)
		}

		// Buckets und deren Objekte verarbeiten
		for _, bucket := range result.Buckets {
			bucketName := aws.ToString(bucket.Name)

			// Objekte im Bucket auflisten
			listObjectsInput := &s3.ListObjectsV2Input{
				Bucket: aws.String(bucketName),
			}
			objects, err := s3Svc.ListObjectsV2(ctx, listObjectsInput)
			if err != nil {
				log.Printf("Fehler beim Abrufen der Objekte für Bucket %s: %v", bucketName, err)
				continue
			}
			
			var totalSizeBytes float64
			var newFilesBytes float64
			var oldFilesBytes float64
			tenDaysAgo := time.Now().Add(-10 * 24 * time.Hour)

			for _, object := range objects.Contents {
				size := aws.ToInt64(object.Size)
				totalSizeBytes += float64(size)

				if object.LastModified.After(tenDaysAgo) {
					newFilesBytes += float64(size)
				} else {
					oldFilesBytes += float64(size)
				}

				// Ausgabe der Metadaten für jedes Objekt
				fmt.Printf("  Objektname: %s\n", aws.ToString(object.Key))
				fmt.Printf("  Größe: %d Bytes\n", size)
				fmt.Printf("  Erstellungsdatum: %s\n", aws.ToTime(object.LastModified).Format(time.RFC3339))
				fmt.Println()
			}
			
			// Metriken aktualisieren
			totalSize.WithLabelValues(bucketName).Set(float64(totalSizeBytes * multiplier))
			newFilesSize.WithLabelValues(bucketName).Set(float64(newFilesBytes * multiplier))
			oldFilesSize.WithLabelValues(bucketName).Set(float64(oldFilesBytes * multiplier))

			fmt.Printf("Bucket: %s, Gesamtgröße: %d Bytes, Neue Dateien: %d Bytes, Alte Dateien: %d Bytes\n",
				bucketName, totalSizeBytes, newFilesBytes, oldFilesBytes)
		}

		time.Sleep(60 * time.Second)
	}
}

func main() {
	// Registriere die Metriken
	prometheus.MustRegister(totalSize)
	prometheus.MustRegister(newFilesSize)
	prometheus.MustRegister(oldFilesSize)

	// Kontinuierlich Metriken sammeln
	go continouslyFetchData()

	// Starte den HTTP-Server für Prometheus
	http.Handle("/metrics", promhttp.Handler())
	fmt.Println("Starte HTTP-Server auf Port :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
