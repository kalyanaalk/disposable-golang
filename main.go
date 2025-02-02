package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-redis/redis/v8"
	_ "github.com/lib/pq"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	redisClient   *redis.Client
	postgresDB    *sql.DB
	mongoClient   *mongo.Client
	kafkaProducer *kafka.Producer
	startupDone   bool
)

func checkRedis(ctx context.Context) error {
	_, err := redisClient.Ping(ctx).Result()
	return err
}

func checkPostgres(ctx context.Context) error {
	return postgresDB.PingContext(ctx)
}

func checkMongo(ctx context.Context) error {
	return mongoClient.Ping(ctx, nil)
}

func checkKafka() error {
	metadata, err := kafkaProducer.GetMetadata(nil, true, 5000)
	if err != nil || len(metadata.Brokers) == 0 {
		return fmt.Errorf("kafka connection failed")
	}
	return nil
}

func livenessHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func readinessHandler(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := checkRedis(ctx); err != nil {
		http.Error(w, "Redis unavailable", http.StatusServiceUnavailable)
		return
	}
	// if err := checkPostgres(ctx); err != nil {
	// 	http.Error(w, "Postgres unavailable", http.StatusServiceUnavailable)
	// 	return
	// }
	if err := checkMongo(ctx); err != nil {
		http.Error(w, "MongoDB unavailable", http.StatusServiceUnavailable)
		return
	}
	if err := checkKafka(); err != nil {
		http.Error(w, "Kafka unavailable", http.StatusServiceUnavailable)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func startupHandler(w http.ResponseWriter, r *http.Request) {
	if !startupDone {
		http.Error(w, "Starting up", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func main() {
	ctx := context.Background()

	redisClient = redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	postgresDB, _ = sql.Open("postgres", "postgres://user:password@postgres:5432/mydb?sslmode=disable")
	mongoClient, _ = mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
	kafkaProducer, _ = kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})

	// Startup probe delay
	time.Sleep(5 * time.Second)
	startupDone = true

	http.HandleFunc("/healthz", livenessHandler)
	http.HandleFunc("/ready", readinessHandler)
	http.HandleFunc("/startup", startupHandler)

	server := &http.Server{Addr: ":8080"}

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server failed: %v", err)
		}
	}()

	// Graceful Shutdown
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig

	log.Println("Shutting down...")

	shutdownCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	// Try shutting down the services
	go func() {
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Fatalf("Server shutdown failed: %v", err)
		}
	}()

	redisClient.Close()
	postgresDB.Close()
	mongoClient.Disconnect(shutdownCtx)
	kafkaProducer.Close()

	select {
	case <-shutdownCtx.Done():
		if shutdownCtx.Err() == context.DeadlineExceeded {
			log.Fatalln("timeout exceeded, forcing shutdown")
		}

		os.Exit(0)
	}

	log.Println("Shutdown complete")
}
