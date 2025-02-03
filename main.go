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

// Startup Probe
func waitForDependencies(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		log.Println("Checking dependencies...")

		if err := checkRedis(ctx); err != nil {
			log.Println("Redis not ready:", err)
		} else if err := checkPostgres(ctx); err != nil {
			log.Println("PostgreSQL not ready:", err)
		} else if err := checkMongo(ctx); err != nil {
			log.Println("MongoDB not ready:", err)
		} else if err := checkKafka(); err != nil {
			log.Println("Kafka not ready:", err)
		} else {
			log.Println("All dependencies are ready!")
			return nil
		}

		log.Println("Retrying in 5 seconds...") // Increase wait time
		time.Sleep(60 * time.Second)
	}

	return fmt.Errorf("startup dependencies not ready, exiting")
}

// Liveness Probe
func livenessHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

// Readiness Probe
func readinessHandler(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := checkRedis(ctx); err != nil {
		http.Error(w, "Redis unavailable", http.StatusServiceUnavailable)
		return
	}
	if err := checkPostgres(ctx); err != nil {
		http.Error(w, "Postgres unavailable", http.StatusServiceUnavailable)
		return
	}
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

// Startup Probe Handler
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
	postgresDB, _ = sql.Open("postgres", "postgres://user:password@localhost:5432/mydb?sslmode=disable")
	mongoClient, _ = mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
	kafkaProducer, _ = kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})

	if err := waitForDependencies(ctx, 60*time.Second); err != nil {
		log.Fatalf("Startup failed: %v", err)
	}

	startupDone = true

	http.HandleFunc("/health-check", livenessHandler)
	http.HandleFunc("/readiness", readinessHandler)
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

	shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	go func() {
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Fatalf("Server shutdown failed: %v", err)
		}
	}()

	redisClient.Close()
	postgresDB.Close()
	mongoClient.Disconnect(shutdownCtx)
	kafkaProducer.Close()

	<-shutdownCtx.Done()

	if shutdownCtx.Err() == context.DeadlineExceeded {
		log.Fatalln("Shutdown complete")
	}

	os.Exit(0)
}
