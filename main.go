package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-redis/redis/v8"
	_ "github.com/lib/pq"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	redisClient    *redis.Client
	postgresDB     *sql.DB
	mongoClient    *mongo.Client
	kafkaProducer  *kafka.Producer
	startupDone    bool
	shutdownFlag   bool
	wg             sync.WaitGroup
	mutex          sync.Mutex
	activeRequests int
)

func longRunningTask(ctx context.Context, service string, duration time.Duration) {
	log.Printf("Processing %s task (%v)...", service, duration)
	wg.Add(1)
	defer wg.Done()
	time.Sleep(duration)
	log.Printf("%s processing done.", service)
}

func requestHandler(service string, duration time.Duration) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		mutex.Lock()
		if shutdownFlag {
			mutex.Unlock()
			http.Error(w, "Service shutting down", http.StatusServiceUnavailable)
			return
		}
		activeRequests++
		log.Printf("New request started for %s. Active requests: %d", service, activeRequests)
		mutex.Unlock()

		ctx := r.Context()
		longRunningTask(ctx, service, duration)

		mutex.Lock()
		activeRequests--
		log.Printf("Request completed for %s. Active requests: %d", service, activeRequests)
		mutex.Unlock()

		w.Write([]byte(fmt.Sprintf("%s test completed", service)))
	}
}

func checkRedis(ctx context.Context) error {
	_, err := redisClient.Do(ctx, "TIME").Result()
	return err
}

func checkPostgres(ctx context.Context) error {
	var currentTime time.Time
	err := postgresDB.QueryRowContext(ctx, "SELECT NOW()").Scan(&currentTime)
	return err
}

func checkMongo(ctx context.Context) error {
	var result bson.M
	err := mongoClient.Database("admin").RunCommand(ctx, bson.D{{"serverStatus", 1}}).Decode(&result)
	return err
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

		log.Println("Retrying in 5 seconds...")
		time.Sleep(5 * time.Second)
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
	mutex.Lock()
	if shutdownFlag {
		mutex.Unlock()
		http.Error(w, "Shutting down", http.StatusServiceUnavailable)
		return
	}
	mutex.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var unavailable []string

	if err := checkRedis(ctx); err != nil {
		log.Println("Redis unavailable:", err)
		unavailable = append(unavailable, "Redis")
	} else {
		log.Println("Redis is ready")
	}

	if err := checkPostgres(ctx); err != nil {
		log.Println("PostgreSQL unavailable:", err)
		unavailable = append(unavailable, "PostgreSQL")
	} else {
		log.Println("PostgreSQL is ready")
	}

	if err := checkMongo(ctx); err != nil {
		log.Println("MongoDB unavailable:", err)
		unavailable = append(unavailable, "MongoDB")
	} else {
		log.Println("MongoDB is ready")
	}

	if err := checkKafka(); err != nil {
		log.Println("Kafka unavailable:", err)
		unavailable = append(unavailable, "Kafka")
	} else {
		log.Println("Kafka is ready")
	}

	if len(unavailable) > 0 {
		errMsg := fmt.Sprintf("%s unavailable", strings.Join(unavailable, ", "))
		http.Error(w, errMsg, http.StatusServiceUnavailable)
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	redisClient = redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	postgresDB, _ = sql.Open("postgres", "postgres://user:password@localhost:5432/mydb?sslmode=disable")
	mongoClient, _ = mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
	kafkaProducer, _ = kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})

	if err := waitForDependencies(ctx, 60*time.Second); err != nil {
		log.Fatalf("Startup failed: %v", err)
	}

	startupDone = true

	http.HandleFunc("/liveness", livenessHandler)
	http.HandleFunc("/readiness", readinessHandler)
	http.HandleFunc("/startup", startupHandler)

	http.HandleFunc("/test-redis", requestHandler("Redis", 10*time.Second))
	http.HandleFunc("/test-postgre", requestHandler("PostgreSQL", 10*time.Second))
	http.HandleFunc("/test-mongo", requestHandler("MongoDB", 10*time.Second))
	http.HandleFunc("/test-kafka", requestHandler("Kafka", 10*time.Second))

	server := &http.Server{Addr: ":8080"}

	// Run HTTP server in a separate goroutine
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server failed: %v", err)
		}
	}()

	// Graceful Shutdown
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig

	log.Println("Shutdown initiated, blocking new requests...")
	mutex.Lock()
	shutdownFlag = true
	mutex.Unlock()

	log.Println("Waiting for ongoing requests to complete...")
	log.Printf("%d ongoing requests left", activeRequests)
	wg.Wait()

	log.Println("Closing external dependencies...")

	if redisClient != nil {
		redisClient.Close()
	}
	if postgresDB != nil {
		postgresDB.Close()
	}
	if mongoClient != nil {
		mongoClient.Disconnect(ctx)
	}
	if kafkaProducer != nil {
		kafkaProducer.Close()
	}

	log.Println("Shutdown complete")
}
