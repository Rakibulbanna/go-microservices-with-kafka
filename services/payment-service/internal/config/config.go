package config

import (
	"os"
	"strings"
	"time"
)

type Config struct {
	Port            string
	KafkaBrokers    []string
	DBHost          string
	DBPort          string
	DBUser          string
	DBPassword      string
	DBName          string
	DBSSLMode       string
	RedisHost       string
	RedisPort       string
	LogLevel        string
	SlowConsumer    bool
	SlowConsumerDelay time.Duration
	MaxRetries      int
}

func Load() Config {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9093"
	}

	slowDelay := os.Getenv("SLOW_CONSUMER_DELAY")
	if slowDelay == "" {
		slowDelay = "5s"
	}
	delay, _ := time.ParseDuration(slowDelay)

	return Config{
		Port:            getEnv("PAYMENT_SERVICE_PORT", "8082"),
		KafkaBrokers:    strings.Split(brokers, ","),
		DBHost:          getEnv("PAYMENT_DB_HOST", "localhost"),
		DBPort:          getEnv("PAYMENT_DB_PORT", "5433"),
		DBUser:          getEnv("PAYMENT_DB_USER", "postgres"),
		DBPassword:      getEnv("PAYMENT_DB_PASSWORD", "postgres"),
		DBName:          getEnv("PAYMENT_DB_NAME", "payments_db"),
		DBSSLMode:       getEnv("PAYMENT_DB_SSLMODE", "disable"),
		RedisHost:       getEnv("REDIS_HOST", "localhost"),
		RedisPort:       getEnv("REDIS_PORT", "6379"),
		LogLevel:        getEnv("LOG_LEVEL", "info"),
		SlowConsumer:    os.Getenv("SLOW_CONSUMER") == "true",
		SlowConsumerDelay: delay,
		MaxRetries:      3,
	}
}

func (c Config) DSN() string {
	return "host=" + c.DBHost +
		" port=" + c.DBPort +
		" user=" + c.DBUser +
		" password=" + c.DBPassword +
		" dbname=" + c.DBName +
		" sslmode=" + c.DBSSLMode
}

func (c Config) RedisAddr() string {
	return c.RedisHost + ":" + c.RedisPort
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}
