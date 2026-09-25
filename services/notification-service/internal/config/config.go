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
	LogLevel        string
	SlowConsumer    bool
	SlowConsumerDelay time.Duration
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
		Port:            getEnv("NOTIFICATION_SERVICE_PORT", "8083"),
		KafkaBrokers:    strings.Split(brokers, ","),
		DBHost:          getEnv("NOTIFICATION_DB_HOST", "localhost"),
		DBPort:          getEnv("NOTIFICATION_DB_PORT", "5434"),
		DBUser:          getEnv("NOTIFICATION_DB_USER", "postgres"),
		DBPassword:      getEnv("NOTIFICATION_DB_PASSWORD", "postgres"),
		DBName:          getEnv("NOTIFICATION_DB_NAME", "notifications_db"),
		DBSSLMode:       getEnv("NOTIFICATION_DB_SSLMODE", "disable"),
		LogLevel:        getEnv("LOG_LEVEL", "info"),
		SlowConsumer:    os.Getenv("SLOW_CONSUMER") == "true",
		SlowConsumerDelay: delay,
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

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}
