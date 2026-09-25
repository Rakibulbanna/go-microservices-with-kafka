package config

import (
	"os"
	"strconv"
	"strings"
)

type Config struct {
	Port          string
	KafkaBrokers  []string
	DBHost        string
	DBPort        string
	DBUser        string
	DBPassword    string
	DBName        string
	DBSSLMode     string
	RedisHost     string
	RedisPort     string
	LogLevel      string
}

func Load() Config {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9093"
	}

	return Config{
		Port:         getEnv("ORDER_SERVICE_PORT", "8081"),
		KafkaBrokers: strings.Split(brokers, ","),
		DBHost:       getEnv("ORDER_DB_HOST", "localhost"),
		DBPort:       getEnv("ORDER_DB_PORT", "5432"),
		DBUser:       getEnv("ORDER_DB_USER", "postgres"),
		DBPassword:   getEnv("ORDER_DB_PASSWORD", "postgres"),
		DBName:       getEnv("ORDER_DB_NAME", "orders_db"),
		DBSSLMode:    getEnv("ORDER_DB_SSLMODE", "disable"),
		RedisHost:    getEnv("REDIS_HOST", "localhost"),
		RedisPort:    getEnv("REDIS_PORT", "6379"),
		LogLevel:     getEnv("LOG_LEVEL", "info"),
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

func GetEnvInt(key string, fallback int) int {
	if val := os.Getenv(key); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return fallback
}
