package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"

	"os/signal"
	"syscall"

	solanapb "github.com/bitquery/streaming_protobuf/v2/solana/messages"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"github.com/joho/godotenv"

	"google.golang.org/protobuf/proto"
)

func main() {
	log.SetOutput(os.Stderr)
	log.SetFlags(log.LstdFlags)

	_ = godotenv.Load()

	cfg, err := loadConfigFromEnv()
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}

	cm := kafka.ConfigMap{
		"bootstrap.servers":                     cfg.bootstrap,
		"security.protocol":                     "SASL_PLAINTEXT",
		"sasl.mechanisms":                       "SCRAM-SHA-512",
		"sasl.username":                         cfg.username,
		"sasl.password":                         cfg.password,
		"group.id":                              cfg.groupID,
		"session.timeout.ms":                    30_000,
		"enable.auto.commit":                    false,
		"ssl.endpoint.identification.algorithm": "none",
		"auto.offset.reset":                     cfg.autoOffset,
	}

	consumer, err := kafka.NewConsumer(&cm)
	if err != nil {
		log.Printf("kafka NewConsumer: %v", err)
		os.Exit(1)
	}
	defer func() {
		log.Println("closing consumer …")
		if err := consumer.Close(); err != nil {
			log.Printf("consumer close: %v", err)
		}
	}()

	if err := consumer.Subscribe(cfg.topic, nil); err != nil {
		log.Printf("subscribe: %v", err)
		os.Exit(1)
	}

	log.Printf("listening topic=%s group.id=%s", cfg.topic, cfg.groupID)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	for {
		if ctx.Err() != nil {
			break
		}

		ev := consumer.Poll(1000)

		switch e := ev.(type) {
		case *kafka.Message:
			if e.Value == nil {
				continue
			}
			var blk solanapb.ParsedIdlBlockMessage
			if err := proto.Unmarshal(e.Value, &blk); err != nil {
				log.Printf("protobuf unmarshal: %v", err)
				continue
			}
			writeProtoReflectTree(os.Stdout, blk.ProtoReflect(), 0)

		case kafka.Error:
			code := e.Code()
			if code == kafka.ErrTimedOut || code == kafka.ErrPartitionEOF {
				continue
			}
			if ctx.Err() != nil && errors.Is(ctx.Err(), context.Canceled) {
				continue
			}
			log.Printf("kafka: %v", e)

		default:
			// ignore unrelated events (assignments, etc.)
		}
	}
}

type envConfig struct {
	username   string
	password   string
	topic      string
	bootstrap  string
	groupID    string
	autoOffset string
}

func loadConfigFromEnv() (*envConfig, error) {
	u := strings.TrimSpace(os.Getenv("KAFKA_USERNAME"))
	p := strings.TrimSpace(os.Getenv("KAFKA_PASSWORD"))
	if u == "" || p == "" {
		return nil, fmt.Errorf("set KAFKA_USERNAME and KAFKA_PASSWORD (copy .env.example to .env)")
	}

	topic := strings.TrimSpace(os.Getenv("KAFKA_TOPIC"))
	if topic == "" {
		topic = "solana.transactions.proto"
	}

	bootstrap := strings.TrimSpace(os.Getenv("KAFKA_BOOTSTRAP_SERVERS"))
	if bootstrap == "" {
		bootstrap = "rpk0.bitquery.io:9092,rpk1.bitquery.io:9092,rpk2.bitquery.io:9092"
	}

	auto := strings.TrimSpace(os.Getenv("KAFKA_AUTO_OFFSET_RESET"))
	if auto == "" {
		auto = "latest"
	}
	if auto != "latest" && auto != "earliest" {
		return nil, fmt.Errorf("KAFKA_AUTO_OFFSET_RESET must be latest or earliest")
	}

	group := strings.TrimSpace(os.Getenv("KAFKA_GROUP_ID"))
	if group == "" {
		group = fmt.Sprintf("%s-group-%s", u, strings.ReplaceAll(uuid.NewString(), "-", ""))
	}

	return &envConfig{
		username:   u,
		password:   p,
		topic:      topic,
		bootstrap:  bootstrap,
		groupID:    group,
		autoOffset: auto,
	}, nil
}
