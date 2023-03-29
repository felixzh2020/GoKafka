package main

/***
@Author FelixZh
@Describe 支持无认证、SASL/PLAIN认证、TLS认证；不支持SASL/SCRAM、SASL/GSSAPI
*/

import (
	"bufio"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"crypto/tls"
	"crypto/x509"

	"github.com/Shopify/sarama"
)

var (
	command    string
	hosts      string
	topic      string
	partition  int
	saslEnable bool
	username   string
	password   string
	tlsEnable  bool
	clientCert string
	clientKey  string
	caCert     string
)

func main() {
	flag.StringVar(&command, "command", "consumer", "consumer|producer")
	flag.StringVar(&hosts, "host", "localhost:9092", "Common separated kafka hosts")
	flag.StringVar(&topic, "topic", "test-topic", "Kafka topic")
	flag.IntVar(&partition, "partition", 0, "Kafka topic partition")

	flag.BoolVar(&saslEnable, "sasl", false, "SASL enable")
	flag.StringVar(&username, "username", "", "SASL Username")
	flag.StringVar(&password, "password", "", "SASL Password")

	flag.BoolVar(&tlsEnable, "tls", false, "TLS enable")
	flag.StringVar(&clientCert, "cert", "cert.pem", "Client Certificate")
	flag.StringVar(&clientKey, "key", "key.pem", "Client Key")
	flag.StringVar(&caCert, "ca", "ca.pem", "CA Certificate")
	flag.Parse()

	config := sarama.NewConfig()
	if saslEnable {
		config.Net.SASL.Enable = true
		config.Net.SASL.User = username
		config.Net.SASL.Password = password
	}

	if tlsEnable {
		tlsConfig, err := genTLSConfig(clientCert, clientKey, caCert)
		if err != nil {
			log.Fatal(err)
		}

		config.Net.TLS.Enable = true
		config.Net.TLS.Config = tlsConfig
	}

	client, err := sarama.NewClient(strings.Split(hosts, ","), config)
	if err != nil {
		log.Fatalf("unable to create kafka client: %q", err)
	}

	if command == "consumer" {
		consumer, err := sarama.NewConsumerFromClient(client)
		if err != nil {
			log.Fatal(err)
		}
		defer func(consumer sarama.Consumer) {
			err := consumer.Close()
			if err != nil {
				log.Fatal(err)
			}
		}(consumer)

		loopConsumer(consumer, topic, partition)
	} else {
		producer, err := sarama.NewAsyncProducerFromClient(client)
		if err != nil {
			log.Fatal(err)
		}
		defer producer.AsyncClose()
		loopProducer(producer, topic, partition)
	}
}

func genTLSConfig(clientCertfile, clientKeyfile, caCertfile string) (*tls.Config, error) {
	// load client cert
	clientCert, err := tls.LoadX509KeyPair(clientCertfile, clientKeyfile)
	if err != nil {
		return nil, err
	}

	// load ca cert pool
	caCert, err := ioutil.ReadFile(caCertfile)
	if err != nil {
		return nil, err
	}
	caCertpool := x509.NewCertPool()
	caCertpool.AppendCertsFromPEM(caCert)

	// generate tlcconfig
	tlsConfig := tls.Config{}
	tlsConfig.RootCAs = caCertpool
	tlsConfig.Certificates = []tls.Certificate{clientCert}
	tlsConfig.BuildNameToCertificate()
	// tlsConfig.InsecureSkipVerify = true // This can be used on test server if domain does not match cert:
	return &tlsConfig, err
}

func loopProducer(producer sarama.AsyncProducer, topic string, partition int) {
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Print("> ")
	for scanner.Scan() {
		text := scanner.Text()
		if text == "" {
		} else if text == "exit" || text == "quit" {
			break
		} else {
			producer.Input() <- &sarama.ProducerMessage{Topic: topic, Key: nil, Value: sarama.StringEncoder(text)}
			log.Printf("Produced message: [%s]\n", text)
		}
		fmt.Print("> ")
	}
}

func loopConsumer(consumer sarama.Consumer, topic string, partition int) {
	partitionConsumer, err := consumer.ConsumePartition(topic, int32(partition), sarama.OffsetNewest)
	if err != nil {
		log.Println(err)
		return
	}
	defer func(partitionConsumer sarama.PartitionConsumer) {
		err := partitionConsumer.Close()
		if err != nil {
			log.Println(err)
		}
	}(partitionConsumer)

	for {
		msg := <-partitionConsumer.Messages()
		log.Printf("Consumed message: [%s], offset: [%d]\n", msg.Value, msg.Offset)
	}
}
