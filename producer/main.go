package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/Shopify/sarama"
	"github.com/gorilla/mux"
)

func PushCommentToQueue(topic string, message []byte) error {
	brokersUrl := []string{"localhost:9092"}
	producer, err := ConnectProducer(brokersUrl)
	if err != nil {
		fmt.Print("asf", err)
		return err
	}
	defer producer.Close()
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		fmt.Print("hh", err)
		return err
	}
	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)
	return nil
}
func ConnectProducer(brokersUrl []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	// NewSyncProducer creates a new SyncProducer using the given broker addresses and configuration.
	conn, err := sarama.NewSyncProducer(brokersUrl, config)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

type Comment struct {
	Text string `json:"text"`
}

func createComment(w http.ResponseWriter, r *http.Request) {
	// Instantiate new Message struct
	var id Comment
	_ = json.NewDecoder(r.Body).Decode(&id)
	fmt.Println("hi", id.Text)

	// convert body into bytes and send it to kafka
	cmtInBytes, err := json.Marshal(id.Text)
	PushCommentToQueue("comments", cmtInBytes)
	if err != nil {

		w.Header().Add("content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "error")
	} else {
		w.Header().Add("content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(id)
	}
	// Return Comment in JSON format

}

func main() {
	router := mux.NewRouter()
	router.HandleFunc("/comment", createComment)
	log.Fatal(http.ListenAndServe(":2223", router))
}
