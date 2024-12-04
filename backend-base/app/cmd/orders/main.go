package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"go.uber.org/fx"
	"wb.ru/sevices/orders/backend-base/controllers/dbcontroller"
	"wb.ru/sevices/orders/backend-base/ordergenerator"
)

func main() {
	if err := dbcontroller.TestDb(); err != nil {
		log.Fatal(err)
	}

	message := `{}`

	go ordergenerator.PushMessage((10 * time.Minute), (3 * time.Second), message)

	fx.New(
		fx.Provide( //регистрируем
			NewKafkaConsumerService,
			NewHTTPServer,
		),
		fx.Invoke( //конфиг запуска (этапы)
			func(*KafkaConsumerService) {},
			func(*http.Server) {},
		),
	).Run() // запуск (выполнение в заданном/конфигурированном порядке/логике)

}

func SubscribeHandler(w http.ResponseWriter, r *http.Request) {
}

func NewHTTPServer(lc fx.Lifecycle) *http.Server {
	srv := &http.Server{Addr: ":8070"}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "../../../html/index.html")
	})

	http.HandleFunc("/orders", SubscribeHandler)

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			go http.ListenAndServe(":9090", nil)
			return nil
		},
		OnStop: func(ctx context.Context) error {
			return srv.Shutdown(ctx)
		},
	})
	return srv
}

type KafkaConsumerService struct {
	consumer sarama.PartitionConsumer
	conn     sarama.Consumer
}

func NewKafkaConsumerService(lc fx.Lifecycle) *KafkaConsumerService {
	topic := "orders"

	conn, err := ConnectConsumer([]string{"localhost:9094"}) // консьюмер (экземпляр)
	if err != nil {
		panic(err)
	}

	consumer, err := conn.ConsumePartition(topic, 0, sarama.OffsetOldest)
	// устанавливаем поле ConsumePartition для проверки использования данного косьюмера текущего топика/портиции
	if err != nil { // ошибка если консьюмер использует данный топик/портицию
		panic(err)
	}
	kafkaService := &KafkaConsumerService{
		consumer: consumer,
		conn:     conn,
	}
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {

			go kafkaService.Start()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			return kafkaService.Stop()
		},
	})

	return kafkaService
}

func (k *KafkaConsumerService) Start() {
	fmt.Println("Consumer started ")
	msgCnt := 0

	// 2. Handle OS signals - used to stop the process.
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// 3. Create a Goroutine to run the consumer / worker.
	doneCh := make(chan struct{})
	go func() {
		for { // читаем/слушаем
			select {
			case err := <-k.consumer.Errors():
				fmt.Println(err)
			case msg := <-k.consumer.Messages():
				msgCnt++
				fmt.Printf("Received order Count %d: | Topic(%s) | Message(%s) \n", msgCnt, string(msg.Topic), string(msg.Value))
				fmt.Printf("Brewing coffee for order: %s\n", msg.Value)
			case <-sigchan:
				fmt.Println("Interrupt is detected")
				doneCh <- struct{}{}
			}
		}
	}()

	<-doneCh
}

func (k *KafkaConsumerService) Stop() error {
	return k.conn.Close()
}

// func NewKafkaConsumerWorker(lc fx.Lifecycle) sarama.PartitionConsumer {

// 	topic := "orders"
// 	msgCnt := 0

// 	// 1. Create a new consumer and start it.
// 	worker, err := ConnectConsumer([]string{"localhost:9094"})
// 	if err != nil {
// 		panic(err)
// 	}

// 	consumer, err := worker.ConsumePartition(topic, 0, sarama.OffsetOldest)
// 	if err != nil {
// 		panic(err)
// 	}

// 	fmt.Println("Consumer started ")

// 	// 2. Handle OS signals - used to stop the process.
// 	sigchan := make(chan os.Signal, 1)
// 	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

// 	// 3. Create a Goroutine to run the consumer / worker.
// 	doneCh := make(chan struct{})
// 	go func() {
// 		for {
// 			select {
// 			case err := <-consumer.Errors():
// 				fmt.Println(err)
// 			case msg := <-consumer.Messages():
// 				msgCnt++
// 				fmt.Printf("Received order Count %d: | Topic(%s) | Message(%s) \n", msgCnt, string(msg.Topic), string(msg.Value))
// 				order := string(msg.Value)
// 				fmt.Printf("Brewing coffee for order: %s\n", order)
// 			case <-sigchan:
// 				fmt.Println("Interrupt is detected")
// 				doneCh <- struct{}{}
// 			}
// 		}
// 	}()

// 	<-doneCh
// 	fmt.Println("Processed", msgCnt, "messages")

// 	// 4. Close the consumer on exit.
// 	if err := worker.Close(); err != nil {
// 		panic(err)
// 	}

// 	return consumer
// }

func ConnectConsumer(brokers []string) (sarama.Consumer, error) {
	config := sarama.NewConfig()         // конфигурации консьюмера возвращаются
	config.Consumer.Return.Errors = true // вносим изменения в одну из конфигов консьюмера
	//есть возможность отконфигурировать консьюмера, много полей

	return sarama.NewConsumer(brokers, config) // возврат консьюмера(экземпляра), ошибки
}
