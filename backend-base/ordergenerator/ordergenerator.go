package ordergenerator

import (
	"log"
	"time"

	"github.com/IBM/sarama"
)

const (
	topic = "orders"
)

func connectProducer(brokers []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	return sarama.NewSyncProducer(brokers, config)
}

func PushMessage(timeout, every time.Duration, message string) {

	// Создаём канал и тиккер
	done := make(chan bool)
	ticker := time.NewTicker(every)

	brokers := []string{"localhost:9094"}

	// Создаём подключение
	producer, err := connectProducer(brokers)
	if err != nil {
		log.Printf(err.Error())
	}

	defer producer.Close()

	go func() {
		for {
			select {
			case <-done:
				ticker.Stop()
				return
			case <-ticker.C:
				// Создаём сообщение
				msg := &sarama.ProducerMessage{
					Topic: topic,
					Value: sarama.StringEncoder(message),
				}
				// Отправляем сообщение
				partition, offset, err := producer.SendMessage(msg)
				if err != nil {
					log.Printf(err.Error())
					done <- true
				}

				log.Printf("Сообщение записано: topic(%s)/partition(%d)/offset(%d)\n",
					topic,
					partition,
					offset)

			}
		}
	}()

	// Ждем
	time.Sleep(timeout)
	done <- true
}
