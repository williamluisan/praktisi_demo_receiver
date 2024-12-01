package main

import (
	"log"
	"strconv"
	"time"

	"github.com/go-pdf/fpdf"
	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"demo_pdf_generation", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	var forever chan struct{}

	go func() {
		queue_order := 0
		for d := range msgs {
			// manually set the order of the queue
			queue_order = queue_order + 1

			message := string(d.Body)
			process_pdf := processPdf(message, queue_order)
			
			if !process_pdf{
				break
			}
			log.Printf("Finished processing")
			
			// manual acknowledgement
			d.Ack(false)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}

func processPdf(user_id string, queue_order int) bool {
	status := true

	log.Printf("Received a message: %s (Order #%s)", user_id, strconv.Itoa(queue_order))

	timestamp := time.Now().Format("20060102150405")

	pdf := fpdf.New("P", "mm", "A4", "")
	pdf.AddPage()
	pdf.SetFont("Arial", "B", 16)
	pdf.Cell(40, 10, timestamp + " " + user_id)

	// Add a large amount of text to create a big PDF
	log.Printf("Processing the pdf: %s_%s_order#%s.pdf", timestamp, user_id, strconv.Itoa(queue_order))
    for i := 1; i <= 1000000; i++ {
        pdf.Cell(40, 10, timestamp + " " + user_id + " " + strconv.Itoa(i))
        pdf.Ln(10) // Move to the next line
    }	
	pdf.OutputFileAndClose("./pdf/" + timestamp + "_" + user_id + "_order#" + strconv.Itoa(queue_order) + ".pdf")

	// send to email (dummy logic)
	send_mail := true
	if queue_order == 4 { // failed send mail on the 2nd order
		send_mail = false
	}

	if !send_mail {
		status = false
	}

	return status
}