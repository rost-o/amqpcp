# AMQP Connection Pool
![GitHub release](https://img.shields.io/github/release/rost-o/amqpcp/all.svg)

### Usage example

Text message sender

```go
package main

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/rost-o/amqpcp"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type ctxKeyType string
type ctxData struct {
	pool     *amqpcp.Pool
	wg       *sync.WaitGroup
	exchange string
}

func routine(ctx context.Context) {
	rtnID, _ := uuid.NewUUID()
	d := ctx.Value(ctxKeyType("data")).(*ctxData)

	defer d.wg.Done()

	ch, e := d.pool.GetChannel()

	if e != nil {
		log.Println(e)
		return
	}

	defer d.pool.PutChannel(ch)

	for {
		select {
		case <-ctx.Done():
			log.Printf("[Routine %s] Stop!\n", rtnID.String())
			return
		default:
			msgID, _ := uuid.NewUUID()
			msgText := fmt.Sprintf("[Routine %s] Hello, my MessageID is %s", rtnID.String(), msgID.String())

			e = ch.PutMessage(d.exchange, "", msgID.String(), "", "", []byte(msgText))

			if e != nil {
				log.Printf("Failed to send message: %s\n", e)
				return
			}

			log.Printf("SENDED: %s\n", msgText)
			time.Sleep(5 * time.Second)
		}
	}
}

func main() {
	rtnNum := 7
	termChan := make(chan os.Signal)

	signal.Notify(termChan, syscall.SIGTERM)
	signal.Notify(termChan, syscall.SIGINT)

	cfg := amqpcp.PoolConfig{
		ServerURI:      "amqp://user:password@localhost:5672",
		ConnMax:        10,
		ChanIdleMax:    1,
		ChanPerConnMax: 3,
	}

	pool := cfg.NewPool()
	defer pool.CloseAll()

	group := sync.WaitGroup{}
	group.Add(rtnNum)

	data := ctxData{
		pool,
		&group,
		"my_exchange",
	}

	ctxBase := context.WithValue(context.Background(), ctxKeyType("data"), &data)
	ctx, cancel := context.WithCancel(ctxBase)

	log.Println("Start go-routines")

	for i := 0; i < rtnNum; i++ {
		go routine(ctx)
		time.Sleep(1 * time.Second)
	}

	go func() {
		<-termChan
		log.Println("Termination signal received. Stop all go-routines...")
		cancel()
	}()

	group.Wait()
	log.Println("Done!")
}

```

Text message receiver

```go
package main

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/rost-o/amqpcp"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

type ctxKeyType string
type ctxData struct {
	pool  *amqpcp.Pool
	wg    *sync.WaitGroup
	queue string
}

func routine(ctx context.Context) {
	rtnID, _ := uuid.NewUUID()
	d := ctx.Value(ctxKeyType("data")).(*ctxData)

	defer d.wg.Done()

	ch, e := d.pool.GetChannel()

	if e != nil {
		log.Println(e)
		return
	}

	defer d.pool.PutChannel(ch)

	recvChan, e := ch.StartConsume(d.queue, rtnID.String(), false, false, false)

	if e != nil {
		log.Println(e)
		return
	}

	for {
		select {
		case <-ctx.Done():
			log.Printf("[Routine %s] Stop!\n", rtnID.String())
			return
		case msg := <-recvChan:
			fmt.Printf("[Routine %s] New message received. ID: %s Body: %s\n", rtnID.String(), msg.MessageId, string(msg.Body))

			if e = msg.Ack(false); e != nil {
				log.Printf("Unable to handle messages successfully: %s\n", e)
				break
			}
		}
	}
}

func main() {
	rtnNum := 5
	termChan := make(chan os.Signal)

	signal.Notify(termChan, syscall.SIGTERM)
	signal.Notify(termChan, syscall.SIGINT)

	cfg := amqpcp.PoolConfig{
		ServerURI:      "amqp://user:password@localhost:5672",
		ConnMax:        10,
		ChanIdleMax:    1,
		ChanPerConnMax: 3,
	}

	pool := cfg.NewPool()
	defer pool.CloseAll()

	group := sync.WaitGroup{}
	group.Add(rtnNum)

	data := ctxData{
		pool,
		&group,
		"my_queue",
	}

	ctxBase := context.WithValue(context.Background(), ctxKeyType("data"), &data)
	ctx, cancel := context.WithCancel(ctxBase)

	log.Println("Start go-routines")

	for i := 0; i < rtnNum; i++ {
		go routine(ctx)
	}

	go func() {
		<-termChan
		log.Println("Termination signal received. Stop all go-routines...")
		cancel()
	}()

	group.Wait()
	log.Println("Done!")
}
```

### Dependencies

`RabbitMQ Client Library` : <https://github.com/streadway/amqp>

## Contributing

Feedback on the utility of this package, thoughts about whether it should be changed
or extended are welcomed.
