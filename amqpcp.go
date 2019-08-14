//
// AMQP Connection Pool
//
// Rostislav Olitto (c) 2019
// This code is licensed under MIT license
//
package amqpcp

import (
	"errors"
	"github.com/streadway/amqp"
	"log"
	"sync"
	"sync/atomic"
)

// Error / Text definition
var (
	ErrTooManyConnections    = errors.New("amqp cp: maximum connection limit reached")
	ErrBadConnection         = errors.New("amqp cp: unexpected connection problem")
	ErrChannelNotAllClosed   = errors.New("amqp cp: not all channels was closed")
	ErrPoolClosed            = errors.New("amqp cp: pool closed")
	ErrMessageNacked         = errors.New("amqp cp: server not ack'ed the message")
	WrnUnableGetChannel      = "amqp cp: unable to get new channel %s\n"
	WrnUnableCloseConnection = "amqp cp: close connection error %s\n"
)

// Configuration
type PoolConfig struct {
	ServerURI      string // Expected format: amqp://user:password@hostname:5672/
	ConnMin        int
	ConnMax        int
	ChanIdleMax    int
	ChanPerConnMax int
}

func (config *PoolConfig) NewPool() *Pool {
	pool := &Pool{
		config:             *config,
		connList:           make([]*Connection, 0, config.ConnMax),
		connDelayCloseChan: make(chan *Connection, config.ConnMax),
		connDelayClosed:    make(chan struct{}),
		chanIdleList:       make([]*Channel, 0, config.ChanIdleMax),
		chanReqList:        &PendingQueue{},
	}

	go func() {
		for conn := range pool.connDelayCloseChan {
			if e := conn.close(true); e != nil {
				log.Printf(WrnUnableCloseConnection, e)
			}
		}

		close(pool.connDelayClosed)
	}()

	return pool
}

// Connection
type Connection struct {
	amqpConn  *amqp.Connection
	chanCount int
	syncLock  sync.RWMutex
}

func (conn *Connection) openChannel() (*Channel, error) {
	var (
		amqpChan *amqp.Channel
		e        error
	)
	conn.syncLock.Lock()
	defer conn.syncLock.Unlock()

	if conn.amqpConn == nil {
		return nil, ErrBadConnection
	}

	if amqpChan, e = conn.amqpConn.Channel(); e != nil {
		return nil, e
	}

	ctx := &Channel{
		conn:     conn,
		amqpChan: amqpChan,
	}

	if e = ctx.amqpChan.Confirm(false); e != nil {
		return nil, e
	}

	ctx.amqpConfirm = ctx.amqpChan.NotifyPublish(make(chan amqp.Confirmation, 1))
	conn.chanCount++

	return ctx, nil
}

func (conn *Connection) closeChannel() {
	conn.syncLock.Lock()
	defer conn.syncLock.Unlock()

	conn.chanCount--
}

func (conn *Connection) getOpenChannelsCount() int {
	conn.syncLock.RLock()
	defer conn.syncLock.RUnlock()

	return conn.chanCount
}

func (conn *Connection) close(force bool) error {
	conn.syncLock.Lock()
	defer conn.syncLock.Unlock()

	if !force && conn.chanCount > 0 {
		return ErrChannelNotAllClosed
	}

	if conn.amqpConn != nil {
		if e := conn.amqpConn.Close(); e != nil {
			log.Printf(WrnUnableCloseConnection, e)
		}
	}

	conn.amqpConn = nil
	return nil
}

// Channel
type Channel struct {
	conn        *Connection
	amqpChan    *amqp.Channel
	amqpConfirm chan amqp.Confirmation
}

func (ch *Channel) DeclareQueue(queueName string, durable, autoDelete, exclusive bool, exchangeName, routingKey string, noWait bool, args amqp.Table) (amqp.Queue, error) {
	var (
		queue amqp.Queue
		e     error
	)

	queue, e = ch.amqpChan.QueueDeclare(
		queueName,
		durable,
		autoDelete,
		exclusive,
		noWait,
		args,
	)

	if e == nil {
		if len(exchangeName) > 0 {
			e = ch.amqpChan.QueueBind(
				queueName,
				routingKey,
				exchangeName,
				noWait,
				nil,
			)
		}
	}

	return queue, e
}

func (ch *Channel) StartConsume(queueName, consumerID string, autoAck, exclusive, noWait bool) (<-chan amqp.Delivery, error) {
	return ch.amqpChan.Consume(
		queueName,
		consumerID,
		autoAck,
		exclusive,
		false, // The noLocal flag is not supported by AMQP
		noWait,
		nil,
	)
}

func (ch *Channel) StopConsume(consumerID string, noWait bool) error {
	return ch.amqpChan.Cancel(consumerID, noWait)
}

func (ch *Channel) PutMessage(exchange, routingKey, messageID, correlationID, replyTo string, data []byte) error {
	e := ch.amqpChan.Publish(
		exchange,
		routingKey,
		true,
		false,
		amqp.Publishing{
			MessageId:     messageID,
			CorrelationId: correlationID,
			ReplyTo:       replyTo,
			Body:          data,
			DeliveryMode:  2,
		})

	if e != nil {
		return e
	}

	confirmed := <-ch.amqpConfirm

	if confirmed.Ack {
		return nil
	}

	return ErrMessageNacked
}

func (ch *Channel) close() {
	_ = ch.amqpChan.Close()

	ch.conn.closeChannel()
	ch.conn = nil
	ch.amqpChan = nil
}

// List of requests for Channel allocation
type PendingQueue struct {
	chanNotifyList []chan *Channel
	syncLock       sync.Mutex
}

func (pendingQueue *PendingQueue) Len() int {
	pendingQueue.syncLock.Lock()
	defer pendingQueue.syncLock.Unlock()

	return len(pendingQueue.chanNotifyList)
}

func (pendingQueue *PendingQueue) Put(ch chan *Channel) {
	pendingQueue.syncLock.Lock()
	defer pendingQueue.syncLock.Unlock()

	pendingQueue.chanNotifyList = append(pendingQueue.chanNotifyList, ch)
}

func (pendingQueue *PendingQueue) NotifyOne(ch *Channel) bool {
	pendingQueue.syncLock.Lock()
	defer pendingQueue.syncLock.Unlock()

	if len(pendingQueue.chanNotifyList) > 0 {
		var chanNotify chan *Channel

		chanNotify, pendingQueue.chanNotifyList = pendingQueue.chanNotifyList[0], pendingQueue.chanNotifyList[1:]
		chanNotify <- ch

		close(chanNotify)
		return true
	}

	return false
}

func (pendingQueue *PendingQueue) NotifyAll() {
	pendingQueue.syncLock.Lock()
	defer pendingQueue.syncLock.Unlock()

	for i := 0; i < len(pendingQueue.chanNotifyList); i++ {
		close(pendingQueue.chanNotifyList[i])
	}

	pendingQueue.chanNotifyList = nil
}

// Connection Pool
type Pool struct {
	config             PoolConfig
	connList           []*Connection
	connDelayCloseChan chan *Connection
	connDelayClosed    chan struct{}
	chanReqList        *PendingQueue
	chanIdleList       []*Channel
	chanBusyCount      int32
	closed             bool
	syncLock           sync.Mutex
}

type PoolState struct {
	ConnOpened    int
	ChanIdle      int
	ChanBusy      int
	ChanRequested int
}

func (pool *Pool) GetChannel() (*Channel, error) {
	var ch *Channel

	for {
		pool.syncLock.Lock()

		if pool.closed {
			pool.syncLock.Unlock()
			return nil, ErrPoolClosed
		}

		// #1 Reuse free channels
		if len(pool.chanIdleList) > 0 {
			var ch *Channel

			ch, pool.chanIdleList = pool.chanIdleList[0], pool.chanIdleList[1:]

			pool.incrChanBusy()
			pool.syncLock.Unlock()

			return ch, nil
		}

		// #2 Trying to get connection from pool (existing or new)
		conn, e := pool.getConnection()

		if e == ErrTooManyConnections {
			pool.syncLock.Unlock()

			// Waiting for available channel
			chanNotify := make(chan *Channel)

			pool.chanReqList.Put(chanNotify)

			if ch := <-chanNotify; ch != nil {
				pool.incrChanBusy()
				return ch, nil
			}

			continue
		} else if e != nil {
			pool.close()
			pool.syncLock.Unlock()

			return nil, e
		}

		// #3 Open new channel
		ch, e = conn.openChannel()

		if e == amqp.ErrClosed || e == ErrBadConnection {
			log.Printf(WrnUnableGetChannel, ErrBadConnection)
			_ = pool.removeConnection(conn)
			pool.syncLock.Unlock()
			continue
		} else if e == amqp.ErrChannelMax {
			log.Printf(WrnUnableGetChannel, amqp.ErrChannelMax)
			pool.syncLock.Unlock()
			continue
		} else if e != nil {
			pool.close()
			pool.syncLock.Unlock()

			return nil, e
		}

		break
	}

	pool.incrChanBusy()
	pool.syncLock.Unlock()

	return ch, nil
}

func (pool *Pool) PutChannel(ch *Channel) {
	pool.decrChanBusy()

	if pool.chanReqList.NotifyOne(ch) {
		return
	}

	pool.syncLock.Lock()
	available := len(pool.chanIdleList)
	pool.syncLock.Unlock()

	if available >= pool.config.ChanIdleMax {
		pool.probeCloseChannel(ch)
		return
	}

	pool.syncLock.Lock()
	pool.chanIdleList = append(pool.chanIdleList, ch)
	pool.syncLock.Unlock()
}

func (pool *Pool) CloseAll() {
	pool.syncLock.Lock()
	defer pool.syncLock.Unlock()

	pool.close()
}

func (pool *Pool) State() *PoolState {
	pool.syncLock.Lock()
	defer pool.syncLock.Unlock()

	return &PoolState{
		ConnOpened:    len(pool.connList),
		ChanIdle:      len(pool.chanIdleList),
		ChanBusy:      pool.getChanBusyCount(),
		ChanRequested: pool.chanReqList.Len(),
	}
}

func (pool *Pool) getConnection() (*Connection, error) {
	if len(pool.connList) > 0 {
		for i := 0; i < len(pool.connList); i++ {
			if pool.connList[i].getOpenChannelsCount() < pool.config.ChanPerConnMax {
				return pool.connList[i], nil
			}
		}
	}

	if pool.config.ConnMax > 0 && len(pool.connList) >= pool.config.ConnMax {
		return nil, ErrTooManyConnections
	}

	amqpConn, e := amqp.Dial(pool.config.ServerURI)

	if e != nil {
		return nil, e
	}

	conn := &Connection{amqpConn: amqpConn, chanCount: 0}
	pool.connList = append(pool.connList, conn)

	return conn, nil
}

func (pool *Pool) removeConnection(conn *Connection) error {
	connIndex := -1

	for i := 0; i < len(pool.connList); i++ {
		if conn == pool.connList[i] {
			connIndex = i
			break
		}
	}

	if connIndex > -1 {
		copy(pool.connList[connIndex:], pool.connList[connIndex+1:])

		pool.connList[len(pool.connList)-1] = nil
		pool.connList = pool.connList[:len(pool.connList)-1]
	}

	if pool.closed {
		if e := conn.close(true); e != nil {
			return e
		}

		return nil
	}

	pool.connDelayCloseChan <- conn
	return nil
}

func (pool *Pool) close() {
	if pool.closed {
		return
	}

	pool.closed = true

	close(pool.connDelayCloseChan)
	<-pool.connDelayClosed

	for i, ch := range pool.chanIdleList {
		ch.close()
		pool.chanIdleList[i] = nil
	}

	pool.chanIdleList = nil

	for i, conn := range pool.connList {
		_ = conn.close(true)
		pool.connList[i] = nil
	}

	pool.connList = nil
}

func (pool *Pool) probeCloseChannel(ch *Channel) {
	conn := ch.conn

	ch.close()

	pool.syncLock.Lock()
	defer pool.syncLock.Unlock()

	if conn.getOpenChannelsCount() == 0 && len(pool.connList) > pool.config.ConnMin {
		_ = pool.removeConnection(conn)
	}
}

func (pool *Pool) getChanBusyCount() int {
	return int(atomic.LoadInt32(&pool.chanBusyCount))
}

func (pool *Pool) incrChanBusy() {
	atomic.AddInt32(&pool.chanBusyCount, int32(1))
}

func (pool *Pool) decrChanBusy() {
	atomic.AddInt32(&pool.chanBusyCount, int32(-1))
}
