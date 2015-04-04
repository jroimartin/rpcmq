// Copyright 2015 The rpcmq Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rpcmq

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/streadway/amqp"
)

// A Client is an RPC client, which is used to invoke remote procedures.
type Client struct {
	msgsName     string
	repliesName  string
	exchangeName string
	exchangeKind string
	ac           *amqpClient
	msgsQueue    amqp.Queue
	repliesQueue amqp.Queue
	mandatory    bool
	deliveries   <-chan amqp.Delivery
	results      chan Result

	// TLSConfig allows to configure the TLS parameters used to connect to
	// the broker via amqps
	TLSConfig *tls.Config
}

// A Result contains the data returned by the invoked procedure or an error
// message, in case that it finished with error. The UUID allows to link the
// result with the procedure call.
type Result struct {
	UUID string
	Data []byte
	Err  string
}

type rpcMsg struct {
	Method string
	Data   []byte
}

// NewClient returns a reference to a Client object. The paremeter uri is the
// network address of the broker and msgsQueue/repliesQueue are the names of
// queues that will be created to exchange the messages between clients and
// servers. On the other hand, the parameters exchange and kind determine the
// type of exchange that will be created. In fanout mode the queue name is
// ignored, so each queue has its own unique id.
func NewClient(uri, msgsQueue, repliesQueue, exchange, kind string) *Client {
	if kind == "fanout" {
		msgsQueue = "" // in fanout mode queue names must be unique
	}
	c := &Client{
		msgsName:     msgsQueue,
		repliesName:  repliesQueue,
		exchangeName: exchange,
		exchangeKind: kind,
		ac:           newAmqpClient(uri),
		results:      make(chan Result),
	}
	c.ac.setupFunc = c.setup
	return c
}

// Init initializes the Client object. It establishes the connection with the
// broker, creating a channel and the queues that will be used under the hood.
func (c *Client) Init() error {
	c.ac.tlsConfig = c.TLSConfig
	if err := c.ac.init(); err != nil {
		return err
	}
	return c.setup()
}

func (c *Client) setup() error {
	err := c.ac.channel.ExchangeDeclare(
		c.exchangeName, // name
		c.exchangeKind, // kind
		true,           // durable
		false,          // autoDelete
		false,          // internal
		false,          // noWait
		nil,            // args
	)
	if err != nil {
		return fmt.Errorf("ExchangeDeclare: %v", err)
	}

	if c.exchangeKind != "fanout" {
		// We should create the queue only in non-fanout mode
		c.msgsQueue, err = c.ac.channel.QueueDeclare(
			c.msgsName, // name
			true,       // durable
			false,      // autoDelete
			false,      // exclusive
			false,      // noWait
			nil,        // args
		)
		if err != nil {
			return fmt.Errorf("QueueDeclare: %v", err)
		}

		err = c.ac.channel.QueueBind(
			c.msgsQueue.Name, // name
			c.msgsQueue.Name, // key
			c.exchangeName,   // exchange
			false,            // noWait
			nil,              // args
		)
		if err != nil {
			return fmt.Errorf("QueueBind: %v", err)
		}
	}

	c.repliesQueue, err = c.ac.channel.QueueDeclare(
		c.repliesName, // name
		true,          // durable
		false,         // autoDelete
		false,         // exclusive
		false,         // noWait
		nil,           // args
	)
	if err != nil {
		return fmt.Errorf("QueueDeclare: %v", err)
	}

	c.ac.consumerTag, err = uuid()
	if err != nil {
		return fmt.Errorf("UUID: %v", err)
	}

	c.deliveries, err = c.ac.channel.Consume(
		c.repliesQueue.Name, // name
		c.ac.consumerTag,    // consumer
		false,               // autoAck
		false,               // exclusive
		false,               // noLocal
		false,               // noWait
		nil,                 // args
	)
	if err != nil {
		return fmt.Errorf("Queue Consume: %v", err)
	}

	go c.getDeliveries()

	return nil
}

func (c *Client) getDeliveries() {
	for d := range c.deliveries {
		if d.CorrelationId == "" {
			d.Nack(false, false) // drop message
			logf("dropped message: %+v", d)
			continue
		}
		var r Result
		if err := json.Unmarshal(d.Body, &r); err != nil {
			d.Nack(false, false) // drop message
			logf("dropped message: %+v", d)
			continue
		}
		c.results <- r
		d.Ack(false)
	}
	c.ac.done <- true
}

// Shutdown shuts down the client gracefully. Using this method will ensure
// that all replies sent by the RPC servers to the client will be received by
// the latter.
func (c *Client) Shutdown() {
	c.ac.shutdown()
}

// Call invokes the remote procedure specified by the parameter method, being
// the parameter data the input passed to it. On the other hand, ttl is the
// time that this task will remain in the queue before being considered dead.
// The returned id can be used to identify the result corresponding to each
// invokation. If ttl is 0, the message will not expire.
func (c *Client) Call(method string, data []byte, ttl time.Duration) (id string, err error) {
	id, err = uuid()
	if err != nil {
		return "", fmt.Errorf("UUID: %v", err)
	}

	msg := &rpcMsg{
		Method: method,
		Data:   data,
	}
	body, err := json.Marshal(msg)
	if err != nil {
		return "", fmt.Errorf("Marshal: %v", err)
	}

	expiration := ""
	if ttl > 0 {
		expiration = fmt.Sprintf("%d", int64(ttl.Seconds()*1000))
	}
	c.mandatory = true
	if c.exchangeKind == "fanout" {
		// In fanout mode the routing key is not really used, so using
		// the mandatory flag does not make sense
		c.mandatory = false
	}

	// guarantee that the received ack/nack corresponds with this publishing
	c.ac.mu.Lock()
	defer c.ac.mu.Unlock()

	err = c.ac.channel.Publish(
		c.exchangeName, // exchange
		c.msgsName,     // key
		c.mandatory,    // mandatory
		false,          // immediate
		amqp.Publishing{ // msg
			CorrelationId: id,
			ReplyTo:       c.repliesQueue.Name,
			ContentType:   "application/json",
			Body:          body,
			DeliveryMode:  amqp.Persistent,
			Expiration:    expiration,
		},
	)
	if err != nil {
		return "", err
	}

	select {
	case _, ok := <-c.ac.acks:
		if ok {
			return id, nil
		}
	case tag, ok := <-c.ac.nacks:
		if ok {
			logf("nack recived (%v)", tag)
			return "", errors.New("nack received")
		}
	}

	logf("missing ack/nack")
	return "", errors.New("missing ack/nack")
}

// Results returns a channel used to receive the results returned by the
// invoked procedures.
func (c *Client) Results() <-chan Result {
	return (<-chan Result)(c.results)
}
