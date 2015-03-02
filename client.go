// Copyright 2014 The rpcmq Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rpcmq

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/streadway/amqp"
)

// A Client is an RPC client, which is used to invoke remote procedures.
type Client struct {
	queueName    string
	ac           *amqpClient
	queue        amqp.Queue
	queueReplies amqp.Queue
	deliveries   <-chan amqp.Delivery
	results      chan Result
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
// network address of the broker and queue is the name of queue that will be
// created to exchange the messages between clients and servers.
//
// It is important to note that it needs to be initialized before being used.
func NewClient(uri, queue string) *Client {
	c := &Client{
		queueName: queue,
		ac:        newAmqpRpc(uri),
		results:   make(chan Result),
	}
	return c
}

// Init initializes the Client object. It establishes the connection with the
// broker, creating a channel and the queues that will be used under the hood.
func (c *Client) Init() error {
	if err := c.ac.init(); err != nil {
		return err
	}

	var err error
	c.queue, err = c.ac.channel.QueueDeclare(
		c.queueName, // name
		true,        // durable
		false,       // autoDelete
		false,       // exclusive
		false,       // noWait
		nil,         // args
	)
	if err != nil {
		return fmt.Errorf("Queue Declare: %v", err)
	}
	c.queueReplies, err = c.ac.channel.QueueDeclare(
		"",    // name
		true,  // durable
		false, // autoDelete
		true,  // exclusive
		false, // noWait
		nil,   // args
	)
	if err != nil {
		return fmt.Errorf("Queue Declare: %v", err)
	}

	c.ac.consumerTag, err = uuid()
	if err != nil {
		return fmt.Errorf("UUID: %v", err)
	}

	c.deliveries, err = c.ac.channel.Consume(
		c.queueReplies.Name, // name
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
			log.Printf("dropped message: %+v\n", d)
			continue
		}
		var r Result
		if err := json.Unmarshal(d.Body, &r); err != nil {
			d.Nack(false, false) // drop message
			log.Printf("dropped message: %+v\n", d)
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
func (c *Client) Shutdown() error {
	return c.ac.shutdown()
}

// Call invokes the remote procedure specified by the parameter method, being
// the parameter data the input passed to it. On the other hand, ttl is the
// time that this task will remain in the queue before being considered dead.
// The returned id can be used to identify the result corresponding to each
// invokation.
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
	err = c.ac.channel.Publish(
		"",          // exchange
		c.queueName, // key
		true,        // mandatory
		false,       // immediate
		amqp.Publishing{ // msg
			CorrelationId: id,
			ReplyTo:       c.queueReplies.Name,
			ContentType:   "application/json",
			Body:          body,
			DeliveryMode:  amqp.Persistent,
			Expiration:    expiration,
		},
	)
	if err != nil {
		return "", err
	}

	return id, nil
}

// Results returns a channel used to receive the results returned by the
// invoked procedures.
func (c *Client) Results() <-chan Result {
	return (<-chan Result)(c.results)
}
