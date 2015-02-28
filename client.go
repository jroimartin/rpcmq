// Copyright 2014 The rpcmq Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rpcmq

import (
	"encoding/json"
	"fmt"

	"github.com/streadway/amqp"
)

type Client struct {
	queue      string
	ac         *amqpClient
	deliveries <-chan amqp.Delivery
	results    chan Result
}

type Result struct {
	UUID string
	Data []byte
	Err  error
}

type rpcMsg struct {
	Method string
	Data   []byte
}

func NewClient(uri, queue string) *Client {
	c := &Client{
		queue:   queue,
		ac:      newAmqpRpc(uri),
		results: make(chan Result),
	}
	return c
}

func (c *Client) Init() error {
	if err := c.ac.init(); err != nil {
		return err
	}

	var err error
	c.ac.queue, err = c.ac.channel.QueueDeclare(
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

	c.ac.consumerTag, err = UUID()
	if err != nil {
		return fmt.Errorf("UUID: %v", err)
	}

	c.deliveries, err = c.ac.channel.Consume(
		c.ac.queue.Name,  // name
		c.ac.consumerTag, // consumer
		false,            // autoAck
		false,            // exclusive
		false,            // noLocal
		false,            // noWait
		nil,              // args
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
			d.Nack(false, false)
			continue
		}
		var r Result
		if err := json.Unmarshal(d.Body, &r); err != nil {
			d.Nack(false, false)
			continue
		}
		c.results <- r
		d.Ack(false)
	}
	c.ac.done <- true
}

func (c *Client) Shutdown() error {
	return c.ac.shutdown()
}

func (c *Client) Call(method string, data []byte) (uuid string, err error) {
	uuid, err = UUID()
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

	err = c.ac.channel.Publish(
		"",      // exchange
		c.queue, // key
		false,   // mandatory
		false,   // immediate
		amqp.Publishing{ // msg
			CorrelationId: uuid,
			ReplyTo:       c.ac.queue.Name,
			ContentType:   "application/json",
			Body:          body,
			DeliveryMode:  amqp.Persistent, // TODO(jrm): Configurable mode
		},
	)
	if err != nil {
		return "", err
	}

	return uuid, nil
}

func (c *Client) Results() <-chan Result {
	return (<-chan Result)(c.results)
}
