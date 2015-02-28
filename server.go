// Copyright 2014 The rpcmq Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rpcmq

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/streadway/amqp"
)

type Function func(args ...interface{}) (interface{}, error)

type Server struct {
	queue      string
	ac         *amqpClient
	deliveries <-chan amqp.Delivery
	methods    map[string]Function
}

func NewServer(uri, queue string) *Server {
	s := &Server{
		queue:   queue,
		methods: make(map[string]Function),
		ac:      newAmqpRpc(uri),
	}
	return s
}

func (s *Server) Init() error {
	if err := s.ac.init(); err != nil {
		return err
	}

	var err error
	s.ac.queue, err = s.ac.channel.QueueDeclare(
		s.queue, // name
		true,    // durable
		false,   // autoDelete
		false,   // exclusive
		false,   // noWait
		nil,     // args
	)
	if err != nil {
		return fmt.Errorf("Queue Declare: %v", err)
	}

	s.ac.consumerTag, err = UUID()
	if err != nil {
		return fmt.Errorf("UUID: %v", err)
	}

	s.deliveries, err = s.ac.channel.Consume(
		s.queue,          // name
		s.ac.consumerTag, // consumer
		false,            // autoAck
		false,            // exclusive
		false,            // noLocal
		false,            // noWait
		nil,              // args
	)
	if err != nil {
		return fmt.Errorf("Queue Consume: %v", err)
	}

	go s.getDeliveries()

	return nil
}

func (s *Server) getDeliveries() {
	for d := range s.deliveries {
		if d.CorrelationId == "" || d.ReplyTo == "" {
			d.Nack(false, false)
			continue
		}

		var msg rpcMsg
		if err := json.Unmarshal(d.Body, &msg); err != nil {
			d.Nack(false, false)
			continue
		}

		var (
			r   interface{}
			err error
		)
		if f, ok := s.methods[msg.Method]; ok {
			if r, err = f(msg.Args...); err != nil {
				d.Nack(false, true)
				continue
			}
		}

		result := &Result{
			UUID: d.CorrelationId,
			Val:  r,
		}
		body, err := json.Marshal(result)
		if err != nil {
			d.Nack(false, true)
			continue
		}

		s.ac.channel.Publish(
			"",
			d.ReplyTo,
			false,
			false,
			amqp.Publishing{
				CorrelationId: d.CorrelationId,
				ContentType:   "application/json",
				Body:          body,
				DeliveryMode:  amqp.Persistent, // TODO(jrm): Configurable mode
			},
		)
		d.Ack(false)
	}
	s.ac.done <- true
}

func (s *Server) Register(method string, f Function) error {
	if _, exists := s.methods[method]; exists {
		return errors.New("Duplicate method name")
	}
	s.methods[method] = f
	return nil
}

func (s *Server) Shutdown() error {
	return s.ac.shutdown()
}
