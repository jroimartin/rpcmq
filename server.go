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

type Function func(data []byte) ([]byte, error)

type Server struct {
	queueName  string
	ac         *amqpClient
	queue      amqp.Queue
	deliveries <-chan amqp.Delivery
	methods    map[string]Function
}

func NewServer(uri, queue string) *Server {
	s := &Server{
		queueName: queue,
		methods:   make(map[string]Function),
		ac:        newAmqpRpc(uri),
	}
	return s
}

func (s *Server) Init() error {
	if err := s.ac.init(); err != nil {
		return err
	}

	var err error
	s.queue, err = s.ac.channel.QueueDeclare(
		s.queueName, // name
		true,        // durable
		false,       // autoDelete
		false,       // exclusive
		false,       // noWait
		nil,         // args
	)
	if err != nil {
		return fmt.Errorf("Queue Declare: %v", err)
	}

	s.ac.consumerTag, err = UUID()
	if err != nil {
		return fmt.Errorf("UUID: %v", err)
	}

	s.deliveries, err = s.ac.channel.Consume(
		s.queueName,      // name
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
			ret []byte
			err error
		)
		f, ok := s.methods[msg.Method]
		if ok {
			ret, err = f(msg.Data)
		} else {
			err = errors.New("method has not been registered")
		}

		result := &Result{
			UUID: d.CorrelationId,
			Data: ret,
			Err:  err,
		}
		body, err := json.Marshal(result)
		if err != nil {
			d.Nack(false, true) // requeue
			continue
		}

		s.ac.channel.Publish(
			"",        // exchange
			d.ReplyTo, // key
			true,      // mandatory
			false,     // immediate
			amqp.Publishing{ // msg
				CorrelationId: d.CorrelationId,
				ContentType:   "application/json",
				Body:          body,
				DeliveryMode:  amqp.Persistent,
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
