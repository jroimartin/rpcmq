// Copyright 2014 The rpcmq Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rpcmq

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/streadway/amqp"
)

type Function func(data []byte) ([]byte, error)

type Server struct {
	queueName string
	ac        *amqpClient
	queue     amqp.Queue
	methods   map[string]Function

	wg              sync.WaitGroup
	parallelMethods chan bool
	deliveries      <-chan amqp.Delivery

	Parallel int
}

func NewServer(uri, queue string) *Server {
	s := &Server{
		queueName: queue,
		methods:   make(map[string]Function),
		ac:        newAmqpRpc(uri),
		Parallel:  4,
	}
	return s
}

func (s *Server) Init() error {
	if err := s.ac.init(); err != nil {
		return err
	}

	s.parallelMethods = make(chan bool, s.Parallel)

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
		s.parallelMethods <- true
		s.wg.Add(1)
		go s.handleDelivery(d)
	}
	s.wg.Wait()
	s.ac.done <- true
}

func (s *Server) handleDelivery(d amqp.Delivery) {
	defer func() {
		<-s.parallelMethods
		s.wg.Done()
	}()

	if d.CorrelationId == "" || d.ReplyTo == "" {
		d.Nack(false, false) // drop message
		log.Printf("dropped message: %+v\n", d)
		return
	}

	var (
		msg rpcMsg
		ret []byte
		err error
	)
	if err = json.Unmarshal(d.Body, &msg); err == nil {
		f, ok := s.methods[msg.Method]
		if ok {
			ret, err = f(msg.Data)
		} else {
			err = errors.New("method has not been registered")
		}
	} else {
		err = errors.New("cannot unmarshal message")
	}

	errStr := ""
	if err != nil {
		errStr = err.Error()
	}
	result := &Result{
		UUID: d.CorrelationId,
		Data: ret,
		Err:  errStr,
	}
	body, err := json.Marshal(result)
	if err != nil {
		d.Nack(false, true) // requeue message
		log.Printf("requeued message: %+v\n", d)
		return
	}

	s.ac.channel.Publish(
		"",        // exchange
		d.ReplyTo, // key
		false,     // mandatory
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
