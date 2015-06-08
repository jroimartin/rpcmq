// Copyright 2015 The rpcmq Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rpcmq

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"sync"

	"github.com/streadway/amqp"
)

// The type Function declares the signature of the methods that can be
// registered by an RPC server. The id parameter contains the uuid of
// the task being executed.
type Function func(id string, data []byte) ([]byte, error)

// A Server is an RPC sever, which is used to register the methods than can be
// invoked remotely.
type Server struct {
	msgsName     string
	exchangeName string
	exchangeKind string
	ac           *amqpClient
	msgsQueue    amqp.Queue
	methods      map[string]Function

	wg              sync.WaitGroup
	parallelMethods chan bool
	deliveries      <-chan amqp.Delivery

	// Parallel allows to define the number of methods to be run in
	// parallel
	Parallel int
	// Prefetch allows to define the number of tasks to be "cached"
	Prefetch int
	// TLSConfig allows to configure the TLS parameters used to connect to
	// the broker via amqps
	TLSConfig *tls.Config
}

// NewServer returns a reference to a Server object. The paremeter uri is the
// network address of the broker and msgsQueue is the name of queue that will
// be created to exchange the messages between clients and servers. On the
// other hand, the parameters exchange and kind determine the type of exchange
// that will be created. In fanout mode the queue name is ignored, so each
// queue has its own unique id.
func NewServer(uri, msgsQueue, exchange, kind string) *Server {
	if kind == "fanout" {
		msgsQueue = "" // in fanout mode queue names must be unique
	}
	s := &Server{
		msgsName:     msgsQueue,
		exchangeName: exchange,
		exchangeKind: kind,
		ac:           newAmqpClient(uri),
		methods:      make(map[string]Function),
		Parallel:     runtime.NumCPU(),
		Prefetch:     4,
	}
	s.ac.setupFunc = s.setup
	return s
}

// Init initializes the Server object. It establishes the connection with the
// broker, creating a channel and the queues that will be used under the hood.
func (s *Server) Init() error {
	s.ac.tlsConfig = s.TLSConfig
	if err := s.ac.init(); err != nil {
		return err
	}
	return s.setup()
}

func (s *Server) setup() error {
	// Limit the number of reserved messages
	err := s.ac.channel.Qos(
		s.Prefetch, // prefetchCount
		0,          // prefetchSize
		false,      // global
	)
	if err != nil {
		return fmt.Errorf("Qos: %v", err)
	}
	s.parallelMethods = make(chan bool, s.Parallel)

	err = s.ac.channel.ExchangeDeclare(
		s.exchangeName, // name
		s.exchangeKind, // kind
		true,           // durable
		false,          // autoDelete
		false,          // internal
		false,          // noWait
		nil,            // args
	)
	if err != nil {
		return fmt.Errorf("ExchangeDeclare: %v", err)
	}

	durable, autoDelete := true, false
	if s.exchangeKind == "fanout" {
		// In fanout mode the queue must be deleted on server restart
		// or when no consumer is using it
		durable = false
		autoDelete = true
	}
	s.msgsQueue, err = s.ac.channel.QueueDeclare(
		s.msgsName, // name
		durable,    // durable
		autoDelete, // autoDelete
		false,      // exclusive
		false,      // noWait
		nil,        // args
	)
	if err != nil {
		return fmt.Errorf("QueueDeclare: %v", err)
	}

	err = s.ac.channel.QueueBind(
		s.msgsQueue.Name, // name
		s.msgsQueue.Name, // key
		s.exchangeName,   // exchange
		false,            // noWait
		nil,              // args
	)
	if err != nil {
		return fmt.Errorf("QueueBind: %v", err)
	}

	s.ac.consumerTag, err = uuid()
	if err != nil {
		return fmt.Errorf("UUID: %v", err)
	}

	s.deliveries, err = s.ac.channel.Consume(
		s.msgsQueue.Name, // name
		s.ac.consumerTag, // consumer
		false,            // autoAck
		false,            // exclusive
		false,            // noLocal
		false,            // noWait
		nil,              // args
	)
	if err != nil {
		return fmt.Errorf("QueueConsume: %v", err)
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
		logf("dropped message: %+v", d)
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
			ret, err = f(d.CorrelationId, msg.Data)
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
		logf("requeued message: %+v", d)
		return
	}

	// guarantee that the received ack/nack corresponds with this publishing
	s.ac.mu.Lock()
	defer s.ac.mu.Unlock()

	err = s.ac.channel.Publish(
		"",        // exchange
		d.ReplyTo, // key
		false,     // mandatory
		false,     // immediate
		amqp.Publishing{ // msg
			CorrelationId: d.CorrelationId,
			ReplyTo:       d.ReplyTo,
			ContentType:   "application/json",
			Body:          body,
			DeliveryMode:  amqp.Persistent,
		},
	)
	if err != nil {
		d.Nack(false, true) // requeue message
		return
	}

	select {
	case _, ok := <-s.ac.acks:
		if ok {
			d.Ack(false)
			return
		}
	case tag, ok := <-s.ac.nacks:
		if ok {
			logf("nack recived (%v)", tag)
			d.Nack(false, true) // requeue message
			return
		}
	}

	logf("missing ack/nack")
	d.Nack(false, true) // requeue message
}

// Register registers a method with the name given by the parameter method and
// links the function f to it. Register should be called before Init() to avoid
// dropping messages due to "not registered method" errors.
func (s *Server) Register(method string, f Function) error {
	if _, exists := s.methods[method]; exists {
		return errors.New("Duplicate method name")
	}
	s.methods[method] = f
	return nil
}

// Shutdown shuts down the server gracefully. Using this method will ensure
// that all requests sent by the RPC clients to the server will be handled by
// the latter.
func (s *Server) Shutdown() {
	s.ac.shutdown()
}
