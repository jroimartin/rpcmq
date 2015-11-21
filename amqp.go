// Copyright 2015 The rpcmq Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rpcmq

import (
	"crypto/tls"
	"fmt"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

// RetrySleep is the time between retries if the connection with the broker
// is lost.
var RetrySleep = 2 * time.Second

type publishing struct {
	msg       amqp.Publishing
	timestamp time.Time
}

// DeliveryMode represents the boker's delivery mode.
type DeliveryMode uint8

// Delivery modes.
const (
	// Persistent delivery mode means that messages will be restored to
	// durable queues during server restart.
	Persistent = DeliveryMode(amqp.Persistent)

	// Transient delivery mode means higher throughput but messages will
	// not be restored on broker restart.
	Transient = DeliveryMode(amqp.Transient)
)

type amqpClient struct {
	uri         string
	consumerTag string

	conn    *amqp.Connection
	channel *amqp.Channel
	done    chan bool

	// The mutex protects the acks/nacks channels from being used
	// simultaneously by multiple publishing processes.
	mu          sync.Mutex
	acks, nacks chan uint64

	setupFunc func() error

	tlsConfig *tls.Config
}

func newAmqpClient(uri string) *amqpClient {
	ac := &amqpClient{
		uri:  uri,
		done: make(chan bool),
	}
	return ac
}

func (ac *amqpClient) init() error {
	var err error
	ac.conn, err = amqp.DialTLS(ac.uri, ac.tlsConfig)
	if err != nil {
		return fmt.Errorf("DialTLS: %v", err)
	}

	ac.channel, err = ac.conn.Channel()
	if err != nil {
		return fmt.Errorf("Channel: %v", err)
	}
	if err := ac.channel.Confirm(false); err != nil {
		return fmt.Errorf("Confirm: %v", err)
	}
	ac.acks, ac.nacks = ac.channel.NotifyConfirm(make(chan uint64), make(chan uint64))

	go ac.handleMsgs()

	return nil
}

func (ac *amqpClient) handleMsgs() {
	returns := ac.channel.NotifyReturn(make(chan amqp.Return))
	errors := ac.channel.NotifyClose(make(chan *amqp.Error))

	for {
		select {
		case ret, ok := <-returns:
			if !ok {
				logf("returns channel closed")
				return
			}
			if ret.ReplyCode == amqp.NoRoute {
				panic("no route")
			}
		case _, ok := <-errors:
			if !ok {
				logf("errors channel closed")
				return
			}
			logf("shutdown")
			ac.shutdown()
			for {
				logf("retrying")
				if err := ac.init(); err != nil {
					logf("amqpClient init: %v", err)
					time.Sleep(RetrySleep)
					continue
				}

				if err := ac.setupFunc(); err != nil {
					panic(fmt.Errorf("setup: %v", err))
				}
				logf("connected")
				return
			}
		}
	}
}

func (ac *amqpClient) shutdown() {
	if ac.consumerTag != "" {
		if err := ac.channel.Cancel(ac.consumerTag, false); err != nil {
			logf("Channel Cancel: %v", err)
		}
	}
	<-ac.done
	if err := ac.channel.Close(); err != nil {
		logf("Channel Close: %v", err)
	}
	if err := ac.conn.Close(); err != nil {
		logf("Connection Close: %v", err)
	}
}
