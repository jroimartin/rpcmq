// Copyright 2015 The rpcmq Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rpcmq

import (
	"crypto/tls"
	"fmt"

	"github.com/streadway/amqp"
)

type amqpClient struct {
	uri         string
	consumerTag string

	conn    *amqp.Connection
	channel *amqp.Channel
	returns chan amqp.Return
	done    chan bool

	tlsConfig *tls.Config
}

func newAmqpRpc(uri string) *amqpClient {
	r := &amqpClient{
		uri:  uri,
		done: make(chan bool),
	}
	return r
}

func (r *amqpClient) init() error {
	var err error
	r.conn, err = amqp.DialTLS(r.uri, r.tlsConfig)
	if err != nil {
		return fmt.Errorf("DialTLS: %v", err)
	}

	r.channel, err = r.conn.Channel()
	if err != nil {
		return fmt.Errorf("Channel: %v", err)
	}

	r.returns = make(chan amqp.Return) // closed by Channel.NotifyReturn
	go r.trackErrors()

	return nil
}

func (r *amqpClient) trackErrors() {
	for ret := range r.channel.NotifyReturn(r.returns) {
		if ret.ReplyCode == amqp.NoRoute {
			panic("no route")
		}
	}
}

func (r *amqpClient) shutdown() error {
	if r.consumerTag != "" {
		if err := r.channel.Cancel(r.consumerTag, false); err != nil {
			return fmt.Errorf("Channel Cancel: %v", err)
		}
	}
	<-r.done

	if err := r.channel.Close(); err != nil {
		return fmt.Errorf("Channel Close: %v", err)
	}
	if err := r.conn.Close(); err != nil {
		return fmt.Errorf("Connection Close: %v", err)
	}

	return nil
}
