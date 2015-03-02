// Copyright 2014 The rpcmq Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rpcmq

import (
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
}

func newAmqpRpc(uri string) *amqpClient {
	r := &amqpClient{
		returns: make(chan amqp.Return),
		done:    make(chan bool),
		uri:     uri,
	}
	return r
}

func (r *amqpClient) init() error {
	var err error
	// TODO(jrm): support TLS
	r.conn, err = amqp.Dial(r.uri)
	if err != nil {
		return fmt.Errorf("Dial: %v", err)
	}

	r.channel, err = r.conn.Channel()
	if err != nil {
		return fmt.Errorf("Channel: %v", err)
	}

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
			return fmt.Errorf("Channel Close: %v", err)
		}
	}
	<-r.done

	if err := r.conn.Close(); err != nil {
		return fmt.Errorf("Connection Close: %v", err)
	}

	return nil
}
