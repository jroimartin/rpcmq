// Copyright 2014 The rpcmq Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rpcmq

import (
	"errors"
	"testing"
)

func TestServer(t *testing.T) {
	s := NewServer("amqp://127.0.0.1:5672", "rcp-queue")
	if err := s.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	defer s.Shutdown()
	if err := s.Register("sum", sum); err != nil {
		t.Fatalf("Register: %v", err)
	}
	select {}
}

func sum(args ...interface{}) (interface{}, error) {
	var result float64
	for _, n := range args {
		ni, ok := n.(float64)
		if !ok {
			return 0, errors.New("argument is not float64")
		}
		result += ni
	}
	return result, nil
}
