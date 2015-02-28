// Copyright 2014 The rpcmq Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rpcmq

import (
	"log"
	"testing"
)

func TestClient(t *testing.T) {
	c := NewClient("amqp://127.0.0.1:5672", "rcp-queue")
	if err := c.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	defer c.Shutdown()
	uuid, err := c.Call("sum", 1, 2, 3, 4, 5, -12)
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	r := <-c.Results()
	if r.UUID == uuid {
		if ret, ok := r.Val.(float64); ok {
			log.Printf("Received: %v (%v)\n", ret, r.UUID)
		} else {
			t.Fatalf("return values is not int")
		}
	}
}
