// Copyright 2015 The rpcmq Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"

	"github.com/jroimartin/rpcmq"
)

func main() {
	certFile := os.Getenv("RPCMQ_CERT")
	keyFile := os.Getenv("RPCMQ_KEY")
	caFile := os.Getenv("RPCMQ_CA")

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		log.Fatalf("LoadX509KeyPair: %v", err)
	}
	caCert, err := ioutil.ReadFile(caFile)
	if err != nil {
		log.Fatalf("ReadFile: %v", err)
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caCertPool,
	}

	s := rpcmq.NewServer("amqps://amqp_broker:5671", "rcp-queue",
		"rpc-exchange", "direct")
	s.TLSConfig = tlsConfig
	if err := s.Register("toUpper", toUpper); err != nil {
		log.Fatalf("Register: %v", err)
	}
	if err := s.Init(); err != nil {
		log.Fatalf("Init: %v", err)
	}
	defer s.Shutdown()

	<-time.After(10 * time.Second)
}

func toUpper(id string, data []byte) ([]byte, error) {
	log.Printf("Received (%v): toUpper(%v)\n", id, string(data))
	return []byte(strings.ToUpper(string(data))), nil
}
