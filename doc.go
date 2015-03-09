// Copyright 2015 The rpcmq Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
Package rpcmq implements an RPC protocol over AMQP.

Client/Server initialization

It is important to note that both clients and servers must be initialized
before being used. Also, any configuration parameter (e.g. TLSConfig or
Parallel) should be set up before calling Init().

SSL/TLS

When connecting to the broker via amqps protocol, the TLS configuration can be
set up using the TLSConfig parameter present in the Client and Server objects.
For more information, see the documentation of the package "crypto/tls".
*/
package rpcmq
