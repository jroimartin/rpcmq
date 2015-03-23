// Copyright 2015 The rpcmq Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rpcmq

import "log"

// Log is the logger used to register warnings and info messages. If it is nil,
// no messages will be logged.
var Log *log.Logger

func logf(format string, args ...interface{}) {
	if Log == nil {
		return
	}
	Log.Printf(format, args...)
}
