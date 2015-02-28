// Copyright 2014 The rpcmq Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rpcmq

import (
	"crypto/rand"
	"encoding/hex"
)

func UUID() (string, error) {
	uuid := make([]byte, 16)
	if _, err := rand.Read(uuid); err != nil {
		return "", err
	}
	return hex.EncodeToString(uuid), nil
}
