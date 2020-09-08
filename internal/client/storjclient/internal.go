// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package storjclient

import (
	"errors"

	"github.com/btcsuite/btcutil/base58"
	"github.com/gogo/protobuf/proto"

	"storj.io/common/pb"
)

// parseSatelliteAddressFromScope hack to get scope internals.
func parseSatelliteAddressFromScope(scope string) (string, error) {
	data, version, err := base58.CheckDecode(scope)
	if err != nil || version != 0 {
		return "", errors.New("invalid scope")
	}

	p := new(pb.Scope)
	if err := proto.Unmarshal(data, p); err != nil {
		return "", err
	}
	return p.SatelliteAddr, nil
}
