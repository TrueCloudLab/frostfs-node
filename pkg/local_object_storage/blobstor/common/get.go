package common

import (
	objectSDK "github.com/TrueCloudLab/frostfs-sdk-go/object"
	oid "github.com/TrueCloudLab/frostfs-sdk-go/object/id"
)

type GetPrm struct {
	Address   oid.Address
	StorageID []byte
	Raw       bool
}

type GetRes struct {
	Object  *objectSDK.Object
	RawData []byte
}
