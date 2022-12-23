package object

import (
	"github.com/TrueCloudLab/frostfs-sdk-go/object"
	oid "github.com/TrueCloudLab/frostfs-sdk-go/object/id"
)

// AddressOf returns the address of the object.
func AddressOf(obj *object.Object) oid.Address {
	var addr oid.Address

	id, ok := obj.ID()
	if ok {
		addr.SetObject(id)
	}

	cnr, ok := obj.ContainerID()
	if ok {
		addr.SetContainer(cnr)
	}

	return addr
}
