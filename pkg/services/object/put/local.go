package putsvc

import (
	"fmt"

	objectCore "github.com/TrueCloudLab/frostfs-node/pkg/core/object"
	"github.com/TrueCloudLab/frostfs-sdk-go/object"
	oid "github.com/TrueCloudLab/frostfs-sdk-go/object/id"
)

// ObjectStorage is an object storage interface.
type ObjectStorage interface {
	// Put must save passed object
	// and return any appeared error.
	Put(*object.Object) error
	// Delete must delete passed objects
	// and return any appeared error.
	Delete(tombstone oid.Address, toDelete []oid.ID) error
	// Lock must lock passed objects
	// and return any appeared error.
	Lock(locker oid.Address, toLock []oid.ID) error
}

type localTarget struct {
	storage ObjectStorage
}

func (t localTarget) WriteObject(obj *object.Object, meta objectCore.ContentMeta) error {
	switch meta.Type() {
	case object.TypeTombstone:
		err := t.storage.Delete(objectCore.AddressOf(obj), meta.Objects())
		if err != nil {
			return fmt.Errorf("could not delete objects from tombstone locally: %w", err)
		}
	case object.TypeLock:
		err := t.storage.Lock(objectCore.AddressOf(obj), meta.Objects())
		if err != nil {
			return fmt.Errorf("could not lock object from lock objects locally: %w", err)
		}
	default:
		// objects that do not change meta storage
	}

	if err := t.storage.Put(obj); err != nil {
		return fmt.Errorf("(%T) could not put object to local storage: %w", t, err)
	}
	return nil
}
