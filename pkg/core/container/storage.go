package container

import (
	"errors"

	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
)

// Source is an interface that wraps
// basic container receiving method.
type Source interface {
	// Get reads the container from the storage by its identifier.
	// It returns the pointer to the requested container and any error encountered.
	//
	// Get must return exactly one non-nil value.
	// Get must return an error of type apistatus.ContainerNotFound if the container is not in the storage.
	//
	// Implementations must not retain the container pointer and modify
	// the container through it.
	Get(cid.ID) (*container.Container, error)
}

// IsErrNotFound checks if the error returned by Source.Get corresponds
// to the missing container.
func IsErrNotFound(err error) bool {
	return errors.As(err, new(apistatus.ContainerNotFound))
}

// ErrEACLNotFound is returned by eACL storage implementations when
// the requested eACL table is not in the storage.
var ErrEACLNotFound = errors.New("extended ACL table is not set for this container")
