package container

import (
	"errors"

	apistatus "github.com/TrueCloudLab/frostfs-sdk-go/client/status"
	"github.com/TrueCloudLab/frostfs-sdk-go/container"
	cid "github.com/TrueCloudLab/frostfs-sdk-go/container/id"
	frostfscrypto "github.com/TrueCloudLab/frostfs-sdk-go/crypto"
	"github.com/TrueCloudLab/frostfs-sdk-go/eacl"
	"github.com/TrueCloudLab/frostfs-sdk-go/session"
)

// Container groups information about the FrostFS container stored in the FrostFS network.
type Container struct {
	// Container structure.
	Value container.Container

	// Signature of the Value.
	Signature frostfscrypto.Signature

	// Session within which Value was created. Nil means session absence.
	Session *session.Container
}

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
	Get(cid.ID) (*Container, error)
}

// IsErrNotFound checks if the error returned by Source.Get corresponds
// to the missing container.
func IsErrNotFound(err error) bool {
	return errors.As(err, new(apistatus.ContainerNotFound))
}

// EACL groups information about the FrostFS container's extended ACL stored in
// the FrostFS network.
type EACL struct {
	// Extended ACL structure.
	Value *eacl.Table

	// Signature of the Value.
	Signature frostfscrypto.Signature

	// Session within which Value was set. Nil means session absence.
	Session *session.Container
}

// EACLSource is the interface that wraps
// basic methods of extended ACL table source.
type EACLSource interface {
	// GetEACL reads the table from the source by identifier.
	// It returns any error encountered.
	//
	// GetEACL must return exactly one non-nil value.
	//
	// Must return apistatus.ErrEACLNotFound if requested
	// eACL table is not in source.
	GetEACL(cid.ID) (*EACL, error)
}
