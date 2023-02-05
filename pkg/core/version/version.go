package version

import (
	"github.com/TrueCloudLab/frostfs-sdk-go/version"
)

// IsValid checks if Version is not earlier than the genesis version of the FrostFS.
func IsValid(v version.Version) bool {
	const (
		startMajor = 2
		startMinor = 7
	)

	mjr := v.Major()

	return mjr > startMajor || mjr == startMajor && v.Minor() >= startMinor
}
