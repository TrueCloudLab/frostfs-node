package netmap

// State groups the current system state parameters.
type State interface {
	// CurrentEpoch returns the number of the current FrostFS epoch.
	CurrentEpoch() uint64
}
