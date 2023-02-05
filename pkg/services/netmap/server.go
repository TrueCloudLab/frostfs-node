package netmap

import (
	"context"

	"github.com/TrueCloudLab/frostfs-api-go/v2/netmap"
)

// Server is an interface of the FrostFS API Netmap service server.
type Server interface {
	LocalNodeInfo(context.Context, *netmap.LocalNodeInfoRequest) (*netmap.LocalNodeInfoResponse, error)
	NetworkInfo(context.Context, *netmap.NetworkInfoRequest) (*netmap.NetworkInfoResponse, error)
	Snapshot(context.Context, *netmap.SnapshotRequest) (*netmap.SnapshotResponse, error)
}
