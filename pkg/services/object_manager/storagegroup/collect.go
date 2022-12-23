package storagegroup

import (
	objutil "github.com/TrueCloudLab/frostfs-node/pkg/services/object/util"
	"github.com/TrueCloudLab/frostfs-sdk-go/checksum"
	cid "github.com/TrueCloudLab/frostfs-sdk-go/container/id"
	"github.com/TrueCloudLab/frostfs-sdk-go/object"
	oid "github.com/TrueCloudLab/frostfs-sdk-go/object/id"
	"github.com/TrueCloudLab/frostfs-sdk-go/storagegroup"
	"github.com/TrueCloudLab/tzhash/tz"
)

// CollectMembers creates new storage group structure and fills it
// with information about members collected via HeadReceiver.
//
// Resulting storage group consists of physically stored objects only.
func CollectMembers(r objutil.HeadReceiver, cnr cid.ID, members []oid.ID, calcHomoHash bool) (*storagegroup.StorageGroup, error) {
	var (
		sumPhySize uint64
		phyMembers []oid.ID
		phyHashes  [][]byte
		addr       oid.Address
		sg         storagegroup.StorageGroup
	)

	addr.SetContainer(cnr)

	for i := range members {
		addr.SetObject(members[i])

		if err := objutil.IterateAllSplitLeaves(r, addr, func(leaf *object.Object) {
			id, ok := leaf.ID()
			if !ok {
				return
			}

			phyMembers = append(phyMembers, id)
			sumPhySize += leaf.PayloadSize()
			cs, _ := leaf.PayloadHomomorphicHash()

			if calcHomoHash {
				phyHashes = append(phyHashes, cs.Value())
			}
		}); err != nil {
			return nil, err
		}
	}

	sg.SetMembers(phyMembers)
	sg.SetValidationDataSize(sumPhySize)

	if calcHomoHash {
		sumHash, err := tz.Concat(phyHashes)
		if err != nil {
			return nil, err
		}

		var cs checksum.Checksum
		tzHash := [64]byte{}
		copy(tzHash[:], sumHash)
		cs.SetTillichZemor(tzHash)

		sg.SetValidationDataHash(cs)
	}

	return &sg, nil
}
