package blobstor

import (
	storagelog "github.com/TrueCloudLab/frostfs-node/pkg/local_object_storage/internal/log"
	"github.com/TrueCloudLab/frostfs-node/pkg/util/logger"
	oid "github.com/TrueCloudLab/frostfs-sdk-go/object/id"
)

const deleteOp = "DELETE"
const putOp = "PUT"

func logOp(l *logger.Logger, op string, addr oid.Address, typ string, sID []byte) {
	storagelog.Write(l,
		storagelog.AddressField(addr),
		storagelog.OpField(op),
		storagelog.StorageTypeField(typ),
		storagelog.StorageIDField(sID),
	)
}
