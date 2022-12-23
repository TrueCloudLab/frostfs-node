package searchsvc

import (
	"github.com/TrueCloudLab/frostfs-api-go/v2/object"
	"github.com/TrueCloudLab/frostfs-api-go/v2/refs"
	objectSvc "github.com/TrueCloudLab/frostfs-node/pkg/services/object"
	oid "github.com/TrueCloudLab/frostfs-sdk-go/object/id"
)

type streamWriter struct {
	stream objectSvc.SearchStream
}

func (s *streamWriter) WriteIDs(ids []oid.ID) error {
	r := new(object.SearchResponse)

	body := new(object.SearchResponseBody)
	r.SetBody(body)

	idsV2 := make([]refs.ObjectID, len(ids))

	for i := range ids {
		ids[i].WriteToV2(&idsV2[i])
	}

	body.SetIDList(idsV2)

	return s.stream.Send(r)
}
