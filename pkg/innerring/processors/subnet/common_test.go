package subnetevents

import subnetid "github.com/TrueCloudLab/frostfs-sdk-go/subnet/id"

type idEvent struct {
	id subnetid.ID

	idErr error
}

func (x idEvent) ReadID(id *subnetid.ID) error {
	if x.idErr != nil {
		return x.idErr
	}

	*id = x.id

	return nil
}
