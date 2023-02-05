package session

import (
	"context"

	"github.com/TrueCloudLab/frostfs-api-go/v2/session"
)

// Server is an interface of the FrostFS API Session service server.
type Server interface {
	Create(context.Context, *session.CreateRequest) (*session.CreateResponse, error)
}
