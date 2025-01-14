package response

import (
	"fmt"

	"github.com/TrueCloudLab/frostfs-node/pkg/services/util"
)

// ServerMessageStreamer represents server-side message streamer
// that sets meta values to all response messages.
type ServerMessageStreamer struct {
	cfg *cfg

	recv util.ResponseMessageReader
}

// Recv calls Recv method of internal streamer, sets response meta
// values and returns the response.
func (s *ServerMessageStreamer) Recv() (util.ResponseMessage, error) {
	m, err := s.recv()
	if err != nil {
		return nil, fmt.Errorf("could not receive response message for signing: %w", err)
	}

	setMeta(m, s.cfg)

	return m, nil
}

// HandleServerStreamRequest builds internal streamer via handlers, wraps it to ServerMessageStreamer and returns the result.
func (s *Service) HandleServerStreamRequest(respWriter util.ResponseMessageWriter) util.ResponseMessageWriter {
	return func(resp util.ResponseMessage) error {
		setMeta(resp, s.cfg)

		return respWriter(resp)
	}
}
