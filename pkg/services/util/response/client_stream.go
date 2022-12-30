package response

import (
	"fmt"

	"github.com/TrueCloudLab/frostfs-node/pkg/services/util"
)

// ClientMessageStreamer represents client-side message streamer
// that sets meta values to the response.
type ClientMessageStreamer struct {
	srv *Service

	send util.RequestMessageWriter

	close util.ClientStreamCloser
}

// Send calls send method of internal streamer.
func (s *ClientMessageStreamer) Send(req any) error {
	if err := s.send(req); err != nil {
		return fmt.Errorf("(%T) could not send the request: %w", s, err)
	}
	return nil
}

// CloseAndRecv closes internal stream, receivers the response,
// sets meta values and returns the result.
func (s *ClientMessageStreamer) CloseAndRecv() (util.ResponseMessage, error) {
	resp, err := s.close()
	if err != nil {
		return nil, fmt.Errorf("(%T) could not close stream and receive response: %w", s, err)
	}

	s.srv.SetMeta(resp)

	return resp, nil
}

// CreateRequestStreamer wraps stream methods and returns ClientMessageStreamer instance.
func (s *Service) CreateRequestStreamer(sender util.RequestMessageWriter, closer util.ClientStreamCloser) *ClientMessageStreamer {
	return &ClientMessageStreamer{
		srv:   s,
		send:  sender,
		close: closer,
	}
}
