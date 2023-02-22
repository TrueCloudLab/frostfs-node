package client

import (
	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/neorpc"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"go.uber.org/zap"
)

// Close closes connection to the remote side making
// this client instance unusable. Closes notification
// channel returned from Client.NotificationChannel(),
// Removes all subscription.
func (c *Client) Close() {
	// closing should be done via the channel
	// to prevent switching to another RPC node
	// in the notification loop
	close(c.closeChan)
}

// SubscribeForExecutionNotifications adds subscription for notifications
// generated during contract transaction execution to this instance of client.
//
// Returns ErrConnectionLost if client has not been able to establish
// connection to any of passed RPC endpoints.
func (c *Client) SubscribeForExecutionNotifications(contract util.Uint160) error {
	c.switchLock.Lock()
	defer c.switchLock.Unlock()

	if c.inactive {
		return ErrConnectionLost
	}

	_, subscribed := c.subscribedEvents[contract]
	if subscribed {
		// no need to subscribe one more time
		return nil
	}

	id, err := c.client.ReceiveExecutionNotifications(&neorpc.NotificationFilter{Contract: &contract}, c.notificationRcv)
	if err != nil {
		return err
	}

	c.subscribedEvents[contract] = id

	return nil
}

// SubscribeForNewBlocks adds subscription for new block events to this
// instance of client.
//
// Returns ErrConnectionLost if client has not been able to establish
// connection to any of passed RPC endpoints.
func (c *Client) SubscribeForNewBlocks() error {
	c.switchLock.Lock()
	defer c.switchLock.Unlock()

	if c.inactive {
		return ErrConnectionLost
	}

	if c.subscribedToNewBlocks {
		// no need to subscribe one more time
		return nil
	}

	_, err := c.client.ReceiveBlocks(nil, c.blockRcv)
	if err != nil {
		return err
	}

	c.subscribedToNewBlocks = true

	return nil
}

// SubscribeForNotaryRequests adds subscription for notary request payloads
// addition or removal events to this instance of client. Passed txSigner is
// used as filter: subscription is only for the notary requests that must be
// signed by txSigner.
//
// Returns ErrConnectionLost if client has not been able to establish
// connection to any of passed RPC endpoints.
func (c *Client) SubscribeForNotaryRequests(txSigner util.Uint160) error {
	if c.notary == nil {
		panic(notaryNotEnabledPanicMsg)
	}

	c.switchLock.Lock()
	defer c.switchLock.Unlock()

	if c.inactive {
		return ErrConnectionLost
	}

	_, subscribed := c.subscribedNotaryEvents[txSigner]
	if subscribed {
		// no need to subscribe one more time
		return nil
	}

	id, err := c.client.ReceiveNotaryRequests(&neorpc.TxFilter{Signer: &txSigner}, c.notaryReqRcv)
	if err != nil {
		return err
	}

	c.subscribedNotaryEvents[txSigner] = id

	return nil
}

// UnsubscribeContract removes subscription for given contract event stream.
//
// Returns ErrConnectionLost if client has not been able to establish
// connection to any of passed RPC endpoints.
func (c *Client) UnsubscribeContract(contract util.Uint160) error {
	c.switchLock.Lock()
	defer c.switchLock.Unlock()

	if c.inactive {
		return ErrConnectionLost
	}

	_, subscribed := c.subscribedEvents[contract]
	if !subscribed {
		// no need to unsubscribe contract
		// without subscription
		return nil
	}

	err := c.client.Unsubscribe(c.subscribedEvents[contract])
	if err != nil {
		return err
	}

	delete(c.subscribedEvents, contract)

	return nil
}

// UnsubscribeNotaryRequest removes subscription for given notary requests
// signer.
//
// Returns ErrConnectionLost if client has not been able to establish
// connection to any of passed RPC endpoints.
func (c *Client) UnsubscribeNotaryRequest(signer util.Uint160) error {
	if c.notary == nil {
		panic(notaryNotEnabledPanicMsg)
	}

	c.switchLock.Lock()
	defer c.switchLock.Unlock()

	if c.inactive {
		return ErrConnectionLost
	}

	_, subscribed := c.subscribedNotaryEvents[signer]
	if !subscribed {
		// no need to unsubscribe signer's
		// requests without subscription
		return nil
	}

	err := c.client.Unsubscribe(c.subscribedNotaryEvents[signer])
	if err != nil {
		return err
	}

	delete(c.subscribedNotaryEvents, signer)

	return nil
}

// UnsubscribeAll removes all active subscriptions of current client.
//
// Returns ErrConnectionLost if client has not been able to establish
// connection to any of passed RPC endpoints.
func (c *Client) UnsubscribeAll() error {
	c.switchLock.Lock()
	defer c.switchLock.Unlock()

	if c.inactive {
		return ErrConnectionLost
	}

	// no need to unsubscribe if there are
	// no active subscriptions
	if len(c.subscribedEvents) == 0 && len(c.subscribedNotaryEvents) == 0 &&
		!c.subscribedToNewBlocks {
		return nil
	}

	err := c.client.UnsubscribeAll()
	if err != nil {
		return err
	}

	c.subscribedEvents = make(map[util.Uint160]string)
	c.subscribedNotaryEvents = make(map[util.Uint160]string)
	c.subscribedToNewBlocks = false

	return nil
}

type subsInfo struct {
	blockRcv        chan *block.Block
	notificationRcv chan *state.ContainedNotificationEvent
	notaryReqRcv    chan *result.NotaryRequestEvent

	subscribedToBlocks     bool
	subscribedEvents       map[util.Uint160]string
	subscribedNotaryEvents map[util.Uint160]string
}

// restoreSubscriptions restores subscriptions according to cached
// information about them.
//
// If it is NOT a background operation switchLock MUST be held.
// Returns a pair: the second is a restoration status and the first
// one contains subscription information applied to the passed cli
// and receivers for the updated subscriptions.
// Does not change Client instance.
func (c *Client) restoreSubscriptions(cli *rpcclient.WSClient, endpoint string, background bool) (si subsInfo, ok bool) {
	var (
		err error
		id  string
	)

	stopCh := make(chan struct{})
	defer close(stopCh)

	blockRcv := make(chan *block.Block)
	notificationRcv := make(chan *state.ContainedNotificationEvent)
	notaryReqRcv := make(chan *result.NotaryRequestEvent)

	// neo-go WS client says to _always_ read notifications
	// from its channel. Subscribing to any notification
	// while not reading them in another goroutine may
	// lead to a dead-lock, thus that async side notification
	// listening while restoring subscriptions
	go func() {
		var e any
		var ok bool

		for {
			select {
			case <-stopCh:
				return
			case e, ok = <-blockRcv:
			case e, ok = <-notificationRcv:
			case e, ok = <-notaryReqRcv:
			}

			if !ok {
				return
			}

			if background {
				// background client (test) switch, no need to send
				// any notification, just preventing dead-lock
				continue
			}

			c.routeEvent(e)
		}
	}()

	if background {
		c.switchLock.RLock()
		defer c.switchLock.RUnlock()
	}

	si.subscribedToBlocks = c.subscribedToNewBlocks
	si.subscribedEvents = copySubsMap(c.subscribedEvents)
	si.subscribedNotaryEvents = copySubsMap(c.subscribedNotaryEvents)
	si.blockRcv = blockRcv
	si.notificationRcv = notificationRcv
	si.notaryReqRcv = notaryReqRcv

	// new block events restoration
	if si.subscribedToBlocks {
		_, err = cli.ReceiveBlocks(nil, blockRcv)
		if err != nil {
			c.logger.Error("could not restore block subscription after RPC switch",
				zap.String("endpoint", endpoint),
				zap.Error(err),
			)

			return
		}
	}

	// notification events restoration
	for contract := range si.subscribedEvents {
		contract := contract // See https://github.com/nspcc-dev/neo-go/issues/2890
		id, err = cli.ReceiveExecutionNotifications(&neorpc.NotificationFilter{Contract: &contract}, notificationRcv)
		if err != nil {
			c.logger.Error("could not restore notification subscription after RPC switch",
				zap.String("endpoint", endpoint),
				zap.Error(err),
			)

			return
		}

		si.subscribedEvents[contract] = id
	}

	// notary notification events restoration
	if c.notary != nil {
		for signer := range si.subscribedNotaryEvents {
			signer := signer // See https://github.com/nspcc-dev/neo-go/issues/2890
			id, err = cli.ReceiveNotaryRequests(&neorpc.TxFilter{Signer: &signer}, notaryReqRcv)
			if err != nil {
				c.logger.Error("could not restore notary notification subscription after RPC switch",
					zap.String("endpoint", endpoint),
					zap.Error(err),
				)

				return
			}

			si.subscribedNotaryEvents[signer] = id
		}
	}

	return si, true
}

func copySubsMap(m map[util.Uint160]string) map[util.Uint160]string {
	newM := make(map[util.Uint160]string, len(m))
	for k, v := range m {
		newM[k] = v
	}

	return newM
}
