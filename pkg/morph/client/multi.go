package client

import (
	"sort"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/neorpc"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient"
	"go.uber.org/zap"
)

// Endpoint represents morph endpoint together with its priority.
type Endpoint struct {
	Address  string
	Priority int
}

type endpoints struct {
	curr int
	list []Endpoint
}

func (e *endpoints) init(ee []Endpoint) {
	sort.SliceStable(ee, func(i, j int) bool {
		return ee[i].Priority < ee[j].Priority
	})

	e.curr = 0
	e.list = ee
}

func (c *Client) switchRPC() bool {
	c.switchLock.Lock()
	defer c.switchLock.Unlock()

	c.client.Close()

	// Iterate endpoints in the order of decreasing priority.
	for c.endpoints.curr = range c.endpoints.list {
		newEndpoint := c.endpoints.list[c.endpoints.curr].Address
		cli, act, err := c.newCli(newEndpoint)
		if err != nil {
			c.logger.Warn("could not establish connection to the switched RPC node",
				zap.String("endpoint", newEndpoint),
				zap.Error(err),
			)

			continue
		}

		c.cache.invalidate()

		c.logger.Info("connection to the new RPC node has been established",
			zap.String("endpoint", newEndpoint))

		subs, ok := c.restoreSubscriptions(cli, newEndpoint, false)
		if !ok {
			// new WS client does not allow
			// restoring subscription, client
			// could not work correctly =>
			// closing connection to RPC node
			// to switch to another one
			cli.Close()
			continue
		}

		c.client = cli
		c.setActor(act)
		c.updateSubs(subs)

		if c.cfg.switchInterval != 0 && !c.switchIsActive.Load() &&
			c.endpoints.list[c.endpoints.curr].Priority != c.endpoints.list[0].Priority {
			c.switchIsActive.Store(true)
			go c.switchToMostPrioritized()
		}

		return true
	}

	return false
}

func (c *Client) notificationLoop() {
	var e any
	var ok bool

	for {
		c.switchLock.RLock()
		bChan := c.blockRcv
		nChan := c.notificationRcv
		nrChan := c.notaryReqRcv
		c.switchLock.RUnlock()

		select {
		case <-c.cfg.ctx.Done():
			_ = c.UnsubscribeAll()
			c.close()

			return
		case <-c.closeChan:
			_ = c.UnsubscribeAll()
			c.close()

			return
		case e, ok = <-bChan:
		case e, ok = <-nChan:
		case e, ok = <-nrChan:
		}

		if ok {
			c.routeEvent(e)
			continue
		}

		if !c.reconnect() {
			return
		}
	}
}

func (c *Client) routeEvent(e any) {
	typedNotification := rpcclient.Notification{Value: e}

	switch e.(type) {
	case *block.Block:
		typedNotification.Type = neorpc.BlockEventID
	case *state.ContainedNotificationEvent:
		typedNotification.Type = neorpc.NotificationEventID
	case *result.NotaryRequestEvent:
		typedNotification.Type = neorpc.NotaryRequestEventID
	}

	select {
	case c.notifications <- typedNotification:
	case <-c.cfg.ctx.Done():
		_ = c.UnsubscribeAll()
		c.close()
	case <-c.closeChan:
		_ = c.UnsubscribeAll()
		c.close()
	}
}

func (c *Client) reconnect() bool {
	if closeErr := c.client.GetError(); closeErr != nil {
		c.logger.Warn("switching to the next RPC node",
			zap.String("reason", closeErr.Error()),
		)
	} else {
		// neo-go client was closed by calling `Close`
		// method, that happens only when a client has
		// switched to the more prioritized RPC
		return true
	}

	if !c.switchRPC() {
		c.logger.Error("could not establish connection to any RPC node")

		// could not connect to all endpoints =>
		// switch client to inactive mode
		c.inactiveMode()

		return false
	}

	// TODO(@carpawell): call here some callback retrieved in constructor
	// of the client to allow checking chain state since during switch
	// process some notification could be lost

	return true
}

func (c *Client) switchToMostPrioritized() {
	t := time.NewTicker(c.cfg.switchInterval)
	defer t.Stop()
	defer c.switchIsActive.Store(false)

mainLoop:
	for {
		select {
		case <-c.cfg.ctx.Done():
			return
		case <-t.C:
			c.switchLock.RLock()

			endpointsCopy := make([]Endpoint, len(c.endpoints.list))
			copy(endpointsCopy, c.endpoints.list)
			currPriority := c.endpoints.list[c.endpoints.curr].Priority
			highestPriority := c.endpoints.list[0].Priority

			c.switchLock.RUnlock()

			if currPriority == highestPriority {
				// already connected to
				// the most prioritized
				return
			}

			for i, e := range endpointsCopy {
				if currPriority == e.Priority {
					// a switch will not increase the priority
					continue mainLoop
				}

				tryE := e.Address

				cli, act, err := c.newCli(tryE)
				if err != nil {
					c.logger.Warn("could not create client to the higher priority node",
						zap.String("endpoint", tryE),
						zap.Error(err),
					)
					continue
				}

				if subs, ok := c.restoreSubscriptions(cli, tryE, true); ok {
					c.switchLock.Lock()

					// higher priority node could have been
					// connected in the other goroutine
					if e.Priority >= c.endpoints.list[c.endpoints.curr].Priority {
						cli.Close()
						c.switchLock.Unlock()
						return
					}

					c.client.Close()
					c.cache.invalidate()
					c.client = cli
					c.setActor(act)
					c.updateSubs(subs)
					c.endpoints.curr = i

					c.switchLock.Unlock()

					c.logger.Info("switched to the higher priority RPC",
						zap.String("endpoint", tryE))

					return
				}

				c.logger.Warn("could not restore side chain subscriptions using node",
					zap.String("endpoint", tryE),
					zap.Error(err),
				)
			}
		}
	}
}

// close closes notification channel and wrapped WS client.
func (c *Client) close() {
	close(c.notifications)
	c.client.Close()
}
