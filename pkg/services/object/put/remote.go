package putsvc

import (
	"context"
	"fmt"

	clientcore "github.com/TrueCloudLab/frostfs-node/pkg/core/client"
	netmapCore "github.com/TrueCloudLab/frostfs-node/pkg/core/netmap"
	objectcore "github.com/TrueCloudLab/frostfs-node/pkg/core/object"
	internalclient "github.com/TrueCloudLab/frostfs-node/pkg/services/object/internal/client"
	"github.com/TrueCloudLab/frostfs-node/pkg/services/object/util"
	"github.com/TrueCloudLab/frostfs-sdk-go/netmap"
	"github.com/TrueCloudLab/frostfs-sdk-go/object"
)

type remoteTarget struct {
	ctx context.Context

	keyStorage *util.KeyStorage

	commonPrm *util.CommonPrm

	nodeInfo clientcore.NodeInfo

	clientConstructor ClientConstructor
}

// RemoteSender represents utility for
// sending an object to a remote host.
type RemoteSender struct {
	keyStorage *util.KeyStorage

	clientConstructor ClientConstructor
}

// RemotePutPrm groups remote put operation parameters.
type RemotePutPrm struct {
	node netmap.NodeInfo

	obj *object.Object
}

func (t *remoteTarget) WriteObject(obj *object.Object, _ objectcore.ContentMeta) error {
	var sessionInfo *util.SessionInfo

	if tok := t.commonPrm.SessionToken(); tok != nil {
		sessionInfo = &util.SessionInfo{
			ID:    tok.ID(),
			Owner: tok.Issuer(),
		}
	}

	key, err := t.keyStorage.GetKey(sessionInfo)
	if err != nil {
		return fmt.Errorf("(%T) could not receive private key: %w", t, err)
	}

	c, err := t.clientConstructor.Get(t.nodeInfo)
	if err != nil {
		return fmt.Errorf("(%T) could not create SDK client %s: %w", t, t.nodeInfo, err)
	}

	var prm internalclient.PutObjectPrm

	prm.SetContext(t.ctx)
	prm.SetClient(c)
	prm.SetPrivateKey(key)
	prm.SetSessionToken(t.commonPrm.SessionToken())
	prm.SetBearerToken(t.commonPrm.BearerToken())
	prm.SetXHeaders(t.commonPrm.XHeaders())
	prm.SetObject(obj)

	_, err = internalclient.PutObject(prm)
	if err != nil {
		return fmt.Errorf("(%T) could not put object to %s: %w", t, t.nodeInfo.AddressGroup(), err)
	}

	return nil
}

// NewRemoteSender creates, initializes and returns new RemoteSender instance.
func NewRemoteSender(keyStorage *util.KeyStorage, cons ClientConstructor) *RemoteSender {
	return &RemoteSender{
		keyStorage:        keyStorage,
		clientConstructor: cons,
	}
}

// WithNodeInfo sets information about the remote node.
func (p *RemotePutPrm) WithNodeInfo(v netmap.NodeInfo) *RemotePutPrm {
	if p != nil {
		p.node = v
	}

	return p
}

// WithObject sets transferred object.
func (p *RemotePutPrm) WithObject(v *object.Object) *RemotePutPrm {
	if p != nil {
		p.obj = v
	}

	return p
}

// PutObject sends object to remote node.
func (s *RemoteSender) PutObject(ctx context.Context, p *RemotePutPrm) error {
	t := &remoteTarget{
		ctx:               ctx,
		keyStorage:        s.keyStorage,
		clientConstructor: s.clientConstructor,
	}

	err := clientcore.NodeInfoFromRawNetmapElement(&t.nodeInfo, netmapCore.Node(p.node))
	if err != nil {
		return fmt.Errorf("parse client node info: %w", err)
	}

	err = t.WriteObject(p.obj, objectcore.ContentMeta{})
	if err != nil {
		return fmt.Errorf("(%T) could not send object: %w", s, err)
	}

	return nil
}
