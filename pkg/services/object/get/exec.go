package getsvc

import (
	"context"
	"crypto/ecdsa"
	"errors"

	clientcore "github.com/TrueCloudLab/frostfs-node/pkg/core/client"
	"github.com/TrueCloudLab/frostfs-node/pkg/core/object"
	"github.com/TrueCloudLab/frostfs-node/pkg/services/object/util"
	"github.com/TrueCloudLab/frostfs-node/pkg/services/object_manager/placement"
	"github.com/TrueCloudLab/frostfs-node/pkg/util/logger"
	objectSDK "github.com/TrueCloudLab/frostfs-sdk-go/object"
	oid "github.com/TrueCloudLab/frostfs-sdk-go/object/id"
	"go.uber.org/zap"
)

type statusError struct {
	status int
	err    error
}

type execCtx struct {
	svc *Service

	ctx context.Context

	prm RangePrm

	statusError

	splitInfo *objectSDK.SplitInfo

	log *logger.Logger

	collectedObject *objectSDK.Object

	curOff uint64

	headOnly bool

	curProcEpoch uint64
}

type execOption func(*execCtx)

const (
	statusUndefined int = iota
	statusOK
	statusINHUMED
	statusVIRTUAL
	statusOutOfRange
)

func headOnly() execOption {
	return func(c *execCtx) {
		c.headOnly = true
	}
}

func withPayloadRange(r *objectSDK.Range) execOption {
	return func(c *execCtx) {
		c.prm.rng = r
	}
}

func (exec *execCtx) setLogger(l *logger.Logger) {
	req := "GET"
	if exec.headOnly {
		req = "HEAD"
	} else if exec.prm.rng != nil {
		req = "GET_RANGE"
	}

	exec.log = &logger.Logger{Logger: l.With(
		zap.String("request", req),
		zap.Stringer("address", exec.prm.addr),
		zap.Bool("raw", exec.prm.raw),
		zap.Bool("local", exec.prm.common.LocalOnly()),
		zap.Bool("with session", exec.prm.common.SessionToken() != nil),
		zap.Bool("with bearer", exec.prm.common.BearerToken() != nil),
	)}
}

// isChild checks if reading object is a parent of the given object.
// Object without reference to the parent (only children with the parent header
// have it) is automatically considered as child: this should be guaranteed by
// upper level logic.
func (exec execCtx) isChild(obj *objectSDK.Object) bool {
	par := obj.Parent()
	return par == nil || equalAddresses(exec.prm.addr, object.AddressOf(par))
}

func (exec execCtx) key() (*ecdsa.PrivateKey, error) {
	if exec.prm.signerKey != nil {
		// the key has already been requested and
		// cached in the previous operations
		return exec.prm.signerKey, nil
	}

	var sessionInfo *util.SessionInfo

	if tok := exec.prm.common.SessionToken(); tok != nil {
		sessionInfo = &util.SessionInfo{
			ID:    tok.ID(),
			Owner: tok.Issuer(),
		}
	}

	return exec.svc.keyStore.GetKey(sessionInfo)
}

func (exec *execCtx) canAssemble() bool {
	return exec.svc.assembly && !exec.prm.raw && !exec.headOnly && !exec.prm.common.LocalOnly()
}

func (exec *execCtx) initEpoch() bool {
	exec.curProcEpoch = exec.prm.common.NetmapEpoch()
	if exec.curProcEpoch > 0 {
		return true
	}

	e, err := exec.svc.currentEpochReceiver.currentEpoch()

	switch {
	default:
		exec.status = statusUndefined
		exec.err = err

		exec.log.Debug("could not get current epoch number",
			zap.String("error", err.Error()),
		)

		return false
	case err == nil:
		exec.curProcEpoch = e
		return true
	}
}

func (exec *execCtx) generateTraverser(addr oid.Address) (*placement.Traverser, bool) {
	obj := addr.Object()

	t, err := exec.svc.traverserGenerator.GenerateTraverser(addr.Container(), &obj, exec.curProcEpoch)

	switch {
	default:
		exec.status = statusUndefined
		exec.err = err

		exec.log.Debug("could not generate container traverser",
			zap.String("error", err.Error()),
		)

		return nil, false
	case err == nil:
		return t, true
	}
}

func (exec *execCtx) getChild(id oid.ID, rng *objectSDK.Range, withHdr bool) (*objectSDK.Object, bool) {
	w := NewSimpleObjectWriter()

	p := exec.prm
	p.common = p.common.WithLocalOnly(false)
	p.objWriter = w
	p.SetRange(rng)

	p.addr.SetContainer(exec.prm.addr.Container())
	p.addr.SetObject(id)

	exec.statusError = exec.svc.get(exec.ctx, p.commonPrm, withPayloadRange(rng))

	child := w.Object()
	ok := exec.status == statusOK

	if ok && withHdr && !exec.isChild(child) {
		exec.status = statusUndefined
		exec.err = errors.New("wrong child header")

		exec.log.Debug("parent address in child object differs")
	}

	return child, ok
}

func (exec *execCtx) headChild(id oid.ID) (*objectSDK.Object, bool) {
	p := exec.prm
	p.common = p.common.WithLocalOnly(false)
	p.addr.SetContainer(exec.prm.addr.Container())
	p.addr.SetObject(id)

	prm := HeadPrm{
		commonPrm: p.commonPrm,
	}

	w := NewSimpleObjectWriter()
	prm.SetHeaderWriter(w)

	err := exec.svc.Head(exec.ctx, prm)

	switch {
	default:
		exec.status = statusUndefined
		exec.err = err

		exec.log.Debug("could not get child object header",
			zap.Stringer("child ID", id),
			zap.String("error", err.Error()),
		)

		return nil, false
	case err == nil:
		child := w.Object()

		if !exec.isChild(child) {
			exec.status = statusUndefined

			exec.log.Debug("parent address in child object differs")
		} else {
			exec.status = statusOK
			exec.err = nil
		}

		return child, true
	}
}

func (exec execCtx) remoteClient(info clientcore.NodeInfo) (getClient, bool) {
	c, err := exec.svc.clientCache.get(info)

	switch {
	default:
		exec.status = statusUndefined
		exec.err = err

		exec.log.Debug("could not construct remote node client")
	case err == nil:
		return c, true
	}

	return nil, false
}

func mergeSplitInfo(dst, src *objectSDK.SplitInfo) {
	if last, ok := src.LastPart(); ok {
		dst.SetLastPart(last)
	}

	if link, ok := src.Link(); ok {
		dst.SetLink(link)
	}

	if splitID := src.SplitID(); splitID != nil {
		dst.SetSplitID(splitID)
	}
}

func (exec *execCtx) writeCollectedHeader() bool {
	if exec.prm.rng != nil {
		return true
	}

	err := exec.prm.objWriter.WriteHeader(
		exec.collectedObject.CutPayload(),
	)

	switch {
	default:
		exec.status = statusUndefined
		exec.err = err

		exec.log.Debug("could not write header",
			zap.String("error", err.Error()),
		)
	case err == nil:
		exec.status = statusOK
		exec.err = nil
	}

	return exec.status == statusOK
}

func (exec *execCtx) writeObjectPayload(obj *objectSDK.Object) bool {
	if exec.headOnly {
		return true
	}

	err := exec.prm.objWriter.WriteChunk(obj.Payload())

	switch {
	default:
		exec.status = statusUndefined
		exec.err = err

		exec.log.Debug("could not write payload chunk",
			zap.String("error", err.Error()),
		)
	case err == nil:
		exec.status = statusOK
		exec.err = nil
	}

	return err == nil
}

func (exec *execCtx) writeCollectedObject() {
	if ok := exec.writeCollectedHeader(); ok {
		exec.writeObjectPayload(exec.collectedObject)
	}
}

// disableForwarding removes request forwarding closure from common
// parameters, so it won't be inherited in new execution contexts.
func (exec *execCtx) disableForwarding() {
	exec.prm.SetRequestForwarder(nil)
}
