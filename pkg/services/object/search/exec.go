package searchsvc

import (
	"context"

	"github.com/TrueCloudLab/frostfs-node/pkg/services/object_manager/placement"
	"github.com/TrueCloudLab/frostfs-node/pkg/util/logger"
	cid "github.com/TrueCloudLab/frostfs-sdk-go/container/id"
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

	prm Prm

	statusError

	log *logger.Logger

	curProcEpoch uint64
}

const (
	statusUndefined int = iota
	statusOK
)

func (exec *execCtx) prepare() {
	if _, ok := exec.prm.writer.(*uniqueIDWriter); !ok {
		exec.prm.writer = newUniqueAddressWriter(exec.prm.writer)
	}
}

func (exec *execCtx) setLogger(l *logger.Logger) {
	exec.log = &logger.Logger{Logger: l.With(
		zap.String("request", "SEARCH"),
		zap.Stringer("container", exec.prm.cnr),
		zap.Bool("local", exec.prm.common.LocalOnly()),
		zap.Bool("with session", exec.prm.common.SessionToken() != nil),
		zap.Bool("with bearer", exec.prm.common.BearerToken() != nil),
	)}
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

func (exec *execCtx) generateTraverser(cnr cid.ID) (*placement.Traverser, bool) {
	t, err := exec.svc.traverserGenerator.generateTraverser(cnr, exec.curProcEpoch)

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

func (exec *execCtx) writeIDList(ids []oid.ID) {
	err := exec.prm.writer.WriteIDs(ids)

	switch {
	default:
		exec.status = statusUndefined
		exec.err = err

		exec.log.Debug("could not write object identifiers",
			zap.String("error", err.Error()),
		)
	case err == nil:
		exec.status = statusOK
		exec.err = nil
	}
}
