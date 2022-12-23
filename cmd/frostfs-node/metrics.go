package main

import (
	"context"

	metricsconfig "github.com/TrueCloudLab/frostfs-node/cmd/frostfs-node/config/metrics"
	httputil "github.com/TrueCloudLab/frostfs-node/pkg/util/http"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

func initMetrics(c *cfg) {
	if !metricsconfig.Enabled(c.appCfg) {
		c.log.Info("prometheus is disabled")
		return
	}

	var prm httputil.Prm

	prm.Address = metricsconfig.Address(c.appCfg)
	prm.Handler = promhttp.Handler()

	srv := httputil.New(prm,
		httputil.WithShutdownTimeout(
			metricsconfig.ShutdownTimeout(c.appCfg),
		),
	)

	c.workers = append(c.workers, newWorkerFromFunc(func(context.Context) {
		runAndLog(c, "metrics", false, func(c *cfg) {
			fatalOnErr(srv.Serve())
		})
	}))

	c.closers = append(c.closers, func() {
		c.log.Debug("shutting down metrics service")

		err := srv.Shutdown()
		if err != nil {
			c.log.Debug("could not shutdown metrics server",
				zap.String("error", err.Error()),
			)
		}

		c.log.Debug("metrics service has been stopped")
	})
}
