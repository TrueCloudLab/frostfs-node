package accounting

import (
	"context"

	"github.com/TrueCloudLab/frostfs-api-go/v2/accounting"
)

// Server is an interface of the FrostFS API Accounting service server.
type Server interface {
	Balance(context.Context, *accounting.BalanceRequest) (*accounting.BalanceResponse, error)
}
