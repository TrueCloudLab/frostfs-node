package util

import (
	"crypto/ecdsa"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/internal/common"
	"github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/internal/commonflags"
	"github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/internal/key"
	commonCmd "github.com/TrueCloudLab/frostfs-node/cmd/internal/common"
	"github.com/TrueCloudLab/frostfs-sdk-go/session"
	"github.com/spf13/cobra"
)

var signSessionCmd = &cobra.Command{
	Use:   "session-token",
	Short: "Sign session token to use it in requests",
	Run:   signSessionToken,
}

func initSignSessionCmd() {
	commonflags.InitWithoutRPC(signSessionCmd)

	flags := signSessionCmd.Flags()

	flags.String(signFromFlag, "", "File with JSON encoded session token to sign")
	_ = signSessionCmd.MarkFlagFilename(signFromFlag)
	_ = signSessionCmd.MarkFlagRequired(signFromFlag)

	flags.String(signToFlag, "", "File to save signed session token (optional)")
}

func signSessionToken(cmd *cobra.Command, _ []string) {
	fPath, err := cmd.Flags().GetString(signFromFlag)
	commonCmd.ExitOnErr(cmd, "", err)

	if fPath == "" {
		commonCmd.ExitOnErr(cmd, "", errors.New("missing session token flag"))
	}

	type iTokenSession interface {
		json.Marshaler
		common.BinaryOrJSON
		Sign(ecdsa.PrivateKey) error
	}
	var errLast error
	var stok iTokenSession

	for _, el := range [...]iTokenSession{
		new(session.Object),
		new(session.Container),
	} {
		errLast = common.ReadBinaryOrJSON(cmd, el, fPath)
		if errLast == nil {
			stok = el
			break
		}
	}

	commonCmd.ExitOnErr(cmd, "decode session: %v", errLast)

	pk := key.GetOrGenerate(cmd)

	err = stok.Sign(*pk)
	commonCmd.ExitOnErr(cmd, "can't sign token: %w", err)

	data, err := stok.MarshalJSON()
	commonCmd.ExitOnErr(cmd, "can't encode session token: %w", err)

	to := cmd.Flag(signToFlag).Value.String()
	if len(to) == 0 {
		common.PrettyPrintJSON(cmd, stok, "session token")
		return
	}

	err = os.WriteFile(to, data, 0644)
	if err != nil {
		commonCmd.ExitOnErr(cmd, "", fmt.Errorf("can't write signed session token to %s: %w", to, err))
	}

	cmd.Printf("signed session token saved in %s\n", to)
}
