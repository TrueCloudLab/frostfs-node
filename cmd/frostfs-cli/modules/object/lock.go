package object

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	objectV2 "github.com/TrueCloudLab/frostfs-api-go/v2/object"
	internalclient "github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/internal/client"
	"github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/internal/common"
	"github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/internal/commonflags"
	"github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/internal/key"
	commonCmd "github.com/TrueCloudLab/frostfs-node/cmd/internal/common"
	cid "github.com/TrueCloudLab/frostfs-sdk-go/container/id"
	objectSDK "github.com/TrueCloudLab/frostfs-sdk-go/object"
	oid "github.com/TrueCloudLab/frostfs-sdk-go/object/id"
	"github.com/TrueCloudLab/frostfs-sdk-go/user"
	"github.com/spf13/cobra"
)

// object lock command.
var objectLockCmd = &cobra.Command{
	Use:   "lock",
	Short: "Lock object in container",
	Long:  "Lock object in container",
	Run: func(cmd *cobra.Command, _ []string) {
		cidRaw, _ := cmd.Flags().GetString(commonflags.CIDFlag)

		var cnr cid.ID
		err := cnr.DecodeString(cidRaw)
		commonCmd.ExitOnErr(cmd, "Incorrect container arg: %v", err)

		oidsRaw, _ := cmd.Flags().GetStringSlice(commonflags.OIDFlag)

		lockList := make([]oid.ID, len(oidsRaw))

		for i := range oidsRaw {
			err = lockList[i].DecodeString(oidsRaw[i])
			commonCmd.ExitOnErr(cmd, fmt.Sprintf("Incorrect object arg #%d: %%v", i+1), err)
		}

		key := key.GetOrGenerate(cmd)

		var idOwner user.ID
		user.IDFromKey(&idOwner, key.PublicKey)

		var lock objectSDK.Lock
		lock.WriteMembers(lockList)

		exp, _ := cmd.Flags().GetUint64(commonflags.ExpireAt)
		lifetime, _ := cmd.Flags().GetUint64(commonflags.Lifetime)
		if exp == 0 && lifetime == 0 { // mutual exclusion is ensured by cobra
			commonCmd.ExitOnErr(cmd, "", errors.New("either expiration epoch of a lifetime is required"))
		}

		if lifetime != 0 {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
			defer cancel()

			endpoint, _ := cmd.Flags().GetString(commonflags.RPC)

			currEpoch, err := internalclient.GetCurrentEpoch(ctx, cmd, endpoint)
			commonCmd.ExitOnErr(cmd, "Request current epoch: %w", err)

			exp = currEpoch + lifetime
		}

		common.PrintVerbose(cmd, "Lock object will expire after %d epoch", exp)

		var expirationAttr objectSDK.Attribute
		expirationAttr.SetKey(objectV2.SysAttributeExpEpoch)
		expirationAttr.SetValue(strconv.FormatUint(exp, 10))

		obj := objectSDK.New()
		obj.SetContainerID(cnr)
		obj.SetOwnerID(&idOwner)
		obj.SetType(objectSDK.TypeLock)
		obj.SetAttributes(expirationAttr)
		obj.SetPayload(lock.Marshal())

		var prm internalclient.PutObjectPrm
		ReadOrOpenSession(cmd, &prm, key, cnr, nil)
		Prepare(cmd, &prm)
		prm.SetHeader(obj)

		res, err := internalclient.PutObject(prm)
		commonCmd.ExitOnErr(cmd, "Store lock object in FrostFS: %w", err)

		cmd.Printf("Lock object ID: %s\n", res.ID())
		cmd.Println("Objects successfully locked.")
	},
}

func initCommandObjectLock() {
	commonflags.Init(objectLockCmd)
	initFlagSession(objectLockCmd, "PUT")

	ff := objectLockCmd.Flags()

	ff.String(commonflags.CIDFlag, "", commonflags.CIDFlagUsage)
	_ = objectLockCmd.MarkFlagRequired(commonflags.CIDFlag)

	ff.StringSlice(commonflags.OIDFlag, nil, commonflags.OIDFlagUsage)
	_ = objectLockCmd.MarkFlagRequired(commonflags.OIDFlag)

	ff.Uint64P(commonflags.ExpireAt, "e", 0, "The last active epoch for the lock")

	ff.Uint64(commonflags.Lifetime, 0, "Lock lifetime")
	objectLockCmd.MarkFlagsMutuallyExclusive(commonflags.ExpireAt, commonflags.Lifetime)
}
