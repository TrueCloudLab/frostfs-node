package cmd

import (
	"crypto/ecdsa"
	"errors"
	"fmt"

	"github.com/mr-tron/base58"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	rawclient "github.com/nspcc-dev/neofs-api-go/v2/rpc/client"
	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	ircontrol "github.com/nspcc-dev/neofs-node/pkg/services/control/ir"
	ircontrolsrv "github.com/nspcc-dev/neofs-node/pkg/services/control/ir/server"
	controlSvc "github.com/nspcc-dev/neofs-node/pkg/services/control/server"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var controlCmd = &cobra.Command{
	Use:   "control",
	Short: "Operations with storage node",
	Long:  `Operations with storage node`,
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		ff := cmd.Flags()

		_ = viper.BindPFlag(walletPath, ff.Lookup(walletPath))
		_ = viper.BindPFlag(address, ff.Lookup(address))
		_ = viper.BindPFlag(controlRPC, ff.Lookup(controlRPC))
	},
}

var healthCheckCmd = &cobra.Command{
	Use:   "healthcheck",
	Short: "Health check of the NeoFS node",
	Long:  "Health check of the NeoFS node. Checks storage node by default, use --ir flag to work with Inner Ring.",
	Run:   healthCheck,
}

var setNetmapStatusCmd = &cobra.Command{
	Use:   "set-status",
	Short: "Set status of the storage node in NeoFS network map",
	Long:  "Set status of the storage node in NeoFS network map",
	Run:   setNetmapStatus,
}

var setShardModeCmd = &cobra.Command{
	Use:   "set-mode",
	Short: "Set work mode of the shard",
	Long:  "Set work mode of the shard",
	Run:   setShardMode,
}

var listShardsCmd = &cobra.Command{
	Use:   "list",
	Short: "List shards of the storage node",
	Long:  "List shards of the storage node",
	Run:   listShards,
}

var shardsCmd = &cobra.Command{
	Use:   "shards",
	Short: "Operations with storage node's shards",
	Long:  "Operations with storage node's shards",
}

const (
	netmapStatusFlag = "status"

	netmapStatusOnline      = "online"
	netmapStatusOffline     = "offline"
	netmapStatusMaintenance = "maintenance"

	shardModeFlag        = "mode"
	shardIDFlag          = "id"
	shardClearErrorsFlag = "clear-errors"

	shardModeReadOnly  = "read-only"
	shardModeReadWrite = "read-write"
	shardModeDegraded  = "degraded"
)

const (
	controlRPC        = "endpoint"
	controlRPCDefault = ""
	controlRPCUsage   = "remote node control address (as 'multiaddr' or '<host>:<port>')"
)

// control healthcheck flags
const (
	healthcheckIRFlag = "ir"
)

// control healthcheck vars
var (
	healthCheckIRVar bool
)

// control netmap vars
var (
	netmapStatus string
)

// control shard vars
var (
	shardMode string
	shardID   string
)

func initControlHealthCheckCmd() {
	initCommonFlagsWithoutRPC(healthCheckCmd)

	flags := healthCheckCmd.Flags()

	flags.String(controlRPC, controlRPCDefault, controlRPCUsage)
	flags.BoolVar(&healthCheckIRVar, healthcheckIRFlag, false, "Communicate with IR node")
}

func initControlSetShardModeCmd() {
	initCommonFlagsWithoutRPC(setShardModeCmd)

	flags := setShardModeCmd.Flags()

	flags.String(controlRPC, controlRPCDefault, controlRPCUsage)
	flags.StringVarP(&shardID, shardIDFlag, "", "", "ID of the shard in base58 encoding")
	flags.StringVarP(&shardMode, shardModeFlag, "", "",
		fmt.Sprintf("new shard mode keyword ('%s', '%s', '%s')",
			shardModeReadWrite,
			shardModeReadOnly,
			shardModeDegraded,
		),
	)
	flags.Bool(shardClearErrorsFlag, false, "Set shard error count to 0")
}

func initControlShardsListCmd() {
	initCommonFlagsWithoutRPC(listShardsCmd)

	flags := listShardsCmd.Flags()

	flags.String(controlRPC, controlRPCDefault, controlRPCUsage)
}

func initControlSetNetmapStatusCmd() {
	initCommonFlagsWithoutRPC(setNetmapStatusCmd)

	flags := setNetmapStatusCmd.Flags()

	flags.String(controlRPC, controlRPCDefault, controlRPCUsage)
	flags.StringVarP(&netmapStatus, netmapStatusFlag, "", "",
		fmt.Sprintf("new netmap status keyword ('%s', '%s', '%s')",
			netmapStatusOnline,
			netmapStatusOffline,
			netmapStatusMaintenance,
		),
	)

	_ = setNetmapStatusCmd.MarkFlagRequired(netmapStatusFlag)
}

func initControlDropObjectsCmd() {
	initCommonFlagsWithoutRPC(dropObjectsCmd)

	flags := dropObjectsCmd.Flags()

	flags.String(controlRPC, controlRPCDefault, controlRPCUsage)
	flags.StringSliceVarP(&dropObjectsList, dropObjectsFlag, "o", nil,
		"List of object addresses to be removed in string format")

	_ = dropObjectsCmd.MarkFlagRequired(dropObjectsFlag)
}

func initControlSnapshotCmd() {
	initCommonFlagsWithoutRPC(snapshotCmd)

	flags := snapshotCmd.Flags()

	flags.String(controlRPC, controlRPCDefault, controlRPCUsage)
	flags.BoolVar(&netmapSnapshotJSON, "json", false,
		"print netmap structure in JSON format")
}

func init() {
	rootCmd.AddCommand(controlCmd)

	shardsCmd.AddCommand(listShardsCmd)
	shardsCmd.AddCommand(setShardModeCmd)
	shardsCmd.AddCommand(dumpShardCmd)
	shardsCmd.AddCommand(restoreShardCmd)

	controlCmd.AddCommand(
		healthCheckCmd,
		setNetmapStatusCmd,
		dropObjectsCmd,
		snapshotCmd,
		shardsCmd,
	)

	initControlHealthCheckCmd()
	initControlSetNetmapStatusCmd()
	initControlDropObjectsCmd()
	initControlSnapshotCmd()
	initControlShardsListCmd()
	initControlSetShardModeCmd()
	initControlDumpShardCmd()
	initControlRestoreShardCmd()
}

func verifyResponseControl(cmd *cobra.Command,
	sigControl interface {
		GetKey() []byte
		GetSign() []byte
	},
	body interface {
		StableMarshal([]byte) ([]byte, error)
	},
) {
	if sigControl == nil {
		exitOnErr(cmd, errors.New("missing response signature"))
	}

	bodyData, err := body.StableMarshal(nil)
	exitOnErr(cmd, errf("marshal response body: %w", err))

	// TODO(@cthulhu-rider): #1387 use Signature message from NeoFS API to avoid conversion
	var sigV2 refs.Signature
	sigV2.SetScheme(refs.ECDSA_SHA512)
	sigV2.SetKey(sigControl.GetKey())
	sigV2.SetSign(sigControl.GetSign())

	var sig neofscrypto.Signature
	sig.ReadFromV2(sigV2)

	if !sig.Verify(bodyData) {
		exitOnErr(cmd, errors.New("invalid response signature"))
	}
}

func healthCheck(cmd *cobra.Command, _ []string) {
	key, err := getKeyNoGenerate()
	exitOnErr(cmd, err)

	cli, err := getControlSDKClient(key)
	exitOnErr(cmd, err)

	if healthCheckIRVar {
		healthCheckIR(cmd, key, cli)
		return
	}

	req := new(control.HealthCheckRequest)

	req.SetBody(new(control.HealthCheckRequest_Body))

	err = controlSvc.SignMessage(key, req)
	exitOnErr(cmd, errf("could not sign message: %w", err))

	var resp *control.HealthCheckResponse
	err = cli.ExecRaw(func(client *rawclient.Client) error {
		resp, err = control.HealthCheck(client, req)
		return err
	})
	exitOnErr(cmd, errf("rpc error: %w", err))

	verifyResponseControl(cmd, resp.GetSignature(), resp.GetBody())

	cmd.Printf("Network status: %s\n", resp.GetBody().GetNetmapStatus())
	cmd.Printf("Health status: %s\n", resp.GetBody().GetHealthStatus())
}

func healthCheckIR(cmd *cobra.Command, key *ecdsa.PrivateKey, c *client.Client) {
	req := new(ircontrol.HealthCheckRequest)

	req.SetBody(new(ircontrol.HealthCheckRequest_Body))

	err := ircontrolsrv.SignMessage(key, req)
	exitOnErr(cmd, errf("could not sign request: %w", err))

	var resp *ircontrol.HealthCheckResponse
	err = c.ExecRaw(func(client *rawclient.Client) error {
		resp, err = ircontrol.HealthCheck(client, req)
		return err
	})
	exitOnErr(cmd, errf("rpc error: %w", err))

	verifyResponseControl(cmd, resp.GetSignature(), resp.GetBody())

	cmd.Printf("Health status: %s\n", resp.GetBody().GetHealthStatus())
}

func setNetmapStatus(cmd *cobra.Command, _ []string) {
	key, err := getKeyNoGenerate()
	exitOnErr(cmd, err)

	var status control.NetmapStatus

	switch netmapStatus {
	default:
		exitOnErr(cmd, fmt.Errorf("unsupported status %s", netmapStatus))
	case netmapStatusOnline:
		status = control.NetmapStatus_ONLINE
	case netmapStatusOffline:
		status = control.NetmapStatus_OFFLINE
	case netmapStatusMaintenance:
		status = control.NetmapStatus_MAINTENANCE
	}

	req := new(control.SetNetmapStatusRequest)

	body := new(control.SetNetmapStatusRequest_Body)
	req.SetBody(body)

	body.SetStatus(status)

	err = controlSvc.SignMessage(key, req)
	exitOnErr(cmd, errf("could not sign request: %w", err))

	cli, err := getControlSDKClient(key)
	exitOnErr(cmd, err)

	var resp *control.SetNetmapStatusResponse
	err = cli.ExecRaw(func(client *rawclient.Client) error {
		resp, err = control.SetNetmapStatus(client, req)
		return err
	})
	exitOnErr(cmd, errf("rpc error: %w", err))

	verifyResponseControl(cmd, resp.GetSignature(), resp.GetBody())

	cmd.Println("Network status update request successfully sent.")
}

const dropObjectsFlag = "objects"

var dropObjectsList []string

var dropObjectsCmd = &cobra.Command{
	Use:   "drop-objects",
	Short: "Drop objects from the node's local storage",
	Long:  "Drop objects from the node's local storage",
	Run: func(cmd *cobra.Command, args []string) {
		key, err := getKeyNoGenerate()
		exitOnErr(cmd, err)

		binAddrList := make([][]byte, 0, len(dropObjectsList))

		for i := range dropObjectsList {
			a := addressSDK.NewAddress()

			err := a.Parse(dropObjectsList[i])
			if err != nil {
				exitOnErr(cmd, fmt.Errorf("could not parse address #%d: %w", i, err))
			}

			binAddr, err := a.Marshal()
			exitOnErr(cmd, errf("could not marshal the address: %w", err))

			binAddrList = append(binAddrList, binAddr)
		}

		req := new(control.DropObjectsRequest)

		body := new(control.DropObjectsRequest_Body)
		req.SetBody(body)

		body.SetAddressList(binAddrList)

		err = controlSvc.SignMessage(key, req)
		exitOnErr(cmd, errf("could not sign request: %w", err))

		cli, err := getControlSDKClient(key)
		exitOnErr(cmd, err)

		var resp *control.DropObjectsResponse
		err = cli.ExecRaw(func(client *rawclient.Client) error {
			resp, err = control.DropObjects(client, req)
			return err
		})
		exitOnErr(cmd, errf("rpc error: %w", err))

		verifyResponseControl(cmd, resp.GetSignature(), resp.GetBody())

		cmd.Println("Objects were successfully marked to be removed.")
	},
}

var snapshotCmd = &cobra.Command{
	Use:   "netmap-snapshot",
	Short: "Get network map snapshot",
	Long:  "Get network map snapshot",
	Run: func(cmd *cobra.Command, args []string) {
		key, err := getKeyNoGenerate()
		exitOnErr(cmd, err)

		req := new(control.NetmapSnapshotRequest)
		req.SetBody(new(control.NetmapSnapshotRequest_Body))

		err = controlSvc.SignMessage(key, req)
		exitOnErr(cmd, errf("could not sign request: %w", err))

		cli, err := getControlSDKClient(key)
		exitOnErr(cmd, err)

		var resp *control.NetmapSnapshotResponse
		err = cli.ExecRaw(func(client *rawclient.Client) error {
			resp, err = control.NetmapSnapshot(client, req)
			return err
		})
		exitOnErr(cmd, errf("rpc error: %w", err))

		verifyResponseControl(cmd, resp.GetSignature(), resp.GetBody())

		prettyPrintNetmap(cmd, resp.GetBody().GetNetmap(), netmapSnapshotJSON)
	},
}

func listShards(cmd *cobra.Command, _ []string) {
	key, err := getKeyNoGenerate()
	exitOnErr(cmd, err)

	req := new(control.ListShardsRequest)
	req.SetBody(new(control.ListShardsRequest_Body))

	err = controlSvc.SignMessage(key, req)
	exitOnErr(cmd, errf("could not sign request: %w", err))

	cli, err := getControlSDKClient(key)
	exitOnErr(cmd, err)

	var resp *control.ListShardsResponse
	err = cli.ExecRaw(func(client *rawclient.Client) error {
		resp, err = control.ListShards(client, req)
		return err
	})
	exitOnErr(cmd, errf("rpc error: %w", err))

	verifyResponseControl(cmd, resp.GetSignature(), resp.GetBody())

	prettyPrintShards(cmd, resp.GetBody().GetShards())
}

// getControlSDKClient calls getSDKClientFlag with "endpoint" flag.
func getControlSDKClient(key *ecdsa.PrivateKey) (*client.Client, error) {
	return getSDKClientFlag(key, controlRPC)
}

func prettyPrintShards(cmd *cobra.Command, ii []*control.ShardInfo) {
	for _, i := range ii {
		var mode string

		switch i.GetMode() {
		case control.ShardMode_READ_WRITE:
			mode = "read-write"
		case control.ShardMode_READ_ONLY:
			mode = "read-only"
		case control.ShardMode_DEGRADED:
			mode = "degraded"
		default:
			mode = "unknown"
		}

		pathPrinter := func(name, path string) string {
			if path == "" {
				return ""
			}

			return fmt.Sprintf("%s: %s\n", name, path)
		}

		cmd.Printf("Shard %s:\nMode: %s\n"+
			pathPrinter("Metabase", i.GetMetabasePath())+
			pathPrinter("Blobstor", i.GetBlobstorPath())+
			pathPrinter("Write-cache", i.GetWritecachePath())+
			fmt.Sprintf("Error count: %d\n", i.GetErrorCount()),
			base58.Encode(i.Shard_ID),
			mode,
		)
	}
}

func setShardMode(cmd *cobra.Command, _ []string) {
	key, err := getKeyNoGenerate()
	exitOnErr(cmd, err)

	var mode control.ShardMode

	switch shardMode {
	default:
		exitOnErr(cmd, fmt.Errorf("unsupported mode %s", mode))
	case shardModeReadWrite:
		mode = control.ShardMode_READ_WRITE
	case shardModeReadOnly:
		mode = control.ShardMode_READ_ONLY
	case shardModeDegraded:
		mode = control.ShardMode_DEGRADED
	}

	req := new(control.SetShardModeRequest)

	body := new(control.SetShardModeRequest_Body)
	req.SetBody(body)

	rawID, err := base58.Decode(shardID)
	exitOnErr(cmd, errf("incorrect shard ID encoding: %w", err))

	body.SetMode(mode)
	body.SetShardID(rawID)

	reset, _ := cmd.Flags().GetBool(shardClearErrorsFlag)
	body.ClearErrorCounter(reset)

	err = controlSvc.SignMessage(key, req)
	exitOnErr(cmd, errf("could not sign request: %w", err))

	cli, err := getControlSDKClient(key)
	exitOnErr(cmd, err)

	var resp *control.SetShardModeResponse
	err = cli.ExecRaw(func(client *rawclient.Client) error {
		resp, err = control.SetShardMode(client, req)
		return err
	})
	exitOnErr(cmd, errf("rpc error: %w", err))

	verifyResponseControl(cmd, resp.GetSignature(), resp.GetBody())

	cmd.Println("Shard mode update request successfully sent.")
}
