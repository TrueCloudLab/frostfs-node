package control

import (
	"github.com/TrueCloudLab/frostfs-api-go/v2/rpc/client"
	"github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/internal/key"
	commonCmd "github.com/TrueCloudLab/frostfs-node/cmd/internal/common"
	"github.com/TrueCloudLab/frostfs-node/pkg/services/control"
	"github.com/spf13/cobra"
)

var flushCacheCmd = &cobra.Command{
	Use:   "flush-cache",
	Short: "Flush objects from the write-cache to the main storage",
	Long:  "Flush objects from the write-cache to the main storage",
	Run:   flushCache,
}

func flushCache(cmd *cobra.Command, _ []string) {
	pk := key.Get(cmd)

	req := &control.FlushCacheRequest{Body: new(control.FlushCacheRequest_Body)}
	req.Body.Shard_ID = getShardIDList(cmd)

	signRequest(cmd, pk, req)

	cli := getClient(cmd, pk)

	var resp *control.FlushCacheResponse
	var err error
	err = cli.ExecRaw(func(client *client.Client) error {
		resp, err = control.FlushCache(client, req)
		return err
	})
	commonCmd.ExitOnErr(cmd, "rpc error: %w", err)

	verifyResponse(cmd, resp.GetSignature(), resp.GetBody())

	cmd.Println("Write-cache has been flushed.")
}

func initControlFlushCacheCmd() {
	initControlFlags(flushCacheCmd)

	ff := flushCacheCmd.Flags()
	ff.StringSlice(shardIDFlag, nil, "List of shard IDs in base58 encoding")
	ff.Bool(shardAllFlag, false, "Process all shards")

	flushCacheCmd.MarkFlagsMutuallyExclusive(shardIDFlag, shardAllFlag)
}
