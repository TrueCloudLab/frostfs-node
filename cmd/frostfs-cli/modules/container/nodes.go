package container

import (
	"crypto/sha256"

	internalclient "github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/internal/client"
	"github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/internal/commonflags"
	"github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/internal/key"
	commonCmd "github.com/TrueCloudLab/frostfs-node/cmd/internal/common"
	containerAPI "github.com/TrueCloudLab/frostfs-sdk-go/container"
	cid "github.com/TrueCloudLab/frostfs-sdk-go/container/id"
	"github.com/TrueCloudLab/frostfs-sdk-go/netmap"
	"github.com/spf13/cobra"
)

var short bool

var containerNodesCmd = &cobra.Command{
	Use:   "nodes",
	Short: "Show nodes for container",
	Long:  "Show nodes taking part in a container at the current epoch.",
	Run: func(cmd *cobra.Command, args []string) {
		var cnr, pkey = getContainer(cmd)

		if pkey == nil {
			pkey = key.GetOrGenerate(cmd)
		}

		cli := internalclient.GetSDKClientByFlag(cmd, pkey, commonflags.RPC)

		var prm internalclient.NetMapSnapshotPrm
		prm.SetClient(cli)

		resmap, err := internalclient.NetMapSnapshot(prm)
		commonCmd.ExitOnErr(cmd, "unable to get netmap snapshot", err)

		var id cid.ID
		containerAPI.CalculateID(&id, cnr)
		binCnr := make([]byte, sha256.Size)
		id.Encode(binCnr)

		policy := cnr.PlacementPolicy()

		var cnrNodes [][]netmap.NodeInfo
		cnrNodes, err = resmap.NetMap().ContainerNodes(policy, binCnr)
		commonCmd.ExitOnErr(cmd, "could not build container nodes for given container: %w", err)

		for i := range cnrNodes {
			cmd.Printf("Descriptor #%d, REP %d:\n", i+1, policy.ReplicaNumberByIndex(i))
			for j := range cnrNodes[i] {
				commonCmd.PrettyPrintNodeInfo(cmd, cnrNodes[i][j], j, "\t", short)
			}
		}
	},
}

func initContainerNodesCmd() {
	commonflags.Init(containerNodesCmd)

	flags := containerNodesCmd.Flags()
	flags.StringVar(&containerID, commonflags.CIDFlag, "", commonflags.CIDFlagUsage)
	flags.StringVar(&containerPathFrom, fromFlag, "", fromFlagUsage)
	flags.BoolVar(&short, "short", false, "Shortens output of node info")
}
