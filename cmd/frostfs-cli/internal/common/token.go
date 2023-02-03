package common

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	commonCmd "github.com/TrueCloudLab/frostfs-node/cmd/internal/common"
	"github.com/TrueCloudLab/frostfs-sdk-go/bearer"
	"github.com/spf13/cobra"
)

// ReadBearerToken reads bearer token from the path provided in a specified flag.
func ReadBearerToken(cmd *cobra.Command, flagname string) *bearer.Token {
	path, err := cmd.Flags().GetString(flagname)
	commonCmd.ExitOnErr(cmd, "", err)

	if len(path) == 0 {
		return nil
	}

	PrintVerbose(cmd, "Reading bearer token from file [%s]...", path)

	var tok bearer.Token

	err = ReadBinaryOrJSON(cmd, &tok, path)
	commonCmd.ExitOnErr(cmd, "invalid bearer token: %v", err)

	return &tok
}

// BinaryOrJSON is an interface of entities which provide json.Unmarshaler
// and FrostFS binary decoder.
type BinaryOrJSON interface {
	Unmarshal([]byte) error
	json.Unmarshaler
}

// ReadBinaryOrJSON reads file data using provided path and decodes
// BinaryOrJSON from the data.
func ReadBinaryOrJSON(cmd *cobra.Command, dst BinaryOrJSON, fPath string) error {
	PrintVerbose(cmd, "Reading file [%s]...", fPath)

	// try to read session token from file
	data, err := os.ReadFile(fPath)
	if err != nil {
		return fmt.Errorf("read file <%s>: %w", fPath, err)
	}

	PrintVerbose(cmd, "Trying to decode binary...")

	err = dst.Unmarshal(data)
	if err != nil {
		PrintVerbose(cmd, "Failed to decode binary: %v", err)

		PrintVerbose(cmd, "Trying to decode JSON...")

		err = dst.UnmarshalJSON(data)
		if err != nil {
			PrintVerbose(cmd, "Failed to decode JSON: %v", err)
			return errors.New("invalid format")
		}
	}

	return nil
}
