package config

import (
	"os"
	"path"

	"github.com/spf13/viper"
)

// ReadConfigDir reads all config files from provided directory in alphabetical order
// and merge its content with current viper configuration.
func ReadConfigDir(v *viper.Viper, configDir string) error {
	entries, err := os.ReadDir(configDir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		ext := path.Ext(entry.Name())
		if ext != ".yaml" && ext != ".yml" {
			continue
		}

		if err = mergeConfig(v, path.Join(configDir, entry.Name())); err != nil {
			return err
		}
	}

	return nil
}

// mergeConfig reads config file and merge its content with current viper.
func mergeConfig(v *viper.Viper, fileName string) (err error) {
	var cfgFile *os.File
	cfgFile, err = os.Open(fileName)
	if err != nil {
		return err
	}

	defer func() {
		errClose := cfgFile.Close()
		if err == nil {
			err = errClose
		}
	}()

	if err = v.MergeConfig(cfgFile); err != nil {
		return err
	}

	return nil
}
