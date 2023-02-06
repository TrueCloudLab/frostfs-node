package config

type opts struct {
	path      string
	configDir string
}

func defaultOpts() *opts {
	return new(opts)
}

// Option allows to set an optional parameter of the Config.
type Option func(*opts)

// WithConfigFile returns an option to set the system path
// to the configuration file.
func WithConfigFile(path string) Option {
	return func(o *opts) {
		o.path = path
	}
}

// WithConfigDir returns an option to set the system path
// to the directory with configuration files.
func WithConfigDir(path string) Option {
	return func(o *opts) {
		o.configDir = path
	}
}
