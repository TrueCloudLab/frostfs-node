# Examples of correct configuration file structures 

Here are files in all supported formats and with all possible configuration values
of FrostFS applications. See [node.yaml](node.yaml) for configuration notes.

All parameters are correct, however, they are for informational purposes only. 
It is not recommended transferring these configs for real application launches.

## Config files

- Storage node
  - JSON: `node.json`
  - YAML: `node.yaml`
- Inner ring
  - YAML: `ir.yaml`
- CLI
  - YAML: `cli.yaml`
  
### Multiple configs

You can split your configuration to several files.
For example, you can use separate yaml file for each shard or each service (pprof, prometheus).
You must use `--config-dir` flag to process several configs:

```shell
$ ./bin/frotsfs-node --config ./config/example/node.yaml --config-dir ./dir/with/additional/configs
```

When the `--config-dir` flag set, the application:
* reads all `*.y[a]ml` files from provided directory,
* use Viper's [MergeConfig](https://pkg.go.dev/github.com/spf13/viper#MergeConfig) functionality to produce the final configuration,
* files are being processing in alphanumerical order so that `01.yaml` may be extended with contents of `02.yaml`, so
if a field is specified in multiple files, the latest occurrence takes effect.

So if we have the following files:
```yaml
# 00.yaml
logger:
  level: debug
pprof:
  enabled: true
  address: localhost:6060
prometheus:
  enabled: true
  address: localhost:9090
```

```yaml
# dir/01.yaml
logger:
  level: info
pprof:
  enabled: false
```

```yaml
# dir/02.yaml
logger:
  level: warn
prometheus:
  address: localhost:9091
```

and provide the following flags:
```shell
$ ./bin/frotsfs-node --config 00.yaml --config-dir dir
```

result config will be:
```yaml
logger:
  level: warn
pprof:
  enabled: false
  address: localhost:6060
prometheus:
  enabled: true
  address: localhost:9091
```

## Environment variables

- Storage node: `node.env`
- Inner ring: `ir.env`
