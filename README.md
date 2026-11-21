# cluster-autoscaler-wol-provider

A [Cluster Autoscaler](https://github.com/kubernetes/autoscaler/tree/master/cluster-autoscaler)
`externalgrpc` cloud provider for clusters of machines you already own. It doesn't create or
delete machines: when Cluster Autoscaler grows a node group, it wakes one of the group's
powered-off machines with Wake-on-LAN; when it shrinks one, it powers the machine off through its
operating system.

Cluster Autoscaler still makes every decision: which pending pods need a node, which node group
fits them (including scale-up from zero, from a template), which node is unneeded, draining it,
PodDisruptionBudgets, scale-down delays. This provider only switches machines on and off.

> Status: early. The gRPC service, Wake-on-LAN and Talos power-off are written and unit tested;
> it hasn't run against real hardware yet. Use `--dry-run` first.

## How it works

- **Node groups** are static lists of existing machines in a YAML file. Each machine names the
  node it registers as, how to power it on (`wol`) and off (`talos`; `ssh` is planned).
- **Target size** is the number of the group's machines that are running (their Node object
  exists) or starting (woken less than `bootTimeout` ago).
- **Scale up** (`NodeGroupIncreaseSize`) sends a magic packet to the next powered-off machine, in
  the order listed. The machine counts as starting until its node registers and is Ready, or until
  `bootTimeout` passes.
- **Scale down** (`NodeGroupDeleteNodes`) arrives after Cluster Autoscaler has drained the node. A
  Ready node's machine is shut down through its power-off method; a node that's already NotReady is
  assumed to be off. Once the node is NotReady, the provider deletes its Node object, so the group
  doesn't carry a dead node; the kubelet registers it again on the next boot.
- **Decrease target size** stops counting woken machines that haven't registered, latest first. A
  machine can't be un-woken: if it boots anyway, it registers and is scaled down like any node.
- **Scale up from zero** needs a `template` per group: labels, taints and capacity of a node as it
  registers.
- Pricing and per-group autoscaling options return `Unimplemented`, which Cluster Autoscaler treats
  as "use the defaults".

State (which machines were just woken or powered off) is kept in memory. After a restart the
provider goes by the Node objects alone.

### Requirements

- **Provider IDs.** Cluster Autoscaler matches nodes to instances by `spec.providerID`. Each node
  must register with `providerIDPrefix` + its node name (default `wol://<node>`), set with the
  kubelet's `--provider-id`. A node's provider ID can only be set when it registers, so an existing
  node has to be deleted and register again after the flag is added. If a node registers with some
  other provider ID, the provider reports that ID for it instead.
- **Wake-on-LAN** enabled in each machine's firmware and network card, for the power state it's
  shut down into (S5).
- **A broadcast that reaches the machines.** A broadcast doesn't cross subnets: run the provider
  with `hostNetwork: true` on a node in the machines' subnet, or send a directed broadcast
  (for example `192.0.2.255:9`) that your router forwards.
- **The provider must not run on a machine it can power off.**
- **For `talos`**: a Talos client certificate (`ca.crt`, `tls.crt`, `tls.key`, typically a mounted
  Secret) whose role may call Shutdown, `os:operator` or `os:admin`, and each machine's Talos API
  endpoint. Generate one with `talosctl config new --roles os:operator`.

## Configuration

See [`examples/config.yaml`](examples/config.yaml). In short:

```yaml
providerIDPrefix: wol://   # instance ID = prefix + node name
bootTimeout: 15m           # keep Cluster Autoscaler's --max-node-provision-time at or below this
shutdownTimeout: 5m        # a machine still Ready this long after power-off is logged as a failure
talos: {caFile: ..., certFile: ..., keyFile: ...}
nodeGroups:
  - id: gpu
    minSize: 0
    maxSize: 1             # at most the number of machines
    template:              # for scale-up from zero
      labels: {nvidia.com/gpu.present: "true"}
      capacity: {cpu: "16", memory: 64Gi, pods: "110", nvidia.com/gpu: "1"}
    machines:
      - node: node-a
        powerOn: wol
        mac: "00:00:5e:00:53:01"
        broadcast: "192.0.2.255:9"
        powerOff: talos
        talosEndpoint: "192.0.2.11:50000"
```

Flags:

| Flag | Default | |
|------|---------|---|
| `--config` | `/etc/wol-provider/config.yaml` | the file above |
| `--listen` | `:8086` | gRPC address |
| `--dry-run` | off | log every decision, switch nothing, report each change as failed |
| `--kubeconfig` | in-cluster | for running outside the cluster |
| `--tls-cert-file`, `--tls-key-file` | | serve TLS |
| `--tls-client-ca-file` | | require client certificates (mTLS) |
| `--debug` | off | debug logging |

## Running it next to Cluster Autoscaler

1. Deploy the provider: [`deploy/example.yaml`](deploy/example.yaml) has a ServiceAccount allowed to
   list and delete nodes, the config, the Talos certificate Secret mount, a Deployment and a Service.
   Build the image from this repository (`docker build .`) until releases are published.
2. Run upstream Cluster Autoscaler with:

   ```
   --cloud-provider=externalgrpc
   --cloud-config=/etc/cluster-autoscaler/cloud-config.yaml
   --max-node-provision-time=15m      # at most the provider's bootTimeout
   ```

   and a cloud config pointing at the provider:

   ```yaml
   address: wol-provider.kube-system.svc:8086
   grpc_timeout: 30s
   # key, cert, cacert: for mTLS, recommended, since anyone who can call the provider can
   # switch machines on and off
   ```

   With the Helm chart, pass these through its extra arguments and mount the cloud config from a
   ConfigMap as an extra volume.
3. Read the logs in `--dry-run` for a while, then remove the flag.

### Dry run

With `--dry-run`, scale-up and scale-down are logged (`dry-run: would wake machine`,
`dry-run: would power off machine`) and answered with an error, so Cluster Autoscaler rolls back:
it backs the node group off for a while and removes its deletion taint from the node. **Cluster
Autoscaler still drains a node before it asks to delete it**, so pods move even in dry-run. To
watch decisions without any evictions, also run Cluster Autoscaler with
`--scale-down-enabled=false`, and look at its own logs for the nodes it considers unneeded.

## Development

```
go test -race ./...
go run ./cmd/wol-provider --config examples/config.yaml --kubeconfig ~/.kube/config --dry-run
```

The externalgrpc protocol and its generated Go code are vendored from kubernetes/autoscaler at a
pinned tag in [`proto/externalgrpc`](proto/externalgrpc); `scripts/vendor-proto.sh <tag>` updates
them.

## Planned

- `ssh` power-off, for hosts that aren't Talos (for example a hypervisor running Windows).
- Published multi-arch images and releases.
- Optional confirmation before a power-off (a hook that must approve it).

## License

Apache-2.0. The vendored protocol is Copyright The Kubernetes Authors, also Apache-2.0.
