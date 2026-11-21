# cluster-autoscaler-wol-provider

A [Cluster Autoscaler](https://github.com/kubernetes/autoscaler/tree/master/cluster-autoscaler)
`externalgrpc` cloud provider for clusters of machines you already own. It doesn't create or
delete machines: when Cluster Autoscaler grows a node group, it wakes one of the group's
powered-off machines with Wake-on-LAN; when it shrinks one, it powers the machine off through its
operating system.

## Why

Home labs and small on-prem clusters are sized for their busiest hour and sit idle most of the day.
Consumer and prosumer machines idle at roughly 30–100 W each, around the clock. A single 100 W idle
node uses about 876 kWh a year: roughly $130 at $0.15/kWh, mostly spent doing nothing.

In the cloud, Cluster Autoscaler fixes this by deleting idle instances and creating new ones on
demand. On bare metal you can't create machines, but you can turn them off and back on. This
provider gives Cluster Autoscaler exactly that: a node group is a fixed set of real machines, "scale
up" powers one on, and "scale down" drains it and powers it off. Node groups can go all the way to
zero.

That works because cold boots are fast now. On an ordinary consumer desktop, with the Kubernetes
node running as a VM on a Windows host:

| Step | Time after the magic packet |
|------|-----------------------------|
| Wake-on-LAN sent | 0 s |
| Operating system up | ~43 s |
| Node Ready | ~89 s |
| First pod Ready | ~110 s |

That measurement includes a 30 s VM autostart delay, so bare metal or a setup without the delay lands
around 80 s, and a plain Linux install is faster still. It fits easily inside Cluster Autoscaler's
provisioning window, and it's fine for CI, batch jobs, media transcoding, model inference and other
on-demand apps. Pair it with [KEDA](https://keda.sh) scale-to-zero for HTTP apps.

The shape this is built for: a small always-on core (the control plane plus one low-power node) runs
Cluster Autoscaler and this provider, while the big, power-hungry machines sleep at zero until
pending pods need them.

Not a fit: latency-critical services pinned to sleeping nodes, or nodes that hold storage replicas or
quorum members you can't lose for a few minutes.

## What it does

Cluster Autoscaler still makes every decision: which pending pods need a node, which node group
fits them (including scale-up from zero, from a template), which node is unneeded, draining it,
PodDisruptionBudgets, scale-down delays. This provider only switches machines on and off.

> Status: early. The gRPC service, Wake-on-LAN and Talos power-off are written and unit tested;
> it hasn't run against real hardware yet. Use `--dry-run` first.

## How it works

- **Node groups** are static lists of existing machines in a YAML file. Each machine is matched to
  its node by name (`node`) or by labels (`nodeSelector`), and says how to power it on (`wol`) and
  off (`talos`; `ssh` is planned).
- **Provider IDs** are set by the provider: see [Provider IDs](#provider-ids).
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

- **Wake-on-LAN** enabled in each machine's firmware and network card, for the power state it's
  shut down into (S5).
- **A broadcast that reaches the machines.** A broadcast doesn't cross subnets: run the provider
  with `hostNetwork: true` on a node in the machines' subnet, or send a directed broadcast
  (for example `192.0.2.255:9`) that your router forwards.
- **The provider must not run on a machine it can power off.**
- **For `talos`**: a Talos client certificate (`ca.crt`, `tls.crt`, `tls.key`, typically a mounted
  Secret) whose role may call Shutdown, `os:operator` or `os:admin`, and each machine's Talos API
  endpoint. Generate one with `talosctl config new --roles os:operator`.

### Provider IDs

Cluster Autoscaler matches nodes to instances by `spec.providerID`. Nodes on machines you own
usually register without one, so the provider sets it: on every refresh, each machine's matched node
whose `spec.providerID` is empty gets `providerIDPrefix` + the machine's name (`wol://<name>`; the
name defaults to the node name). No kubelet flag, no re-registration.

- The API server lets a node's `spec.providerID` go from empty to a value once, and refuses any
  later change (`node updates may not change providerID except from "" to valid`). The provider
  can't overwrite an ID something else set; CI checks this rule against a real API server.
- A node that already has a provider ID (set by its kubelet, a cloud controller, or Talos) keeps it.
  Give that value as the machine's `providerID` and the provider uses it as-is; without that it
  still uses the node's ID, and logs a warning.
- When the provider deletes a powered-off machine's Node object, the kubelet registers it again
  with an empty ID on the next boot, and the provider sets it again.
- `assignProviderIDs: false` turns this off. It's on in `--dry-run` too, since it switches nothing
  on or off and Cluster Autoscaler can't match nodes without it.
- It needs `patch` on nodes; see the ClusterRole in [`deploy/example.yaml`](deploy/example.yaml)
  (`get`, `list`, `watch`, `patch`, `delete` on `nodes`).

## Configuration

See [`examples/config.yaml`](examples/config.yaml). In short:

```yaml
providerIDPrefix: wol://   # instance ID = prefix + machine name, set on the node (see above)
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
   list, patch and delete nodes, the config, the Talos certificate Secret mount, a Deployment and a
   Service. Images for linux/amd64 and linux/arm64 are at
   `ghcr.io/oldsj/cluster-autoscaler-wol-provider`: `X.Y.Z` and `X.Y` from release tags, `main` and
   `sha-<commit>` from the main branch.
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
- Optional confirmation before a power-off (a hook that must approve it).

## License

Apache-2.0. The vendored protocol is Copyright The Kubernetes Authors, also Apache-2.0.
