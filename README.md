# cluster-autoscaler-wol-provider

A [Cluster Autoscaler](https://github.com/kubernetes/autoscaler/tree/master/cluster-autoscaler)
`externalgrpc` cloud provider for clusters of machines you already own. It doesn't create or
delete machines: when Cluster Autoscaler grows a node group, it powers one of the group's machines
on; when it shrinks one, it powers the machine off. How is up to plugins, per machine: Wake-on-LAN,
an HTTP call (a smart plug, Home Assistant, a BMC), a command, `ssh` (any OS, Windows included) or
the Talos API.

## Why

Home labs and small on-prem clusters are sized for their busiest hour and sit idle most of the day.
Consumer and prosumer machines idle at roughly 30–100 W each, around the clock. A single 100 W idle
node uses about 876 kWh a year: roughly $130 at $0.15/kWh, mostly spent doing nothing.

In the cloud, Cluster Autoscaler fixes this by deleting idle instances and creating new ones on
demand. On bare metal you can't create machines, but you can turn them off and back on. This
provider gives Cluster Autoscaler exactly that: a node group is a fixed set of real machines, "scale
up" powers one on, and "scale down" drains it and powers it off. Node groups can go all the way to
zero.

That works because cold boots are fast now. One example measurement, on an ordinary consumer desktop
with the Kubernetes node running as a VM on a Windows host:

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

> Status: early. Every plugin is unit tested (ssh against an in-process SSH server, http against a
> test server), and the API-server behaviour it relies on is tested against a real API server in
> CI. It hasn't run against real hardware yet. Start with `--dry-run`.

## Examples

Full file: [`examples/config.yaml`](examples/config.yaml). Addresses are from the documentation
ranges; use your own.

### Plain Linux: Wake-on-LAN on, ssh off

```yaml
defaults:
  powerOn:
    wol: {broadcast: "192.0.2.255:9"}
  powerOff:
    ssh: {user: autoscaler, keyFile: /etc/wol-provider/ssh/id_ed25519, knownHostsFile: /etc/wol-provider/ssh/known_hosts}
nodeGroups:
  - id: batch
    minSize: 0
    maxSize: 2
    machines:
      - node: node-a
        powerOn: {wol: {mac: "00:00:5e:00:53:01"}}
        powerOff: {ssh: {host: 192.0.2.11}}           # runs "sudo poweroff"
        probe: {tcp: {address: "192.0.2.11:22"}}
      - node: node-b                                    # a VM on a Windows host: shut the host down
        powerOn: {wol: {mac: "00:00:5e:00:53:02"}}
        powerOff: {ssh: {host: 192.0.2.12, user: administrator, command: "shutdown.exe /s /t 0"}}
```

On each Linux machine, the ssh user needs to run the command without a password, for example a
sudoers line `autoscaler ALL=(root) NOPASSWD: /usr/sbin/poweroff`. On Windows, install OpenSSH
Server and put the public key in `C:\ProgramData\ssh\administrators_authorized_keys`.

### Talos

```yaml
defaults:
  powerOff:
    talos: {caFile: /etc/wol-provider/talos/ca.crt, certFile: /etc/wol-provider/talos/tls.crt, keyFile: /etc/wol-provider/talos/tls.key}
nodeGroups:
  - id: gpu
    minSize: 0
    maxSize: 1
    template:
      labels: {kubernetes.io/arch: amd64, nvidia.com/gpu.present: "true"}
      capacity: {cpu: "16", memory: 64Gi, pods: "110", nvidia.com/gpu: "1"}
    machines:
      - node: node-c
        powerOn: {wol: {mac: "00:00:5e:00:53:03"}}
        powerOff: {talos: {endpoint: "192.0.2.13:50000"}}
```

The certificate's role must allow Shutdown (`os:operator` or `os:admin`): `talosctl config new
--roles os:operator`. If something else issues your Talos certificates (Omni, for example), check
that a certificate issued this way is accepted before relying on it.

### A smart plug over HTTP

For a machine without Wake-on-LAN: set its firmware to power on when AC returns, and let the plug
switch it on. Shut the OS down with ssh; never cut power to a running OS.

```yaml
machines:
  - node: node-d
    powerOn:
      http:
        url: http://192.0.2.50/relay/0?turn=on
        method: GET
    powerOff: {ssh: {host: 192.0.2.14}}
    probe: {tcp: {address: "192.0.2.14:22"}}
```

## Configuration

Each machine:

| Field | |
|-------|---|
| `node` or `nodeSelector` | the machine's Node: by name, or by labels (exactly one node must match) |
| `name` | the machine's name; defaults to `node`, required with `nodeSelector` |
| `providerID` | the node's existing `spec.providerID`, if something else already set one |
| `powerOn` | one plugin: `{wol: {...}}`, `{http: {...}}` or `{exec: {...}}` |
| `powerOff` | one plugin: `{ssh: {...}}`, `{talos: {...}}`, `{http: {...}}` or `{exec: {...}}` |
| `probe` | optional: `{tcp: {...}}` or `{icmp: {...}}` |

`defaults.powerOn`, `defaults.powerOff` and `defaults.probe` hold settings shared by every machine
using a plugin; a machine's own settings override them key by key. Every plugin's settings are
checked at startup.

A `nodeSelector` should use labels the kubelet can't set on its own Node, under
`node-restriction.kubernetes.io/` (with the NodeRestriction admission plugin); otherwise a
compromised node could claim to be another machine.

### Plugins

**Power on**

- `wol`: a Wake-on-LAN magic packet, sent three times and resent while the machine is starting.
  `mac` (required), `broadcast` (host:port, default `255.255.255.255:9`). Needs Wake-on-LAN enabled
  in firmware and the network card for the state the machine shuts down into (S5), and a broadcast
  that reaches it: run the provider with `hostNetwork` on a node in the machines' subnet, or use a
  directed broadcast (`192.0.2.255:9`) your router forwards.
- `http`: see below.
- `exec`: see below.

**Power off**

- `ssh`: runs a command over SSH. `host` and `user` (required), `keyFile` (a private key, typically
  a mounted Secret), `knownHostsFile` (required unless `insecureIgnoreHostKey: true`), `port` (22),
  `command` (default `sudo poweroff`), `timeout` (30s). A connection that drops while the command
  runs counts as success (the host went down); a non-zero exit status is a failure.
- `talos`: graceful Shutdown through the Talos API. `endpoint`, `caFile`, `certFile`, `keyFile`
  (all required). Nothing outside this plugin imports Talos.
- `http`, `exec`: see below.

**Either**

- `http`: an HTTP request. `url` (required, a template), `method` (POST), `headers`, `headerFiles`
  (header values read from files on every request, for tokens in a mounted Secret), `body` (a
  template), `timeout` (10s), `caFile`. Any 2xx succeeds. Templates see `{{.Name}}` (machine),
  `{{.Node}}`, `{{.Group}}` and `{{.Action}}` (`on`, `off` or `approve`).
- `exec`: runs a command in the provider's container. `command` (required, a list; each item a
  template), `env`, `timeout` (60s). Exit status 0 succeeds. The image has no shell or tools: mount
  the program, or build an image from this one with it added.

**Probes** (optional; without one, only the node's Ready condition counts)

- `tcp`: up when a TCP connection to `address` succeeds (SSH, or the kubelet's 10250). `timeout` (2s).
- `icmp`: up when `host` (IPv4) answers a ping. Uses unprivileged ping sockets: the provider's group
  must be inside the `net.ipv4.ping_group_range` sysctl, which with `hostNetwork` is the host's.

A probe makes the provider more accurate: a machine that's up but hasn't registered counts as
starting and isn't woken twice; a machine being powered off counts as off as soon as the probe says
it's down; a NotReady node whose machine still answers isn't mistaken for a powered-off one.

### Approval

Optionally, every power-off must be approved first, by an HTTP endpoint:

```yaml
approval:
  http:
    url: https://approvals.example.com/power-off?machine={{.Name}}
    headerFiles: {Authorization: /etc/wol-provider/approval/token}
  wait: 20s          # how long one scale-down waits for an answer
  pollInterval: 2s
  expire: 1h         # a pending request older than this is asked again from scratch
```

2xx (except 202) approves; 202 means pending, and the provider polls the response's `Location`
with GET (or repeats the request when there's none); anything else denies. A request still pending
after `wait` fails the scale-down, so Cluster Autoscaler rolls it back and tries again later; the
provider remembers the pending request and checks it next time instead of starting a new one.

### Timing

| Setting | Default | |
|---------|---------|---|
| `bootTimeout` | 20m | a powered-on machine counts as starting this long; keep Cluster Autoscaler's `--max-node-provision-time` (default 15m) below it |
| `wakeResendInterval` | 30s | power-on is resent this often while starting |
| `minOffTime` | 1m | a machine that just went off isn't powered on again sooner |
| `powerOffTimeout` | 2m | how long one power-off plugin call may take |
| `shutdownTimeout` | 5m | a machine still Ready this long after its power-off gets its node back |
| `notReadyGrace` | 10m | a node NotReady this long, with no probe saying it's up, counts as off |
| `probeInterval` | 15s | |

### Flags

| Flag | Default | |
|------|---------|---|
| `--config` | `/etc/wol-provider/config.yaml` | the file above |
| `--listen` | `:8086` | gRPC address |
| `--tls-cert-file`, `--tls-key-file`, `--tls-client-ca-file` | | mutual TLS; required |
| `--insecure` | off | plaintext, no client authentication; for testing only |
| `--health-listen` | `:8087` | plaintext gRPC health only, for kubelet probes; empty disables it |
| `--dry-run` | off | log every decision, switch nothing, report each change as failed |
| `--kubeconfig` | in-cluster | for running outside the cluster |
| `--debug` | off | debug logging |

The provider refuses to start without mutual TLS unless `--insecure` is given: anyone who can call
it can power machines off.

## How it works

- **Target size** is the number of the group's machines that are running (a Node exists and is
  Ready, or NotReady for less than `notReadyGrace`, or its probe says it's up) or starting (powered
  on less than `bootTimeout` ago, or seen up by its probe and not registered yet).
- **Scale up** powers on the next machines that are off, in config order; a machine whose last
  power-on timed out goes after the others, so one dead machine doesn't block its group. The
  power-on is resent every `wakeResendInterval` until the node registers.
- **Scale down** arrives after Cluster Autoscaler has drained the node. Safety rules:
  - a machine with a registered node is powered off only if that node carries Cluster Autoscaler's
    `ToBeDeletedByClusterAutoscaler` taint, read fresh from the API server;
  - a placeholder node (Cluster Autoscaler's stand-in for an instance it thinks never registered)
    is refused when the machine does have a registered node;
  - with `approval`, the hook must approve.

  The provider records the power-off as a node annotation (`wol-provider/power-off-requested`)
  before calling the plugin, so a restart picks it up, and runs the plugin on its own
  `powerOffTimeout`, apart from Cluster Autoscaler's request. If the plugin fails, it removes the
  annotation and Cluster Autoscaler's taint so the node is schedulable again. A NotReady node's
  machine is powered off too, and a failure there is tolerated. Once the node is NotReady (or the
  probe says the machine is down) the provider deletes the Node object; the kubelet registers it
  again on the next boot. A machine still Ready after `shutdownTimeout` gets its node back.
- **Decrease target size** stops counting powered-on machines that haven't registered, latest
  first, and never touches a registered node.
- **Scale up from zero** needs a `template` per group: labels (include `kubernetes.io/arch`),
  taints and capacity of a node as it registers.
- Pricing and per-group autoscaling options return `Unimplemented`, which Cluster Autoscaler treats
  as "use the defaults".

### Provider IDs

Cluster Autoscaler matches nodes to instances by `spec.providerID`. Nodes on machines you own
usually register without one, so the provider sets it: on every refresh, each machine's matched node
whose `spec.providerID` is empty gets `providerIDPrefix` + the machine's name (`wol://<name>`). No
kubelet flag, no re-registration.

- The API server lets a node's `spec.providerID` go from empty to a value once, and refuses any
  later change (`node updates may not change providerID except from "" to valid`). The provider
  can't overwrite an ID something else set; CI checks this rule against a real API server.
- That also means the provider can win a race with something that sets provider IDs later, such as
  a cloud controller manager, and the node then keeps the provider's ID for good. If you run one,
  set `assignProviderIDs: false` and give its IDs as each machine's `providerID`.
- A node that already has a provider ID (set by its kubelet, a cloud controller, or Talos) keeps it.
  Give that value as the machine's `providerID`; without that, the provider still uses the node's
  ID and logs a warning once.
- `assignProviderIDs: false` turns assignment off. It's on in `--dry-run` too, since it switches
  nothing on or off and Cluster Autoscaler can't match nodes without it.
- Instance IDs must be unique across machines; the config is refused otherwise.

### Permissions

The provider needs `list`, `get`, `patch`, `update` and `delete` on nodes: it sets empty provider
IDs, records and clears its power-off annotation, removes Cluster Autoscaler's taint after a failed
power-off, and deletes the Node object of a machine it powered off. RBAC can't narrow that to some
nodes or fields, so [`deploy/example.yaml`](deploy/example.yaml) adds a ValidatingAdmissionPolicy
(Kubernetes 1.30+) under which the provider's service account can only set `spec.providerID`, set or
clear its own annotation and remove taints, and can only delete a node it's powering off.
`hack/test-node-policy.sh` checks it.

## Running it next to Cluster Autoscaler

1. Deploy the provider: [`deploy/example.yaml`](deploy/example.yaml) has the ServiceAccount, RBAC
   and admission policy, the config, mutual TLS and ssh key Secret mounts, a Deployment (Recreate,
   since `hostNetwork` holds the port) and a Service. Images for linux/amd64 and linux/arm64 are at
   `ghcr.io/oldsj/cluster-autoscaler-wol-provider`: `X.Y.Z`, `X.Y` and `latest` from release tags,
   `main` and `sha-<commit>` from the main branch.
2. Issue a server certificate for the provider and a client certificate for Cluster Autoscaler from
   one CA (cert-manager works).
3. Run upstream Cluster Autoscaler with:

   ```
   --cloud-provider=externalgrpc
   --cloud-config=/etc/cluster-autoscaler/cloud-config.yaml
   --max-node-provision-time=15m      # below the provider's bootTimeout
   ```

   and a cloud config pointing at the provider:

   ```yaml
   address: wol-provider.kube-system.svc:8086
   grpc_timeout: 30s
   cert: /etc/cluster-autoscaler/tls/tls.crt
   key: /etc/cluster-autoscaler/tls/tls.key
   cacert: /etc/cluster-autoscaler/tls/ca.crt
   ```

   With the Helm chart, pass these through its extra arguments and mount the cloud config and
   certificate as extra volumes.
4. Read the logs in `--dry-run` for a while, then remove the flag.

### Dry run

With `--dry-run`, scale-up and scale-down are logged (`dry-run: would power on machine`,
`dry-run: would power off machine`) and answered with an error, so Cluster Autoscaler rolls back:
it backs the node group off for a while and removes its deletion taint from the node. **Cluster
Autoscaler still drains a node before it asks to delete it**, so pods move even in dry-run. To
watch decisions without any evictions, also run Cluster Autoscaler with
`--scale-down-enabled=false`, and look at its own logs for the nodes it considers unneeded.

### Upgrading from 0.2

0.3 changes the config format: `powerOn` and `powerOff` name a plugin with its settings
(`powerOn: {wol: {mac: ...}}`) instead of a method name plus top-level fields (`mac`, `broadcast`,
`talosEndpoint`, `talos`). And the provider now refuses to start without mutual TLS unless
`--insecure` is given.

## Development

```
go test -race ./...
go run ./cmd/wol-provider --config examples/config.yaml --kubeconfig ~/.kube/config --dry-run --insecure
```

CI also runs golangci-lint and govulncheck, checks the API server's providerID rule and the
example's node policy (`hack/test-node-policy.sh`) against a disposable kind cluster, and
cross-builds for Linux, macOS and Windows.

A plugin is a package under [`internal/plugins`](internal/plugins) with a constructor that takes its
settings as JSON and returns a `plugin.PowerOn`, `plugin.PowerOff` or `plugin.Probe`
([`internal/plugin`](internal/plugin/plugin.go)); register it in
[`cmd/wol-provider/build.go`](cmd/wol-provider/build.go). The core never imports a plugin.

The externalgrpc protocol and its generated Go code are vendored from kubernetes/autoscaler at a
pinned tag in [`proto/externalgrpc`](proto/externalgrpc); `scripts/vendor-proto.sh <tag>` updates
them.

## License

Apache-2.0. The vendored protocol is Copyright The Kubernetes Authors, also Apache-2.0.
