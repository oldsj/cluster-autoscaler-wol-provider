#!/usr/bin/env bash
# Checks deploy/example.yaml's RBAC and ValidatingAdmissionPolicy against a disposable cluster: the
# provider's service account may set spec.providerID, its own annotation and remove taints, and may
# delete only a node it is powering off. Needs kubectl pointed at a throwaway cluster (CI uses kind).
set -euo pipefail
cd "$(dirname "$0")/.."

awk 'BEGIN{RS="\n---\n"; ORS="\n---\n"} /kind: (ServiceAccount|ClusterRole|ClusterRoleBinding|ValidatingAdmissionPolicy|ValidatingAdmissionPolicyBinding)\n/' \
  deploy/example.yaml | kubectl apply --server-side -f -
kubectl apply --server-side -f - <<'NODE'
apiVersion: v1
kind: Node
metadata: {name: policy-test, labels: {x: "1"}}
spec:
  taints: [{key: ToBeDeletedByClusterAutoscaler, value: "1", effect: NoSchedule}]
NODE
trap 'kubectl delete node policy-test --ignore-not-found >/dev/null' EXIT
sleep 5 # the policy takes a moment to load

sa() { kubectl --as=system:serviceaccount:kube-system:wol-provider "$@"; }
failed=0
expect() {
  local want=$1 name=$2; shift 2
  if out=$("$@" 2>&1); then got=allow; else got=deny; fi
  if [[ $got == "$want" ]]; then echo "ok   $name ($got)"; else echo "FAIL $name: got $got, want $want: $out"; failed=1; fi
}
patch() { sa patch node policy-test --type=merge -p "$1"; }

expect allow "set providerID"        patch '{"spec":{"providerID":"wol://policy-test"}}'
expect deny  "change a label"        patch '{"metadata":{"labels":{"x":"2"}}}'
expect deny  "add another annotation" patch '{"metadata":{"annotations":{"foo":"bar"}}}'
expect deny  "cordon"                patch '{"spec":{"unschedulable":true}}'
expect deny  "add a taint"           patch '{"spec":{"taints":[{"key":"ToBeDeletedByClusterAutoscaler","value":"1","effect":"NoSchedule"},{"key":"x","effect":"NoExecute"}]}}'
expect deny  "delete, not powering off" sa delete node policy-test
expect allow "set power-off annotation" patch '{"metadata":{"annotations":{"wol-provider/power-off-requested":"2026-01-01T00:00:00Z"}}}'
expect allow "remove taints"         patch '{"spec":{"taints":null}}'
expect allow "delete, powering off"  sa delete node policy-test
exit $failed
