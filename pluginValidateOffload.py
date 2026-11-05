# SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
# SPDX-License-Identifier: Apache-2.0

import json
import re
import shlex
import typing

from dataclasses import dataclass
from typing import Optional

from ktoolbox import common
from ktoolbox import host
from ktoolbox import kjinja2

import pluginbase
import task
import tftbase

from task import PluginTask
from task import TaskOperation
from testSettings import TestSettings
from tftbase import BaseOutput
from tftbase import ClusterMode
from tftbase import PluginOutput
from tftbase import PodType
from tftbase import TaskRole

logger = common.ExtendedLogger("tft." + __name__)


VF_REP_TRAFFIC_THRESHOLD = 1000


def ethtool_stat_parse(output: str) -> dict[str, str]:
    result = {}
    for line in output.splitlines():
        try:
            key, val = line.split(":", 2)
        except Exception:
            continue
        if val == "" and " " in key:
            # This is a section heading.
            continue
        result[key.strip()] = val.strip()
    return result


def ethtool_stat_get_packets(data: dict[str, str], packet_type: str) -> Optional[int]:

    # Case1: Try to parse rx_packets and tx_packets from ethtool output
    val = data.get(f"{packet_type}_packets")
    if val is not None:
        try:
            return int(val)
        except KeyError:
            return None

    # Case2: Ethtool output does not provide these fields, so we need to sum
    # the queues manually.
    total_packets = 0
    prefix = f"{packet_type}_queue_"
    packet_suffix = "_xdp_packets"
    any_match = False

    for k, v in data.items():
        if k.startswith(prefix) and k.endswith(packet_suffix):
            try:
                total_packets += int(v)
            except KeyError:
                return None
            any_match = True
    if not any_match:
        return None
    return total_packets


KEY_NAMES = {
    "start": {
        "rx": "rx_start",
        "tx": "tx_start",
    },
    "end": {
        "rx": "rx_end",
        "tx": "tx_end",
    },
}


def ethtool_stat_get_startend(
    parsed_data: dict[str, int],
    ethtool_data: str,
    suffix: typing.Literal["start", "end"],
) -> bool:
    ethtool_dict = ethtool_stat_parse(ethtool_data)
    has_any = False
    for ethtool_name in ("rx", "tx"):
        # Don't construct key_name as f"{ethtool_name}_{suffix}", because the
        # keys should appear verbatim in source code, so we can grep for them.
        key_name = KEY_NAMES[suffix][ethtool_name]
        v = ethtool_stat_get_packets(ethtool_dict, ethtool_name)
        if v is None:
            continue
        parsed_data[key_name] = v
        has_any = True
    return has_any


def check_no_traffic_on_vf_rep(
    parsed_data: dict[str, typing.Any],
    direction: typing.Literal["rx", "tx"],
    statistics_backend: str,
    threshold: int = VF_REP_TRAFFIC_THRESHOLD,
) -> Optional[str]:
    start = common.dict_get_typed(
        parsed_data, KEY_NAMES["start"][direction], int, allow_missing=True
    )
    end = common.dict_get_typed(
        parsed_data, KEY_NAMES["end"][direction], int, allow_missing=True
    )
    if start is None or end is None:
        if start is not None or end is not None:
            return f"missing {statistics_backend} {direction} packet statistics"
        return None
    if end - start >= threshold:
        return f"traffic on VF rep detected in {statistics_backend} {direction} statistics ({end-start} packets is higher than threshold {threshold})"
    return None


# Match the host PF to one external DPU port using its MAC and optional PF index.
def find_host_pf_representor(
    data: str, mac: str, pf_index: Optional[int] = None
) -> str:
    ports = json.loads(data)["port"].values()
    matches = [
        port["netdev"]
        for port in ports
        if port.get("flavour") == "pcipf"
        and port.get("external") is True
        and (pf_index is None or port.get("pfnum") == pf_index)
        and port.get("function", {}).get("hw_addr", "").lower() == mac.lower()
        and isinstance(port.get("netdev"), str)
    ]
    if len(matches) != 1:
        raise ValueError(f"expected one external host PF representor for MAC {mac}")
    return str(matches[0])


@common.strict_dataclass
@dataclass(frozen=True, kw_only=True)
class PluginValidateOffload(pluginbase.Plugin):
    PLUGIN_NAME = "validate_offload"
    STATISTICS_BACKEND = "ethtool"

    vf_rep_traffic_threshold: int = VF_REP_TRAFFIC_THRESHOLD
    host_network_interfaces: Optional[dict[str, str]] = None

    def statistics_command(self, vf_rep: str) -> str:
        return f"ethtool -S {shlex.quote(vf_rep)}"

    def statistics_get_startend(
        self,
        parsed_data: dict[str, int],
        output: str,
        suffix: typing.Literal["start", "end"],
    ) -> bool:
        return ethtool_stat_get_startend(parsed_data, output, suffix)

    # Create endpoint monitors, sharing host-network counter checks on the same node.
    def _enable(
        self,
        *,
        ts: TestSettings,
        perf_server: task.ServerTask,
        perf_client: task.ClientTask,
        tenant: bool,
    ) -> list[PluginTask]:
        if self.host_network_interfaces is not None and ts.test_case_id in (
            tftbase.TestCaseType.HOST_TO_HOST_SAME_NODE,
            tftbase.TestCaseType.HOST_TO_CLUSTER_IP_TO_HOST_SAME_NODE,
        ):
            if ts.test_case_id == tftbase.TestCaseType.HOST_TO_HOST_SAME_NODE:
                logger.info(
                    f"{self.PLUGIN_NAME}: skipping direct same-node host-to-host "
                    "offload validation; traffic stays in the host network stack"
                )
            else:
                # Same-node host-to-host ClusterIP traffic hairpins and cannot be offloaded.
                logger.info(
                    f"{self.PLUGIN_NAME}: skipping same-node host-to-host ClusterIP "
                    "offload validation; hairpin traffic cannot be offloaded"
                )
            return []

        monitors: list[PluginTask] = []
        host_nodes: set[str] = set()
        for role, perf in (
            (TaskRole.SERVER, perf_server),
            (TaskRole.CLIENT, perf_client),
        ):
            if (
                self.host_network_interfaces is not None
                and perf.pod_type == PodType.HOSTBACKED
                and perf.pod_name != task.EXTERNAL_PERF_SERVER
            ):
                # Host-network endpoints on one node share the same representors.
                if perf.node_name in host_nodes:
                    continue
                host_nodes.add(perf.node_name)
            monitors.append(TaskValidateOffload(self, ts, role, perf, tenant))
        return monitors


plugin = pluginbase.register_plugin(PluginValidateOffload())


class TaskValidateOffload(PluginTask):
    @property
    def plugin(self) -> PluginValidateOffload:
        return self._plugin

    @property
    def _is_dpu_mode(self) -> bool:
        return self.tc.mode == ClusterMode.DPU

    def __init__(
        self,
        plugin: PluginValidateOffload,
        ts: TestSettings,
        task_role: TaskRole,
        perf_instance: task.ServerTask | task.ClientTask,
        tenant: bool,
    ):
        self._plugin = plugin
        super().__init__(
            ts=ts,
            index=0,
            task_role=task_role,
            tenant=tenant,
        )

        plugin_name = self.plugin.PLUGIN_NAME.replace("_", "-")
        self.pod_name = f"tools-pod-{self.node_location}-{self.task_role.name.lower()}-{plugin_name}"
        self.in_file_template = tftbase.get_manifest("tools-pod.yaml.j2")
        self._perf_instance = perf_instance
        self.perf_pod_name = perf_instance.pod_name
        self.perf_pod_type = perf_instance.pod_type

        # DPU mode: we need a tools pod on the DPU to query VF reps
        self._dpu_pod_name: Optional[str] = None
        self._dpu_node_name: Optional[str] = None
        logger.debug(
            f"TaskValidateOffload for {task_role.name}: DPU mode = {self._is_dpu_mode}"
        )
        if self._is_dpu_mode:
            # In DPU mode, create a separate tools pod on the DPU cluster
            # The DPU node name may differ from host node name
            self._dpu_node_name = self._get_dpu_node_name()
            logger.info(
                f"DPU node name for worker {self.node_name}: {self._dpu_node_name}"
            )
            # DPU node names are already valid Kubernetes names, so we use a
            # simpler sanitization - just replace any remaining problematic chars
            dpu_node_name_safe = self._dpu_node_name.lower().replace("_", "-")
            self._dpu_pod_name = f"tools-dpu-{dpu_node_name_safe}"
            logger.info(f"DPU tools pod name: {self._dpu_pod_name}")

    def _get_dpu_node_name(self) -> str:
        """Get the DPU node name corresponding to this host node.

        Queries DPU nodes by the configured dpu_node_host_label.
        For example, with label "provisioning.dpu.nvidia.com/host",
        finds nodes where that label equals this worker's node name.
        """
        host_label = self.tc.dpu_node_host_label
        if not host_label:
            raise ValueError(
                "dpu_node_host_label must be configured when running in DPU mode. "
                "Set it in config.yaml (e.g., dpu_node_host_label: 'provisioning.dpu.nvidia.com/host')"
            )

        selector = f"{host_label}={self.node_name}"
        result = self.tc.client_infra.oc(
            f"get nodes -l {selector} -o jsonpath='{{.items[*].metadata.name}}'",
            may_fail=True,
        )

        if not result.success:
            raise RuntimeError(
                f"Failed to query DPU nodes by label {selector}: {result.err}"
            )

        nodes_str = result.out.strip().strip("'\"")
        if not nodes_str:
            raise RuntimeError(
                f"No DPU node found with label {selector}. "
                f"Ensure DPU nodes have label '{host_label}' set to the worker node name."
            )

        dpu_nodes = nodes_str.split()
        if len(dpu_nodes) == 1:
            logger.info(
                f"Found DPU node {dpu_nodes[0]} for worker {self.node_name} via label"
            )
            return dpu_nodes[0]

        # Multiple matches - use first but warn
        logger.warning(
            f"Multiple DPU nodes found with label {selector}: {dpu_nodes}. "
            f"Using first: {dpu_nodes[0]}"
        )
        return dpu_nodes[0]

    def initialize(self) -> None:
        super().initialize()
        self.render_pod_file("Plugin Pod Yaml")

        # In DPU mode, also create and deploy the DPU tools pod
        if self._is_dpu_mode:
            if not self._dpu_pod_name:
                raise RuntimeError(
                    f"DPU mode enabled but no DPU pod name set for {self.task_role.name}"
                )
            logger.info(
                f"DPU mode enabled for {self.task_role.name}, "
                f"DPU node: {self._dpu_node_name}, pod: {self._dpu_pod_name}"
            )
            self._ensure_dpu_namespaces()
            self._initialize_dpu_pod()

    def _ensure_dpu_namespaces(self) -> None:
        base_namespace = self.ts.cfg_descr.get_tft().namespace
        namespaces = (base_namespace, tftbase.get_udn_namespace(base_namespace))
        for namespace in namespaces:
            if (
                self.tc.client_infra.oc_get(
                    f"namespace/{namespace}", may_fail=True, namespace=None
                )
                is not None
            ):
                continue
            logger.info(f"Creating namespace {namespace} on DPU cluster")
            self.tc.client_infra.oc(
                f"create namespace {namespace}",
                die_on_error=True,
                namespace=None,
            )
            self.tc.infra_namespaces.append(namespace)

    def _initialize_dpu_pod(self) -> None:
        """Create and deploy the tools pod on the DPU cluster."""
        assert self._dpu_pod_name is not None

        logger.info(
            f"Creating DPU tools pod {self._dpu_pod_name} on node {self._dpu_node_name}"
        )

        namespace = self.get_namespace()
        template_args = {
            "name_space": f'"{namespace}"',
            "test_image": f'"{tftbase.get_tft_test_image()}"',
            "image_pull_policy": f'"{tftbase.get_tft_image_pull_policy()}"',
            "command": '["/usr/bin/container-entry-point.sh"]',
            "args": '["sleep", "infinity"]',
            "label_tft_tests": f'"{self.index}"',
            "node_name": f'"{self._dpu_node_name}"',
            "pod_name": f'"{self._dpu_pod_name}"',
        }
        logger.debug(f"DPU pod template args: {template_args}")

        # Render the pod YAML
        dpu_pod_yaml_path = tftbase.get_manifest_renderpath(
            self._dpu_pod_name + ".yaml"
        )
        kjinja2.render_file(
            self.in_file_template,
            template_args,
            out_file=dpu_pod_yaml_path,
        )
        logger.info(f"DPU Pod Yaml rendered to {dpu_pod_yaml_path}")

        # Apply the pod on the infra (DPU) cluster
        logger.info(f"Applying DPU pod YAML to infra cluster in namespace {namespace}")
        result = self.tc.client_infra.oc(
            f"apply -f {dpu_pod_yaml_path}",
            namespace=namespace,
            may_fail=True,
        )
        if not result.success:
            raise RuntimeError(
                f"Failed to create DPU tools pod: {result.err}. "
                f"DPU pod YAML path: {dpu_pod_yaml_path}"
            )
        logger.info(f"DPU pod apply result: {result.out}")

        # Wait for the pod to be ready
        logger.info(f"Waiting for DPU tools pod {self._dpu_pod_name} to be ready...")
        result = self.tc.client_infra.oc(
            f"wait --for=condition=Ready pod/{self._dpu_pod_name} --timeout=60s",
            namespace=namespace,
            may_fail=True,
        )
        if not result.success:
            # Check pod status for better error message
            status_result = self.tc.client_infra.oc(
                f"get pod/{self._dpu_pod_name} -o wide",
                namespace=namespace,
                may_fail=True,
            )
            pod_status = status_result.out if status_result.success else "unknown"
            raise RuntimeError(
                f"DPU tools pod failed to become ready: {result.err}. "
                f"Pod status: {pod_status}"
            )

        logger.info(f"DPU tools pod {self._dpu_pod_name} is ready")

    def _run_oc_exec_dpu(
        self,
        cmd: str,
        *,
        may_fail: bool = False,
    ) -> host.Result:
        """Run a command on the DPU tools pod via the infra client."""
        assert self._dpu_pod_name is not None
        return self.tc.client_infra.oc_exec(
            cmd,
            pod_name=self._dpu_pod_name,
            may_fail=may_fail,
            namespace=self.get_namespace(),
        )

    def _run_tools_exec(
        self,
        cmd: str,
        *,
        may_fail: bool = False,
    ) -> host.Result:
        if self._is_dpu_mode:
            return self._run_oc_exec_dpu(cmd, may_fail=may_fail)
        return self.run_oc_exec(cmd, may_fail=may_fail)

    def _get_vf_info_from_pod(
        self, pod_name: str, ifname: str
    ) -> Optional[tuple[int, int]]:
        """Get the VF index and PF index for an interface inside a pod.

        Uses standard Linux sysfs interfaces (vendor-agnostic).
        Returns (vf_index, pf_index) or None if not found.
        """
        ns = self.get_namespace()

        # Get VF PCI path
        r = self.tc.client_tenant.oc_exec(
            f"readlink -f /sys/class/net/{ifname}/device",
            pod_name=pod_name,
            may_fail=True,
            namespace=ns,
        )
        if not r.success or not r.out.strip():
            logger.warning(f"Could not get device path for {ifname}")
            return None
        vf_pci_path = r.out.strip()
        logger.debug(f"VF PCI path: {vf_pci_path}")

        # Get PF PCI path (physfn symlink indicates this is a VF)
        r = self.tc.client_tenant.oc_exec(
            f"readlink -f /sys/class/net/{ifname}/device/physfn",
            pod_name=pod_name,
            may_fail=True,
            namespace=ns,
        )
        if not r.success or not r.out.strip():
            logger.warning(f"Could not get PF path for {ifname} - might not be a VF")
            return None
        pf_pci_path = r.out.strip()
        logger.debug(f"PF PCI path: {pf_pci_path}")

        # Get PF index from PCI function number (e.g., 0000:b5:00.1 -> 1)
        pf_pci = pf_pci_path.split("/")[-1]
        match = re.search(r"\.(\d+)$", pf_pci)
        pf_index = int(match.group(1)) if match else 0
        logger.debug(f"PF PCI: {pf_pci}, PF index: {pf_index}")

        # List virtfn symlinks to find VF index
        r = self.tc.client_tenant.oc_exec(
            f"ls -la {pf_pci_path}/virtfn*",
            pod_name=pod_name,
            may_fail=True,
            namespace=ns,
        )
        if not r.success:
            logger.warning(f"Could not list virtfn symlinks: {r.err}")
            return None

        # Parse: lrwxrwxrwx. 1 root root 0 virtfn0 -> ../0000:b5:00.3
        vf_pci = vf_pci_path.split("/")[-1]
        for line in r.out.strip().split("\n"):
            match = re.search(r"virtfn(\d+)\s+->\s+.*?([^/]+)$", line)
            if match and match.group(2) == vf_pci:
                vf_index = int(match.group(1))
                logger.info(
                    f"Found VF index {vf_index}, PF index {pf_index} for {ifname}"
                )
                return (vf_index, pf_index)

        logger.warning(f"Could not determine VF index for {ifname}")
        return None

    def _get_perf_pod_ifname(self) -> Optional[str]:
        if not self.ts.test_case_id.is_udn:
            return "net1" if self._perf_instance.uses_secondary_ip else "eth0"

        pod = self._perf_instance.run_oc_get(
            f"pod/{self.perf_pod_name}", die_on_error=True
        )
        if pod is None:
            logger.warning(f"Could not get pod {self.perf_pod_name}")
            return None

        try:
            network_status = json.loads(
                pod["metadata"]["annotations"]["k8s.v1.cni.cncf.io/network-status"]
            )
            pod_ip = self._perf_instance.get_pod_ip()
            for network in network_status:
                if not isinstance(network, dict):
                    continue
                if pod_ip in network.get("ips", []):
                    ifname = network.get("interface")
                    if isinstance(ifname, str) and ifname:
                        logger.info(f"Found interface {ifname} for UDN pod IP {pod_ip}")
                        return ifname
        except (KeyError, TypeError, json.JSONDecodeError) as e:
            logger.warning(
                f"Could not parse network status for pod {self.perf_pod_name}: {e}"
            )
            return None

        logger.warning(
            f"Could not find an interface for UDN pod IP {pod_ip} "
            f"in network status for pod {self.perf_pod_name}"
        )
        return None

    def _get_vf_rep_on_dpu(self, vf_index: int, pf_index: int = 0) -> Optional[str]:
        """Find the VF representor on the DPU for a given VF index.

        Uses devlink port show for vendor-agnostic lookup via pfnum/vfnum.
        """
        logger.info(
            f"Looking for VF representor: pf_index={pf_index}, vf_index={vf_index}"
        )

        # Use devlink (vendor-agnostic)
        # Format: pci/.../N: type eth netdev <name> flavour pcivf pfnum X vfnum Y
        r = self._run_oc_exec_dpu("devlink port show", may_fail=True)
        if not r.success:
            logger.warning(f"devlink port show failed: {r.err}")
            return None

        for line in r.out.strip().split("\n"):
            # Look for pcivf flavour with matching pfnum and vfnum
            if "flavour pcivf" in line or "flavor pcivf" in line:
                pf_match = re.search(r"pfnum\s+(\d+)", line)
                vf_match = re.search(r"vfnum\s+(\d+)", line)
                netdev_match = re.search(r"netdev\s+(\S+)", line)

                if pf_match and vf_match and netdev_match:
                    if (
                        int(pf_match.group(1)) == pf_index
                        and int(vf_match.group(1)) == vf_index
                    ):
                        ifname = netdev_match.group(1)
                        logger.info(f"Found VF representor via devlink: {ifname}")
                        return ifname

        logger.warning(
            f"Could not find VF representor for pf{pf_index}vf{vf_index} in devlink output"
        )
        return None

    def _get_vf_rep_dpu_mode(
        self,
        pod_name: str,
        ifname: str,
    ) -> Optional[str]:
        """Get VF representor in DPU mode.

        1. Get VF index and PF index from the pod (via sysfs)
        2. Find the VF representor on the DPU by phys_port_name (pf<X>vf<N>)
        """
        assert self._dpu_pod_name is not None

        # Step 1: Get VF and PF index from the perf pod
        vf_info = self._get_vf_info_from_pod(pod_name, ifname)
        if vf_info is None:
            logger.warning(
                f"Could not determine VF info for {ifname} in pod {pod_name}"
            )
            return None

        vf_index, pf_index = vf_info

        # Step 2: Find VF representor on DPU
        vf_rep = self._get_vf_rep_on_dpu(vf_index, pf_index=pf_index)
        return vf_rep

    # Select the configured uplink by actual node placement, not endpoint role.
    def _host_interface(self) -> str:
        interfaces = self.plugin.host_network_interfaces
        assert interfaces is not None
        (client,) = self.ts.connection.client
        (server,) = self.ts.connection.server
        if client.name == server.name and interfaces["client"] != interfaces["server"]:
            raise ValueError("the same configured node has conflicting host interfaces")
        pod = self._perf_instance.run_oc_get(
            f"pod/{self.perf_pod_name}", die_on_error=True
        )
        if pod is None or pod["spec"]["nodeName"] != self.node_name:
            raise ValueError("host-network endpoint is not on its selected node")
        # Keys identify configured nodes, even when the client runs on the server node.
        if self.node_name == server.name:
            return interfaces["server"]
        if self.node_name == client.name:
            return interfaces["client"]
        raise ValueError(f"no host interface configured for node {self.node_name}")

    # Inspect the host through the traffic pod, which shares its network namespace.
    def _host_exec(self, command: str) -> host.Result:
        result = self._perf_instance.run_oc_exec(command, may_fail=True)
        if not result.success:
            raise RuntimeError(result.debug_msg())
        return result

    # Match a configured host PF to its external representor on the paired DPU.
    def _host_representor(self, ifname: str) -> str:
        if not self._is_dpu_mode:
            raise ValueError("host-network offload validation requires DPU mode")
        interface_type = self._host_exec(
            f"test -L /sys/class/net/{shlex.quote(ifname)}/device/physfn "
            "&& echo vf || echo pf"
        ).out.strip()
        if interface_type == "vf":
            raise ValueError(
                f"configured host interface {ifname} must be a PF, not a VF"
            )
        info = json.loads(
            self._host_exec(f"ip -j link show dev {shlex.quote(ifname)}").out
        )
        ports = self._run_tools_exec("devlink -j port show", may_fail=True)
        if not ports.success:
            raise RuntimeError(ports.debug_msg())
        port_name = self._perf_instance.run_oc_exec(
            f"cat /sys/class/net/{shlex.quote(ifname)}/phys_port_name",
            may_fail=True,
        )
        match = re.fullmatch(r"p(\d+)", port_name.out.strip())
        pf_index = int(match.group(1)) if match else None
        return find_host_pf_representor(ports.out, info[0]["address"], pf_index)

    # Measure representor counter growth across the transfer using the plugin backend.
    def _create_task_operation(self) -> TaskOperation:
        def _thread_action() -> BaseOutput:
            success_result = True
            msg: Optional[str] = None
            stats_backend = self.plugin.STATISTICS_BACKEND
            result_data: dict[str, typing.Any] = {}
            vf_rep: Optional[str] = None
            ifname: Optional[str] = None

            host_network = (
                self.perf_pod_type == PodType.HOSTBACKED
                and self.plugin.host_network_interfaces is not None
                and self.perf_pod_name != task.EXTERNAL_PERF_SERVER
            )
            representors: dict[str, str] = {}
            if host_network:
                try:
                    ifname = self._host_interface()
                    host_rep = self._host_representor(ifname)
                    management_rep = self._get_vf_rep_dpu_mode(
                        self.perf_pod_name, "ovn-k8s-mp0"
                    )
                    if management_rep is None:
                        raise ValueError("cannot discover representor for ovn-k8s-mp0")
                    # Host traffic can use either path, depending on routing.
                    representors = {
                        "ovn-k8s-mp0": management_rep,
                        ifname: host_rep,
                    }
                except (ValueError, RuntimeError, KeyError, TypeError, IndexError) as e:
                    success_result = False
                    msg = str(e)
            elif self.perf_pod_type == PodType.HOSTBACKED:
                logger.info("The VF representor is: ovn-k8s-mp0")
                msg = "Hostbacked pod"
            elif self.perf_pod_name == task.EXTERNAL_PERF_SERVER:
                logger.info("There is no VF on an external server")
                msg = "External Iperf Server"
            else:
                ifname = self._get_perf_pod_ifname()
                if ifname is None:
                    success_result = False
                    msg = "cannot determine pod interface"
                else:
                    if self._is_dpu_mode:
                        logger.info(
                            "DPU mode: querying VF representor from DPU cluster"
                        )
                        vf_rep = self._get_vf_rep_dpu_mode(
                            pod_name=self.perf_pod_name,
                            ifname=ifname,
                        )
                    else:
                        vf_rep = self.pod_get_vf_rep(
                            pod_name=self.perf_pod_name,
                            ifname=ifname,
                            host_pod_name=self.pod_name,
                        )

                    if vf_rep is None:
                        success_result = False
                        msg = "cannot determine VF_REP for pod"
                        logger.error(
                            f"VF representor for {ifname} in {self.perf_pod_name} not detected"
                        )
                    else:
                        logger.info(
                            f"VF representor for {ifname} in pod {self.perf_pod_name} is {repr(vf_rep)}"
                        )
                        representors[ifname] = vf_rep

            commands = {
                rep: self.plugin.statistics_command(rep)
                for rep in dict.fromkeys(representors.values())
            }
            self.ts.clmo_barrier.wait()

            starts = {
                rep: self._run_tools_exec(command, may_fail=True)
                for rep, command in commands.items()
            }
            if commands:
                self.ts.event_client_finished.wait()
            ends = {
                rep: self._run_tools_exec(command, may_fail=True)
                for rep, command in commands.items()
            }
            if host_network:
                result_data["interfaces"] = representors
                result_data["representors"] = {}
            for rep, stats_cmd in commands.items():
                r1, r2 = starts[rep], ends[rep]
                data1 = ""
                data2 = ""
                parsed_data: dict[str, typing.Any] = {}
                if host_network:
                    result_data["representors"][rep] = parsed_data
                else:
                    # Ordinary pod results retain their existing flat structure.
                    result_data = parsed_data
                parsed_data["statistics_backend"] = stats_backend
                parsed_data[f"{stats_backend}_cmd_1"] = common.dataclass_to_dict(r1)
                parsed_data[f"{stats_backend}_cmd_2"] = common.dataclass_to_dict(r2)

                if r1.success:
                    data1 = r1.out
                if r2.success:
                    data2 = r2.out

                if not r1.success:
                    success_result = False
                    msg = f"{stats_backend} statistics command failed"
                elif not r2.success:
                    success_result = False
                    msg = f"{stats_backend} statistics command at end failed"

                parsed_start = self.plugin.statistics_get_startend(
                    parsed_data, data1, "start"
                )
                parsed_end = self.plugin.statistics_get_startend(
                    parsed_data, data2, "end"
                )

                if not parsed_start:
                    if success_result:
                        success_result = False
                        msg = f"{stats_backend} statistics output cannot be parsed"
                if not parsed_end:
                    if success_result:
                        success_result = False
                        msg = (
                            f"{stats_backend} statistics output at end cannot be parsed"
                        )

                logger.info(
                    f"{stats_backend} statistics for pod {self.perf_pod_name} "
                    f"on node {self.node_name}, representor {rep}:\n"
                    f"rx_packet_start: {parsed_data.get('rx_start', 'N/A')}\n"
                    f"tx_packet_start: {parsed_data.get('tx_start', 'N/A')}\n"
                    f"rx_packet_end: {parsed_data.get('rx_end', 'N/A')}\n"
                    f"tx_packet_end: {parsed_data.get('tx_end', 'N/A')}\n"
                )

                # Check RX/TX per representor; summing can count the same packet twice.
                if r1.success and r2.success and parsed_start and parsed_end:
                    m1 = check_no_traffic_on_vf_rep(
                        parsed_data,
                        "rx",
                        stats_backend,
                        self.plugin.vf_rep_traffic_threshold,
                    )
                    m2 = check_no_traffic_on_vf_rep(
                        parsed_data,
                        "tx",
                        stats_backend,
                        self.plugin.vf_rep_traffic_threshold,
                    )
                    if m1 is not None or m2 is not None:
                        success_result = False
                        failure = "; ".join(
                            message for message in (m1, m2) if message is not None
                        )
                        failure = f"{rep}: {failure}" if host_network else failure
                        msg = f"{msg}; {failure}" if msg else failure

            return PluginOutput(
                success=success_result,
                msg=msg,
                plugin_metadata=self.get_plugin_metadata(),
                command="; ".join(commands.values()),
                result=result_data,
            )

        return TaskOperation(
            log_name=self.log_name,
            thread_action=_thread_action,
        )

    def _aggregate_output_log_success(
        self,
        result: tftbase.AggregatableOutput,
    ) -> None:
        assert isinstance(result, PluginOutput)

        if (
            self.perf_pod_type == PodType.HOSTBACKED
            and self.plugin.host_network_interfaces is None
        ):
            if isinstance(self._perf_instance, task.ClientTask):
                logger.info("The client VF representor ovn-k8s-mp0_0 does not exist")
            else:
                logger.info("The server VF representor ovn-k8s-mp0_0 does not exist")

        msg_suffix = f", {result.msg}" if result.msg is not None else ""
        logger.info(
            f"{self.plugin.PLUGIN_NAME} results on {self.perf_pod_name}: "
            f"success={result.success}{msg_suffix}"
        )

        logger.debug(f"Full results: {result.result}")
