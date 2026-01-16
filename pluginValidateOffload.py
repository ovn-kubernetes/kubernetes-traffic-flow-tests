import re
import typing

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
) -> Optional[str]:
    start = common.dict_get_typed(
        parsed_data, KEY_NAMES["start"][direction], int, allow_missing=True
    )
    end = common.dict_get_typed(
        parsed_data, KEY_NAMES["end"][direction], int, allow_missing=True
    )
    if start is None or end is None:
        if start is not None or end is not None:
            return f"missing ethtool output for {direction}"
        return None
    if end - start >= VF_REP_TRAFFIC_THRESHOLD:
        return f"traffic on VF rep detected for {repr(direction)} ({end-start} packets is higher than threshold {VF_REP_TRAFFIC_THRESHOLD})"
    return None


class PluginValidateOffload(pluginbase.Plugin):
    PLUGIN_NAME = "validate_offload"

    def _enable(
        self,
        *,
        ts: TestSettings,
        perf_server: task.ServerTask,
        perf_client: task.ClientTask,
        tenant: bool,
    ) -> list[PluginTask]:
        # TODO allow this to run on each individual server + client pairs.
        return [
            TaskValidateOffload(ts, TaskRole.SERVER, perf_server, tenant),
            TaskValidateOffload(ts, TaskRole.CLIENT, perf_client, tenant),
        ]


plugin = pluginbase.register_plugin(PluginValidateOffload())


class TaskValidateOffload(PluginTask):
    @property
    def plugin(self) -> pluginbase.Plugin:
        return plugin

    @property
    def _is_dpu_mode(self) -> bool:
        return self.tc.mode == ClusterMode.DPU

    def __init__(
        self,
        ts: TestSettings,
        task_role: TaskRole,
        perf_instance: task.ServerTask | task.ClientTask,
        tenant: bool,
    ):
        super().__init__(
            ts=ts,
            index=0,
            task_role=task_role,
            tenant=tenant,
        )

        self.pod_name = f"tools-pod-{self.node_name_sanitized}-validate-offload"
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
            self._initialize_dpu_pod()

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

    def _create_task_operation(self) -> TaskOperation:
        def _thread_action() -> BaseOutput:

            success_result = True
            msg: Optional[str] = None
            ethtool_cmd = ""
            parsed_data: dict[str, typing.Any] = {}
            data1 = ""
            data2 = ""
            vf_rep: Optional[str] = None

            if self.perf_pod_type == PodType.HOSTBACKED:
                logger.info("The VF representor is: ovn-k8s-mp0")
                msg = "Hostbacked pod"
            elif self.perf_pod_name == task.EXTERNAL_PERF_SERVER:
                logger.info("There is no VF on an external server")
                msg = "External Iperf Server"
            else:
                # Get VF representor - use DPU mode if configured
                if self._is_dpu_mode:
                    logger.info("DPU mode: querying VF representor from DPU cluster")
                    vf_rep = self._get_vf_rep_dpu_mode(
                        pod_name=self.perf_pod_name,
                        ifname="eth0",
                    )
                else:
                    vf_rep = self.pod_get_vf_rep(
                        pod_name=self.perf_pod_name,
                        ifname="eth0",
                        host_pod_name=self.pod_name,
                    )

                if vf_rep is None:
                    success_result = False
                    msg = "cannot determine VF_REP for pod"
                    logger.error(
                        f"VF representor for {self.perf_pod_name} not detected"
                    )
                else:
                    logger.info(
                        f"VF representor for eth0 in pod {self.perf_pod_name} is {repr(vf_rep)}"
                    )
                    ethtool_cmd = f"ethtool -S {vf_rep}"

            self.ts.clmo_barrier.wait()

            if vf_rep is not None:
                # Run ethtool - use DPU pod if in DPU mode
                if self._is_dpu_mode:
                    r1 = self._run_oc_exec_dpu(ethtool_cmd)
                else:
                    r1 = self.run_oc_exec(ethtool_cmd)

                self.ts.event_client_finished.wait()

                if self._is_dpu_mode:
                    r2 = self._run_oc_exec_dpu(ethtool_cmd)
                else:
                    r2 = self.run_oc_exec(ethtool_cmd)

                parsed_data["ethtool_cmd_1"] = common.dataclass_to_dict(r1)
                parsed_data["ethtool_cmd_2"] = common.dataclass_to_dict(r2)

                if r1.success:
                    data1 = r1.out
                if r2.success:
                    data2 = r2.out

                if not r1.success:
                    success_result = False
                    msg = "ethtool command failed"
                elif not r2.success:
                    success_result = False
                    msg = "ethtool command at end failed"

                if not ethtool_stat_get_startend(parsed_data, data1, "start"):
                    if success_result:
                        success_result = False
                        msg = "ethtool output cannot be parsed"
                if not ethtool_stat_get_startend(parsed_data, data2, "end"):
                    if success_result:
                        success_result = False
                        msg = "ethtool output at end cannot be parsed"

                logger.info(
                    f"rx_packet_start: {parsed_data.get('rx_start', 'N/A')}\n"
                    f"tx_packet_start: {parsed_data.get('tx_start', 'N/A')}\n"
                    f"rx_packet_end: {parsed_data.get('rx_end', 'N/A')}\n"
                    f"tx_packet_end: {parsed_data.get('tx_end', 'N/A')}\n"
                )

                if success_result:
                    m1 = check_no_traffic_on_vf_rep(parsed_data, "rx")
                    m2 = check_no_traffic_on_vf_rep(parsed_data, "tx")
                    if m1 is not None or m2 is not None:
                        success_result = False
                        msg = m1 if m1 is not None else m2

            return PluginOutput(
                success=success_result,
                msg=msg,
                plugin_metadata=self.get_plugin_metadata(),
                command=ethtool_cmd,
                result=parsed_data,
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

        if self.perf_pod_type == PodType.HOSTBACKED:
            if isinstance(self._perf_instance, task.ClientTask):
                logger.info("The client VF representor ovn-k8s-mp0_0 does not exist")
            else:
                logger.info("The server VF representor ovn-k8s-mp0_0 does not exist")

        logger.info(f"validateOffload results on {self.perf_pod_name}: {result.result}")
