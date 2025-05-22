import typing

from typing import Optional

from ktoolbox import common

import pluginbase
import task
import tftbase

from task import PluginTask
from task import TaskOperation
from testSettings import TestSettings
from tftbase import BaseOutput
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

        self.pod_name = f"tools-pod-{self.node_name_sanitized()}-validate-offload"
        self.in_file_template = tftbase.get_manifest("tools-pod.yaml.j2")
        self.out_file_yaml = tftbase.get_manifest_renderpath(self.pod_name + ".yaml")
        self._perf_instance = perf_instance
        self.perf_pod_name = perf_instance.pod_name
        self.perf_pod_type = perf_instance.pod_type

    def initialize(self) -> None:
        super().initialize()
        self.render_pod_file("Plugin Pod Yaml")

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
                r1 = self.run_oc_exec(ethtool_cmd)

                self.ts.event_client_finished.wait()

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
