import jc
import typing

from typing import Any
from typing import Optional

from ktoolbox import common

import task
import pluginbase
import tftbase

from task import PluginTask
from task import TaskOperation
from testSettings import TestSettings
from tftbase import BaseOutput
from tftbase import ClusterMode
from tftbase import PluginOutput
from tftbase import TaskRole

logger = common.ExtendedLogger("tft." + __name__)


class PluginMeasureCpu(pluginbase.Plugin):
    PLUGIN_NAME = "measure_cpu"

    def _enable(
        self,
        *,
        ts: TestSettings,
        perf_server: task.ServerTask,
        perf_client: task.ClientTask,
        tenant: bool,
    ) -> list[PluginTask]:
        if ts.cfg_descr.tc.mode == ClusterMode.DPU:
            tenant = False
        return [
            TaskMeasureCPU(ts, TaskRole.SERVER, tenant),
            TaskMeasureCPU(ts, TaskRole.CLIENT, tenant),
        ]


plugin = pluginbase.register_plugin(PluginMeasureCpu())


class TaskMeasureCPU(PluginTask):
    @property
    def _is_dpu_mode(self) -> bool:
        return self.tc.mode == ClusterMode.DPU

    @property
    def node_name(self) -> str:
        return self._dpu_node_name or super().node_name

    @property
    def plugin(self) -> pluginbase.Plugin:
        return plugin

    def __init__(self, ts: TestSettings, task_role: TaskRole, tenant: bool):
        self._dpu_node_name: Optional[str] = None
        super().__init__(
            ts=ts,
            index=0,
            task_role=task_role,
            tenant=tenant,
        )

        if self._is_dpu_mode:
            # OVS, VF representors, and slow-path packet processing run on the DPU,
            # so measure the paired DPU instead of the host worker.
            self._dpu_node_name = self._get_dpu_node_name()
            logger.info(
                f"Measuring CPU on DPU node {self._dpu_node_name} "
                f"paired with {super().node_name}"
            )

        self.pod_name = (
            f"tools-pod-{self.node_location}-{self.task_role.name.lower()}-measure-cpu"
        )
        self.in_file_template = tftbase.get_manifest("tools-pod.yaml.j2")

    def _get_dpu_node_name(self) -> str:
        """Get the DPU node corresponding to the configured host worker.

        DPU nodes are paired to host workers through dpu_node_host_label.
        """
        host_node_name = super().node_name
        host_label = self.tc.dpu_node_host_label
        if not host_label:
            raise ValueError(
                "dpu_node_host_label must be configured when running in DPU mode. "
                "Set it in config.yaml (e.g., dpu_node_host_label: "
                "'provisioning.dpu.nvidia.com/host')"
            )

        selector = f"{host_label}={host_node_name}"
        result = self.tc.client_infra.oc(
            f"get nodes -l {selector} -o jsonpath='{{.items[*].metadata.name}}'",
            may_fail=True,
        )
        if not result.success:
            raise RuntimeError(
                f"Failed to query DPU nodes by label {selector}: {result.err}"
            )

        dpu_nodes = result.out.strip().strip("'\"").split()
        if not dpu_nodes:
            raise RuntimeError(
                f"No DPU node found with label {selector}. "
                f"Ensure DPU nodes have label '{host_label}' set to the worker node name."
            )
        if len(dpu_nodes) == 1:
            logger.info(
                f"Found DPU node {dpu_nodes[0]} for worker {host_node_name} via label"
            )
            return dpu_nodes[0]

        if len(dpu_nodes) > 1:
            logger.warning(
                f"Multiple DPU nodes found with label {selector}: {dpu_nodes}. "
                f"Using first: {dpu_nodes[0]}"
            )
        return dpu_nodes[0]

    def initialize(self) -> None:
        super().initialize()
        self.render_pod_file("Plugin Pod Yaml")

    def _create_task_operation(self) -> TaskOperation:
        def _thread_action() -> BaseOutput:

            self.ts.clmo_barrier.wait()

            cmd = f"mpstat -P ALL {self.get_duration()} 1"
            r = self.run_oc_exec(cmd)

            success = True
            msg: Optional[str] = None
            result: dict[str, Any] = {}

            if not r.success:
                success = False
                msg = r.debug_msg()

            if success:
                try:
                    lst = typing.cast(list[dict[str, Any]], jc.parse("mpstat", r.out))
                    rdict = lst[0]
                except Exception:
                    success = False
                    msg = f'Output of "{cmd}" cannot be parsed: {r.debug_msg()}'

            if success:
                if (
                    isinstance(rdict, dict)
                    and all(isinstance(k, str) for k in rdict)
                    and all(required_key in rdict for required_key in ("percent_idle",))
                ):
                    result = rdict
                else:
                    success = False
                    msg = 'Output of "{cmd}" contains unexpected data: {r.debug_msg()}'

            result["cmd"] = common.dataclass_to_dict(r)

            return PluginOutput(
                success=success,
                msg=msg,
                plugin_metadata=self.get_plugin_metadata(),
                command=cmd,
                result=result,
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
        p_idle = result.result["percent_idle"]
        logger.info(f"Idle on {self.node_name} = {p_idle}%")
