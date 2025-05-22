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
        return [
            TaskMeasureCPU(ts, TaskRole.SERVER, tenant),
            TaskMeasureCPU(ts, TaskRole.CLIENT, tenant),
        ]


plugin = pluginbase.register_plugin(PluginMeasureCpu())


class TaskMeasureCPU(PluginTask):
    @property
    def plugin(self) -> pluginbase.Plugin:
        return plugin

    def __init__(self, ts: TestSettings, task_role: TaskRole, tenant: bool):
        super().__init__(
            ts=ts,
            index=0,
            task_role=task_role,
            tenant=tenant,
        )

        self.pod_name = f"tools-pod-{self.node_name_sanitized()}-measure-cpu"
        self.in_file_template = tftbase.get_manifest("tools-pod.yaml.j2")

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
