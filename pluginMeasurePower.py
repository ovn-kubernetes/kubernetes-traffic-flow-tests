import re
import time

from typing import Any
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
from tftbase import TaskRole


logger = common.ExtendedLogger("tft." + __name__)


class PluginMeasurePower(pluginbase.Plugin):
    PLUGIN_NAME = "measure_power"

    def _enable(
        self,
        *,
        ts: TestSettings,
        perf_server: task.ServerTask,
        perf_client: task.ClientTask,
        tenant: bool,
    ) -> list[PluginTask]:
        return [
            TaskMeasurePower(ts, TaskRole.SERVER, tenant),
            TaskMeasurePower(ts, TaskRole.CLIENT, tenant),
        ]


plugin = pluginbase.register_plugin(PluginMeasurePower())


def _extract(ipmitool_output: str) -> Optional[int]:
    for e in ipmitool_output.split("\n"):
        match = re.search(r"^ *Instantaneous power reading: +(\d+) +Watts *$", e)
        if match:
            return int(match.group(1))
    return None


class TaskMeasurePower(PluginTask):
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
        self.out_file_yaml = tftbase.get_manifest_renderpath(self.pod_name + ".yaml")

    def initialize(self) -> None:
        super().initialize()
        self.render_pod_file("Plugin Pod Yaml")

    def _create_task_operation(self) -> TaskOperation:
        def _thread_action() -> BaseOutput:
            cmd = "ipmitool dcmi power reading"
            self.ts.clmo_barrier.wait()

            success_result = True
            msg: Optional[str] = None
            total_pwr = 0
            iteration = 0
            result: dict[str, Any] = {}
            while not self.ts.event_client_finished.is_set():
                r = self.run_oc_exec(cmd)
                if not r.success:
                    if success_result:
                        success_result = False
                        result["failed_cmd"] = common.dataclass_to_dict(r)
                        msg = "Failed running ipmitool command"
                else:
                    pwr = _extract(r.out)
                    if pwr is None:
                        if success_result:
                            success_result = False
                            result["failed_cmd"] = common.dataclass_to_dict(r)
                            msg = "Failed to parse ipmitool output"
                    else:
                        total_pwr += pwr
                iteration += 1
                time.sleep(0.2)

            result["measure_power"] = f"{total_pwr/iteration}"

            return PluginOutput(
                success=success_result,
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
        logger.info(f"measurePower results: {result.result['measure_power']}")
