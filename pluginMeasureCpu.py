import logging
import jc

from typing import Any
from typing import cast

import task
import pluginbase
import tftbase

from task import PluginTask
from task import TaskOperation
from testSettings import TestSettings
from tftbase import BaseOutput
from tftbase import PluginOutput
from tftbase import TaskRole


logger = logging.getLogger("tft." + __name__)


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

        self.in_file_template = tftbase.get_manifest("tools-pod.yaml.j2")
        self.out_file_yaml = tftbase.get_manifest_renderpath(
            f"tools-pod-{self.node_name}-measure-cpu.yaml"
        )
        self.pod_name = f"tools-pod-{self.node_name}-measure-cpu"

    def get_template_args(self) -> dict[str, str | list[str]]:
        return {
            **super().get_template_args(),
            "pod_name": self.pod_name,
            "test_image": tftbase.get_tft_test_image(),
        }

    def initialize(self) -> None:
        super().initialize()
        self.render_file("Server Pod Yaml")

    def _create_task_operation(self) -> TaskOperation:
        def _thread_action() -> BaseOutput:

            self.ts.clmo_barrier.wait()

            cmd = f"mpstat -P ALL {self.get_duration()} 1"
            r = self.run_oc_exec(cmd)

            data = r.out

            # satisfy the linter. jc.parse returns a list of dicts in this case
            parsed_data = cast(list[dict[str, Any]], jc.parse("mpstat", data))
            return PluginOutput(
                plugin_metadata=self.get_plugin_metadata(),
                command=cmd,
                result=parsed_data[0],
            )

        return TaskOperation(
            log_name=self.log_name,
            thread_action=_thread_action,
        )

    def _aggregate_output(
        self,
        result: tftbase.AggregatableOutput,
        tft_result_builder: tftbase.TftResultBuilder,
    ) -> None:
        result = tft_result_builder.add_plugin(result)
        p_idle = result.result["percent_idle"]
        logger.info(f"Idle on {self.node_name} = {p_idle}%")
