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
from tftbase import TaskRole


logger = common.ExtendedLogger("tft." + __name__)


DEFAULT_BANDWIDTH_THRESHOLD = 18.0  # Gbps


def validate_bandwidth(
    bitrate_gbps: tftbase.Bitrate,
    threshold_gbps: float,
) -> Optional[str]:
    """Validate bandwidth against threshold.

    Args:
        bitrate_gbps: Bitrate object with tx/rx values
        threshold_gbps: Minimum expected bandwidth in Gbps

    Returns:
        Error message if validation fails, None if passes
    """
    if bitrate_gbps.tx is None or bitrate_gbps.rx is None:
        return "Bitrate is None - cannot validate bandwidth"

    if bitrate_gbps.tx < threshold_gbps:
        return f"TX bitrate ({bitrate_gbps.tx:.2f} Gbps) is below threshold ({threshold_gbps} Gbps)"

    if bitrate_gbps.rx < threshold_gbps:
        return f"RX bitrate ({bitrate_gbps.rx:.2f} Gbps) is below threshold ({threshold_gbps} Gbps)"

    return None


class PluginValidateBandwidth(pluginbase.Plugin):
    PLUGIN_NAME = "validate_bandwidth"

    def _enable(
        self,
        *,
        ts: TestSettings,
        perf_server: task.ServerTask,
        perf_client: task.ClientTask,
        tenant: bool,
    ) -> list[PluginTask]:
        # Only need one task since we're validating the test results
        return [
            TaskValidateBandwidth(ts, TaskRole.CLIENT, perf_client, tenant),
        ]


plugin = pluginbase.register_plugin(PluginValidateBandwidth())


class TaskValidateBandwidth(PluginTask):
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

        self.pod_name = f"tools-pod-{self.node_name_sanitized}-validate-bandwidth"
        self.in_file_template = tftbase.get_manifest("tools-pod.yaml.j2")
        self._perf_instance = perf_instance
        self.perf_pod_name = perf_instance.pod_name
        self.perf_pod_type = perf_instance.pod_type

    def initialize(self) -> None:
        super().initialize()
        self.render_pod_file("Plugin Pod Yaml")

    def _create_task_operation(self) -> TaskOperation:
        def _thread_action() -> BaseOutput:
            # Always use the default bandwidth threshold
            bandwidth_threshold = DEFAULT_BANDWIDTH_THRESHOLD

            logger.info(
                f"Bandwidth validation plugin initialized with threshold: {bandwidth_threshold} Gbps"
            )

            success_result = True
            msg = f"Bandwidth validation plugin initialized with threshold: {bandwidth_threshold} Gbps"
            parsed_data = {
                "bandwidth_threshold_gbps": bandwidth_threshold,
                "validation_pending": True,
            }

            return PluginOutput(
                success=success_result,
                msg=msg,
                plugin_metadata=self.get_plugin_metadata(),
                command="bandwidth_validation",
                result=parsed_data,
            )

        return TaskOperation(log_name=self.log_name, thread_action=_thread_action)

    def aggregate_output(self, tft_result_builder: tftbase.TftResultBuilder) -> None:
        """Override aggregate_output to perform bandwidth validation after flow test results are available."""
        if self._result is None:
            return
        if not isinstance(self._result, tftbase.AggregatableOutput):
            return

        result = self._result

        if isinstance(result, tftbase.PluginOutput):
            # Check if we have flow test results available for validation
            flow_test = getattr(tft_result_builder, "_flow_test", None)
            if flow_test is not None and isinstance(flow_test, tftbase.FlowTestOutput):
                # Perform the actual bandwidth validation
                validated_result = self.validate_test_results(flow_test)
                tft_result_builder.add_plugin(validated_result)
            else:
                # Fall back to the original result if no flow test available
                tft_result_builder.add_plugin(result)

        if not result.success:
            logger.warn(f"Result of {type(self).__name__} failed: {result.eval_msg}")
        else:
            self._aggregate_output_log_success(result)

    def _aggregate_output_log_success(
        self,
        result: tftbase.AggregatableOutput,
    ) -> None:
        assert isinstance(result, PluginOutput)
        logger.info(f"validateBandwidth results: {result.result}")

    def validate_test_results(
        self, flow_test_output: tftbase.FlowTestOutput
    ) -> tftbase.PluginOutput:
        """Validate the bandwidth from the flow test results.

        This method is called during result aggregation when we have access
        to the actual test results.
        """
        # Always use the default bandwidth threshold
        bandwidth_threshold = DEFAULT_BANDWIDTH_THRESHOLD

        success = True
        msg: Optional[str] = None
        parsed_data: dict[str, typing.Any] = {
            "bandwidth_threshold_gbps": bandwidth_threshold,
            "actual_tx_gbps": flow_test_output.bitrate_gbps.tx,
            "actual_rx_gbps": flow_test_output.bitrate_gbps.rx,
        }

        if not flow_test_output.success:
            success = False
            msg = "Cannot validate bandwidth - flow test failed"
        else:
            validation_error = validate_bandwidth(
                flow_test_output.bitrate_gbps, bandwidth_threshold
            )
            if validation_error:
                success = False
                msg = validation_error
            else:
                msg = f"Bandwidth validation passed - both TX ({flow_test_output.bitrate_gbps.tx:.2f} Gbps) and RX ({flow_test_output.bitrate_gbps.rx:.2f} Gbps) meet threshold ({bandwidth_threshold} Gbps)"

        parsed_data["validation_success"] = success
        parsed_data["validation_message"] = msg

        return PluginOutput(
            success=success,
            msg=msg,
            plugin_metadata=self.get_plugin_metadata(),
            command="bandwidth_validation",
            result=parsed_data,
        )
