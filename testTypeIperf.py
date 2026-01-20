import json
import task

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from typing import Optional

from ktoolbox import common

import tftbase

from task import ClientTask
from task import ServerTask
from task import TaskOperation
from testSettings import TestSettings
from testType import TestTypeHandler
from tftbase import BaseOutput
from tftbase import Bitrate
from tftbase import FlowTestOutput
from tftbase import TestType

logger = common.ExtendedLogger("tft." + __name__)


IPERF_EXE = "iperf3"
IPERF_UDP_OPT = "-u -b 25G"
IPERF_REV_OPT = "-R"


class ResultTcp:
    def __init__(self, data: Mapping[str, Any]):
        sum_sent: Mapping[str, Any] = data["end"]["sum_sent"]
        sum_received: Mapping[str, Any] = data["end"]["sum_received"]

        self.transfer_sent = float(sum_sent["bytes"]) / (1024**3)
        self.bitrate_sent = float(sum_sent["bits_per_second"]) / 1e9
        self.transfer_received = float(sum_received["bytes"]) / (1024**3)
        self.bitrate_received = float(sum_received["bits_per_second"]) / 1e9
        self.mss = int(data["start"]["tcp_mss_default"])
        self.sum_sent_seconds = float(sum_sent["seconds"])
        self.sum_received_seconds = float(sum_received["seconds"])

        self.bitrate = Bitrate(
            tx=float(f"{self.bitrate_sent:.5g}"),
            rx=float(f"{self.bitrate_received:.5g}"),
        )

    def log(self) -> None:
        logger.info(
            f"\n  [ ID]   Interval              Transfer        Bitrate\n"
            f"  [SENT]   0.00-{self.sum_sent_seconds:.2f} sec   {self.transfer_sent:.2f} GBytes  {self.bitrate_sent:.2f} Gbits/sec sender\n"
            f"  [REC]   0.00-{self.sum_received_seconds:.2f} sec   {self.transfer_received:.2f} GBytes  {self.bitrate_received:.2f} Gbits/sec receiver\n"
            f"  MSS = {self.mss}"
        )


class ResultUdp:
    def __init__(self, data: Mapping[str, Any]):
        sum_data: Mapping[str, Any] = data["end"]["sum"]

        self.total_gigabytes = float(sum_data["bytes"]) / (1024**3)
        self.average_gigabitrate = float(sum_data["bits_per_second"]) / 1e9
        self.average_jitter = float(sum_data["jitter_ms"])
        self.total_lost_packets = float(sum_data["lost_packets"])
        self.total_lost_percent = float(sum_data["lost_percent"])

        self.bitrate = Bitrate(
            tx=float(f"{self.average_gigabitrate:.5g}"),
            rx=float(f"{self.average_gigabitrate:.5g}"),
        )

    def log(self) -> None:
        logger.info(
            f"\n  Total GBytes: {self.total_gigabytes:.4f} GBytes\n"
            f"  Average Bitrate: {self.average_gigabitrate:.2f} Gbits/s\n"
            f"  Average Jitter: {self.average_jitter:.9f} ms\n"
            f"  Total Lost Packets: {self.total_lost_packets}\n"
            f"  Total Lost Percent: {self.total_lost_percent:.2f}%"
        )


def _calculate_gbps(test_type: TestType, result: Mapping[str, Any]) -> Bitrate:
    try:
        if test_type == TestType.IPERF_TCP:
            return ResultTcp(result).bitrate
        else:
            return ResultUdp(result).bitrate
    except Exception:
        return Bitrate.NA


@dataclass(frozen=True)
class TestTypeHandlerIperf(TestTypeHandler):
    def _create_server_client(self, ts: TestSettings) -> tuple[ServerTask, ClientTask]:
        s = IperfServer(ts=ts)
        c = IperfClient(ts=ts, server=s)
        return (s, c)

    def can_run_reverse(self) -> bool:
        if self.test_type == TestType.IPERF_TCP:
            return True
        return False


TestTypeHandler.register_test_type(TestTypeHandlerIperf(TestType.IPERF_TCP))
TestTypeHandler.register_test_type(TestTypeHandlerIperf(TestType.IPERF_UDP))


class IperfServer(task.ServerTask):
    def cmd_line_args(self, *, for_template: bool = False) -> list[str]:
        if for_template:
            extra_args = []
        else:
            extra_args = ["--one-off", "--json"]
        return [
            IPERF_EXE,
            "-s",
            "-p",
            f"{self.port}",
            *extra_args,
        ]

    def _create_setup_operation_get_cancel_action_cmd(self) -> str:
        return f"killall {IPERF_EXE}"

    def _get_server_listen_protocol(self) -> Optional[str]:
        # You may wonder why even for TestType.IPERF_UDP, we would detect for TCP here.
        # The reason is that iperf3 server always listens on TCP even for UDP tests.
        # Iperf3 defaults to listening to TCP port for the control channel. This is used for
        # managing the test setup and reporting.
        return "tcp"


class IperfClient(task.ClientTask):
    def _create_task_operation(self) -> TaskOperation:
        server_ip = self.get_target_ip()
        target_port = self.get_target_port()
        cmd = f"{IPERF_EXE} -c {server_ip} -p {target_port} --json -t {self.get_duration()}"
        if self.test_type == TestType.IPERF_UDP:
            cmd += f" {IPERF_UDP_OPT}"
        if self.reverse:
            cmd += f" {IPERF_REV_OPT}"

        def _thread_action() -> BaseOutput:
            self.ts.clmo_barrier.wait()
            r = self.run_oc_exec(cmd)
            self.ts.event_client_finished.set()

            success = True
            msg: Optional[str] = None
            result: dict[str, Any] = {}

            if not r.success:
                success = False
                msg = f'Command "{cmd}" failed: {r.debug_msg()}'

            # Log raw output for debugging
            logger.debug(f"iperf3 raw output: {repr(r.out)}")

            if success:
                try:
                    result = json.loads(r.out)
                except Exception as e:
                    success = False
                    msg = f'Output of "{cmd}" is not valid JSON ({e}): {r.debug_msg()}'
                    logger.error(
                        f"Failed to parse JSON. Raw output: {repr(r.out[:500])}"
                    )

            if success:
                if (
                    not result
                    or not isinstance(result, dict)
                    or not all(isinstance(k, str) for k in result)
                ):
                    success = False
                    msg = f'Output of "{cmd}" contains unexpected data: {r.debug_msg()}'

            if success:
                if "error" in result:
                    success = False
                    msg = f'Output of "{cmd}" contains "error": {r.debug_msg()}'

            bitrate_gbps = _calculate_gbps(self.test_type, result)
            if success:
                if bitrate_gbps == Bitrate.NA:
                    success = False
                    msg = f'Output of "{cmd}" does not contain expected data: {r.debug_msg()}'

            return FlowTestOutput(
                success=success,
                msg=msg,
                tft_metadata=self.ts.get_test_metadata(),
                command=cmd,
                result=result,
                bitrate_gbps=bitrate_gbps,
            )

        return TaskOperation(
            log_name=self.log_name,
            thread_action=_thread_action,
        )

    def _aggregate_output_log_success(
        self,
        result: tftbase.AggregatableOutput,
    ) -> None:
        assert isinstance(result, FlowTestOutput)
        if self.test_type == TestType.IPERF_TCP:
            ResultTcp(result.result).log()
        else:
            ResultUdp(result.result).log()
