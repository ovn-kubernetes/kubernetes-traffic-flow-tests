import task

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


# RDMA perftest tools
RDMA_WRITE_BW_EXE = "ib_write_bw"
RDMA_READ_BW_EXE = "ib_read_bw"
RDMA_SEND_BW_EXE = "ib_send_bw"

# Default options for RDMA tests
RDMA_DEFAULT_OPTS = "--report_gbits"


def _get_rdma_exe(test_type: TestType) -> str:
    if test_type == TestType.IB_WRITE_BW:
        return RDMA_WRITE_BW_EXE
    elif test_type == TestType.IB_READ_BW:
        return RDMA_READ_BW_EXE
    elif test_type == TestType.IB_SEND_BW:
        return RDMA_SEND_BW_EXE
    raise ValueError(f"Unknown RDMA test type: {test_type}")


class RdmaResult:
    """Parse RDMA perftest output to extract bandwidth results."""

    def __init__(self, output: str):
        self.output = output
        self.bandwidth_gbps: float = 0.0
        self.msg_rate: float = 0.0
        self._parse_output()

    def _parse_output(self) -> None:
        # perftest tools output format varies:
        #
        # Without --duration (5 data columns):
        #   #bytes  #iterations  BW peak[Gb/sec]  BW average[Gb/sec]  MsgRate[Mpps]
        #   65536   5000         98.12            97.85               0.186652
        #
        # With --duration (4 data columns, no #iterations):
        #   #bytes  BW peak[Gb/sec]  BW average[Gb/sec]  MsgRate[Mpps]
        #   65536   128.05           128.00              0.244094
        #
        lines = self.output.strip().split("\n")
        for line in lines:
            # Skip header and comment lines
            if line.startswith("#") or "#bytes" in line or not line.strip():
                continue
            # Try to parse data line - first column should be message size (integer)
            parts = line.split()
            if len(parts) >= 4:
                try:
                    # Verify first column is an integer (message size in bytes)
                    int(parts[0])

                    if len(parts) >= 5:
                        # Format with #iterations: bytes, iters, peak, avg, msgrate
                        self.bandwidth_gbps = float(parts[3])
                        self.msg_rate = float(parts[4])
                    else:
                        # Format with --duration: bytes, peak, avg, msgrate
                        self.bandwidth_gbps = float(parts[2])
                        self.msg_rate = float(parts[3])
                    break
                except (ValueError, IndexError):
                    continue

    @property
    def bitrate(self) -> Bitrate:
        return Bitrate(
            tx=float(f"{self.bandwidth_gbps:.5g}"),
            rx=float(f"{self.bandwidth_gbps:.5g}"),
        )

    def log(self) -> None:
        logger.info(
            f"\n  RDMA Bandwidth: {self.bandwidth_gbps:.2f} Gbits/s\n"
            f"  Message Rate: {self.msg_rate:.6f} Mpps"
        )


def _calculate_gbps(output: str) -> Bitrate:
    try:
        result = RdmaResult(output)
        return result.bitrate
    except Exception:
        return Bitrate.NA


@dataclass(frozen=True)
class TestTypeHandlerRdma(TestTypeHandler):
    def _create_server_client(self, ts: TestSettings) -> tuple[ServerTask, ClientTask]:
        s = RdmaServer(ts=ts)
        c = RdmaClient(ts=ts, server=s)
        return (s, c)

    def can_run_reverse(self) -> bool:
        return False


TestTypeHandler.register_test_type(TestTypeHandlerRdma(TestType.IB_WRITE_BW))
TestTypeHandler.register_test_type(TestTypeHandlerRdma(TestType.IB_READ_BW))
TestTypeHandler.register_test_type(TestTypeHandlerRdma(TestType.IB_SEND_BW))


class RdmaServer(task.ServerTask):
    def cmd_line_args(self, *, for_template: bool = False) -> list[str]:
        rdma_exe = _get_rdma_exe(self.ts.connection.test_type)
        # Append any user-specified args from configuration
        server_args = self.ts.cfg_descr.get_server().args or ()
        return [
            rdma_exe,
            "--port",
            f"{self.port}",
            RDMA_DEFAULT_OPTS,
            *server_args,
        ]

    def _create_setup_operation_get_cancel_action_cmd(self) -> str:
        rdma_exe = _get_rdma_exe(self.ts.connection.test_type)
        return f"killall {rdma_exe}"

    def _get_server_listen_protocol(self) -> Optional[str]:
        # RDMA perftest tools listen on TCP for the control channel
        return "tcp"


class RdmaClient(task.ClientTask):
    def _create_task_operation(self) -> TaskOperation:
        server_ip = self.get_target_ip()
        target_port = self.get_target_port()
        rdma_exe = _get_rdma_exe(self.test_type)
        duration = self.get_duration()

        # ib_send_bw doesn't work well with --duration, use iterations instead
        if self.test_type == TestType.IB_SEND_BW:
            cmd = f"{rdma_exe} {server_ip} --port {target_port} {RDMA_DEFAULT_OPTS}"
        else:
            cmd = f"{rdma_exe} {server_ip} --port {target_port} {RDMA_DEFAULT_OPTS} --duration {duration}"

        # Append any user-specified args from configuration
        client_args = self.ts.cfg_descr.get_client().args
        if client_args:
            cmd += " " + " ".join(client_args)

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
            logger.debug(f"RDMA raw output: {repr(r.out)}")

            bitrate_gbps = Bitrate.NA
            if success:
                bitrate_gbps = _calculate_gbps(r.out)
                if bitrate_gbps == Bitrate.NA:
                    success = False
                    msg = f'Output of "{cmd}" does not contain expected data: {r.debug_msg()}'
                else:
                    result = {
                        "output": r.out,
                        "bandwidth_gbps": bitrate_gbps.tx,
                    }

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
        if "output" in result.result:
            RdmaResult(result.result["output"]).log()
