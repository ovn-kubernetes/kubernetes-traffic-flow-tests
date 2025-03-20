import shlex

from dataclasses import dataclass

from ktoolbox import common

import task
import tftbase

from task import ClientTask
from task import ServerTask
from task import TaskOperation
from testSettings import TestSettings
from testType import TestTypeHandler
from tftbase import BaseOutput
from tftbase import FlowTestOutput
from tftbase import TestType


@dataclass(frozen=True)
class TestTypeHandlerSimple(TestTypeHandler):
    def __init__(self) -> None:
        super().__init__(TestType.SIMPLE)

    def _create_server_client(self, ts: TestSettings) -> tuple[ServerTask, ClientTask]:
        s = SimpleServer(ts=ts)
        c = SimpleClient(ts=ts, server=s)
        return (s, c)


TestTypeHandler.register_test_type(TestTypeHandlerSimple())

CMD_SIMPLE_TCP_SERVER_CLIENT = "simple-tcp-server-client"


class SimpleServer(task.ServerTask):
    def cmd_line_args(self, *, for_template: bool = False) -> list[str]:
        return [
            CMD_SIMPLE_TCP_SERVER_CLIENT,
            "--server",
            "--addr",
            "0.0.0.0",
            "--port",
            f"{self.port}",
            *(self.ts.cfg_descr.get_server().args or ()),
        ]

    def get_template_args(self) -> dict[str, str | list[str]]:

        extra_args: dict[str, str | list[str]] = {}
        if self.exec_persistent:
            extra_args["args"] = self.cmd_line_args(for_template=True)

        return {
            **super().get_template_args(),
            **extra_args,
        }

    def _create_setup_operation_get_thread_action_cmd(self) -> str:
        return shlex.join(self.cmd_line_args())

    def _create_setup_operation_get_cancel_action_cmd(self) -> str:
        return "killall python3"


class SimpleClient(task.ClientTask):
    def cmd_line_args(self) -> list[str]:
        return [
            CMD_SIMPLE_TCP_SERVER_CLIENT,
            "--addr",
            f"{self.get_target_ip()}",
            "--port",
            f"{self.port}",
            "--duration",
            f"{self.get_duration()}",
            *(self.ts.cfg_descr.get_client().args or ()),
        ]

    def _create_task_operation(self) -> TaskOperation:
        cmd = shlex.join(self.cmd_line_args())

        def _thread_action() -> BaseOutput:
            self.ts.clmo_barrier.wait()
            r = self.run_oc_exec(cmd)
            self.ts.event_client_finished.set()

            return FlowTestOutput(
                success=r.success,
                tft_metadata=self.ts.get_test_metadata(),
                command=cmd,
                result={
                    "result": common.dataclass_to_dict(r),
                },
                bitrate_gbps=tftbase.Bitrate.NA,
            )

        return TaskOperation(
            log_name=self.log_name,
            thread_action=_thread_action,
        )
