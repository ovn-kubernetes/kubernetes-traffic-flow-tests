import time

from dataclasses import dataclass
from typing import Optional

from ktoolbox import common
from ktoolbox import host

import task
import tftbase

from task import ClientTask
from task import ServerTask
from task import TaskOperation
from testSettings import TestSettings
from testType import TestTypeHandler
from tftbase import BaseOutput
from tftbase import ConnectionMode
from tftbase import FlowTestOutput
from tftbase import TestType

logger = common.ExtendedLogger("tft." + __name__)

_INTERNET_DEFAULT_SERVER_STRING = "The document has moved"


@dataclass(frozen=True)
class TestTypeHandlerHttp(TestTypeHandler):
    def __init__(self) -> None:
        super().__init__(TestType.HTTP)

    def _create_server_client(self, ts: TestSettings) -> tuple[ServerTask, ClientTask]:
        s = HttpServer(ts=ts)
        c = HttpClient(ts=ts, server=s)
        return (s, c)


TestTypeHandler.register_test_type(TestTypeHandlerHttp())


class HttpServer(task.ServerTask):
    def cmd_line_args(self, *, for_template: bool = False) -> list[str]:
        return [
            "python3",
            "-m",
            "http.server",
            "-d",
            "/etc/kubernetes-traffic-flow-tests",
            f"{self.port}",
        ]

    def _create_setup_operation_get_cancel_action_cmd(self) -> str:
        return "killall python3"

    def _get_server_listen_protocol(self) -> Optional[str]:
        return "tcp"


class HttpClient(task.ClientTask):
    def _create_task_operation(self) -> TaskOperation:
        external_url = tftbase.get_tft_external_url()
        use_internet = (
            self.connection_mode == ConnectionMode.EXTERNAL_IP
            and external_url is not None
        )

        if use_internet:
            cmd = f"curl -s -m 30 {external_url}"
            expected_string = (
                tftbase.get_tft_external_server_string()
                or _INTERNET_DEFAULT_SERVER_STRING
            )

            def _check_success_internet(r: host.Result) -> bool:
                return expected_string in r.out

        else:
            server_ip = self.get_target_ip()
            target_port = self.get_target_port()
            cmd = f"curl --fail -s http://{server_ip}:{target_port}/data"

            def _check_success_podman(r: host.Result) -> bool:
                return r.success and r.match(
                    out="kubernetes-traffic-flow-tests\n",
                    err="",
                )

        def _thread_action() -> BaseOutput:
            self.ts.clmo_barrier.wait()

            if use_internet:
                _check_success = _check_success_internet
            else:
                _check_success = _check_success_podman

            sleep_time = 0.2
            end_timestamp = time.monotonic() + self.get_duration() - sleep_time

            while True:
                r = self.run_oc_exec(cmd)
                if not _check_success(r):
                    break
                if time.monotonic() >= end_timestamp:
                    break
                time.sleep(sleep_time)

            self.ts.event_client_finished.set()

            success = _check_success(r)
            if success:
                msg = ""
            elif use_internet:
                if expected_string not in r.out:
                    msg = f'Output of "{cmd}" does not contain expected string {repr(expected_string)}: {r.debug_msg()[:100]}'
                else:
                    msg = f'Output of "{cmd}" failed: {r.debug_msg()[:100]}'
            else:
                msg = f'Output of "{cmd}" failed: {r.debug_msg()[:100]}'

            return FlowTestOutput(
                success=success,
                msg=msg,
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
