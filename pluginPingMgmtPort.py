import ipaddress
import json

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

PING_COUNT = 5
PING_TIMEOUT_SEC = 2


def mgmt_port_ip_from_node_subnets(node_subnets_raw: str) -> str:
    node_subnets = json.loads(node_subnets_raw)
    subnet = node_subnets["default"][0]
    network = ipaddress.ip_network(subnet, strict=False)
    return str(network.network_address + 2)


class PluginPingMgmtPort(pluginbase.Plugin):
    PLUGIN_NAME = "ping_mgmt_port"

    def _enable(
        self,
        *,
        ts: TestSettings,
        perf_server: task.ServerTask,
        perf_client: task.ClientTask,
        tenant: bool,
    ) -> list[PluginTask]:
        return [
            TaskPingMgmtPort(ts, tenant),
        ]


plugin = pluginbase.register_plugin(PluginPingMgmtPort())


class TaskPingMgmtPort(PluginTask):
    @property
    def plugin(self) -> pluginbase.Plugin:
        return plugin

    def __init__(self, ts: TestSettings, tenant: bool):
        super().__init__(
            ts=ts,
            index=0,
            task_role=TaskRole.CLIENT,
            tenant=tenant,
        )

        self.pod_name = f"tools-pod-{self.node_location}-client-ping-mgmt-port"
        self.in_file_template = tftbase.get_manifest("tools-pod.yaml.j2")

    def initialize(self) -> None:
        super().initialize()
        self.render_pod_file("Plugin Pod Yaml")

    def _get_mgmt_port_ip(self) -> str:
        server_node = self.ts.node_server.name

        node_data = self.run_oc_get(f"node/{server_node}", namespace=None)
        if node_data is None:
            raise RuntimeError(f"Management port node {server_node!r} not found")

        annotations = node_data.get("metadata", {}).get("annotations", {})
        node_subnets_raw = annotations.get("k8s.ovn.org/node-subnets")
        if node_subnets_raw is None:
            raise RuntimeError(
                f'Node {server_node!r} has no "k8s.ovn.org/node-subnets" annotation'
            )

        try:
            return mgmt_port_ip_from_node_subnets(node_subnets_raw)
        except (json.JSONDecodeError, KeyError, IndexError, ValueError) as e:
            raise RuntimeError(
                f"Could not determine management port IP for node "
                f"{server_node!r} from node-subnets {node_subnets_raw!r}: {e}"
            ) from e

    def _create_task_operation(self) -> TaskOperation:
        def _thread_action() -> BaseOutput:
            self.ts.clmo_barrier.wait()

            try:
                mgmt_ip = self._get_mgmt_port_ip()
            except RuntimeError as e:
                logger.error(str(e))
                return PluginOutput(
                    success=False,
                    msg=str(e),
                    plugin_metadata=self.get_plugin_metadata(),
                    command="",
                    result={},
                )

            cmd = f"ping -c {PING_COUNT} -W {PING_TIMEOUT_SEC} {mgmt_ip}"
            r = self.run_oc_exec(cmd, may_fail=True)

            success = r.success
            msg: Optional[str] = None
            result: dict[str, Any] = {
                "mgmt_port_ip": mgmt_ip,
                "server_node": self.ts.node_server.name,
                "cmd": common.dataclass_to_dict(r),
            }

            if not success:
                msg = (
                    f"Ping to management port {mgmt_ip} on node "
                    f"{self.ts.node_server.name} failed: {r.err}"
                )

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
        mgmt_ip = result.result.get("mgmt_port_ip", "unknown")
        server = result.result.get("server_node", "unknown")
        logger.info(f"Ping to management port {mgmt_ip} on node {server} succeeded")
