import enum
import json
import logging
import os
import re
import shlex
import threading
import time
import typing
import yaml

from abc import ABC
from abc import abstractmethod
from collections.abc import Iterable
from collections.abc import Mapping
from threading import Thread
from typing import Any
from typing import Callable
from typing import Optional
from typing import TypeVar

from ktoolbox import common
from ktoolbox import host
from ktoolbox import kjinja2
from ktoolbox import netdev
from ktoolbox.k8sClient import K8sClient

import testConfig
import tftbase

from pluginbase import Plugin
from testSettings import TestSettings
from tftbase import BaseOutput
from tftbase import ClusterMode
from tftbase import ConnectionMode
from tftbase import PluginOutput
from tftbase import PodType
from tftbase import TaskRole
from tftbase import TestType

_j = json.dumps

logger = common.ExtendedLogger("tft." + __name__)


EXTERNAL_PERF_SERVER = "external-perf-server"

_SECONDARY_MODES = (
    ConnectionMode.MULTI_HOME,
    ConnectionMode.MNP_2ND_DENY,
    ConnectionMode.MNP_2ND_ALLOW,
)


NP_ACTION_ALLOW = "Allow"
NP_ACTION_DENY = "Deny"
NP_ACTION_PASS = "Pass"
ANP_DEFAULT_PRIORITY = 50


T = TypeVar("T")


class _OperationState(enum.Enum):
    NEW = (1,)
    STARTING = (2,)
    RUNNING = (3,)
    STOPPING = 4
    STOPPED = 5


def _detect_default_resource_name(
    client: K8sClient,
    namespace: str,
    secondary_network_nad: Optional[str],
) -> Optional[str]:
    if secondary_network_nad is not None:
        if "/" in secondary_network_nad:
            ns, nad = secondary_network_nad.split("/", 1)
        else:
            ns, nad = namespace, secondary_network_nad
        data = client.oc_get(
            f"network-attachment-definition/{nad}",
            namespace=ns,
        )
    else:
        data = None
    resource_name = None

    if data is not None:
        try:
            r = data["metadata"]["annotations"]["k8s.v1.cni.cncf.io/resourceName"]
            if isinstance(r, str) and r:
                resource_name = r
        except Exception:
            pass
    return resource_name


class TaskOperation:
    @typing.overload
    def __init__(
        self,
        *,
        log_name: str,
        thread_action: None = None,
        collect_action: Callable[[], BaseOutput],
        cancel_action: Optional[Callable[[], None]] = None,
        wait_ready: Optional[Callable[[], None]] = None,
    ) -> None:
        pass

    @typing.overload
    def __init__(
        self,
        *,
        log_name: str,
        thread_action: Callable[[], BaseOutput],
        collect_action: None = None,
        cancel_action: Optional[Callable[[], None]] = None,
        wait_ready: Optional[Callable[[], None]] = None,
    ) -> None:
        pass

    @typing.overload
    def __init__(
        self,
        *,
        log_name: str,
        thread_action: Callable[[], T],
        collect_action: Callable[[T], BaseOutput],
        cancel_action: Optional[Callable[[], None]] = None,
        wait_ready: Optional[Callable[[], None]] = None,
    ) -> None:
        pass

    def __init__(
        self,
        *,
        log_name: str,
        thread_action: Optional[Callable[[], Any]] = None,
        collect_action: Optional[
            Callable[[], BaseOutput] | Callable[[Any], BaseOutput]
        ] = None,
        cancel_action: Optional[Callable[[], None]] = None,
        wait_ready: Optional[Callable[[], None]] = None,
    ) -> None:
        if thread_action is None and collect_action is None:
            raise ValueError("either thread_action or collect_action must be provided")
        if cancel_action is not None and thread_action is None:
            raise ValueError("cannot set cancel_action without thread_action")
        super().__init__()
        self.log_name = log_name

        self._thread_action = thread_action
        self._collect_action = collect_action
        self._cancel_action = cancel_action
        self._wait_ready = wait_ready

        self._thread: Optional[Thread] = None

        self._intermediate_result: Any

        self._state = _OperationState.NEW
        self._lock = threading.Lock()

    def access_thread(self) -> Optional[Thread]:
        with self._lock:
            if self._thread is None:
                if self._thread_action is None:
                    return None
                self._thread = Thread(
                    target=self._run_thread_action,
                    name=self.log_name,
                )
                # this also starts the thread right away
                self._thread.start()
            return self._thread

    def _run_thread_action(self) -> None:
        assert not hasattr(self, "_intermediate_result")
        assert self._thread_action is not None
        logger.debug(f"thread[{self.log_name}]: call action")

        try:
            result = self._thread_action()
        except BaseException as e:
            import traceback

            logger.error(f"thread[{self.log_name}]: action raised exception {e}")
            logger.error(f"backtrace:\n{traceback.format_exc()}")
            os._exit(common.EX_SOFTWARE)

        with self._lock:
            assert not hasattr(self, "_intermediate_result")
            self._intermediate_result = result
        logger.debug(f"thread[{self.log_name}]: action completed ({result})")

    def start(self) -> None:
        with self._lock:
            assert self._state == _OperationState.NEW
            self._state = _OperationState.STARTING
        self.access_thread()
        self._start_wait_ready()

    def _start_wait_ready(self) -> None:
        with self._lock:
            if self._state.value > _OperationState.STARTING.value:
                return
            assert self._state == _OperationState.STARTING
        if self._wait_ready is not None:
            self._wait_ready()
        with self._lock:
            if self._state == _OperationState.STARTING:
                self._state = _OperationState.RUNNING

    def _cancel(self) -> None:
        if self._cancel_action is None:
            return
        logger.debug(f"thread[{self.log_name}]: cancel thread")
        self._cancel_action()
        logger.debug(f"thread[{self.log_name}]: cancel thread done")

    def finish(self, timeout: Optional[float] = None) -> BaseOutput:
        th = self.access_thread()
        if th is not None:
            with self._lock:
                assert self._state in (
                    _OperationState.STARTING,
                    _OperationState.RUNNING,
                )
                self._state = _OperationState.STOPPING
            t1 = timeout
            if self._cancel_action is not None:
                t1 = 0
            th.join(t1)
            if th.is_alive():
                # Abort and try to join again.
                if self._cancel_action is not None:
                    self._cancel()
                    th.join(timeout)
                if th.is_alive():
                    logger.error(
                        f"thread[{self.log_name}] did not terminate within the timeout {timeout}"
                    )
                    raise RuntimeError(
                        f"Thread {self.log_name} did not terminate within timeout {timeout}"
                    )

        result: Optional[BaseOutput] = None

        intermediate_result: Any

        with self._lock:
            if th is not None:
                assert self._state == _OperationState.STOPPING
                self._state = _OperationState.STOPPED
            else:
                assert self._state in (
                    _OperationState.STARTING,
                    _OperationState.RUNNING,
                )
                self._state = _OperationState.STOPPED

            if self._thread_action is None:
                intermediate_result = None
            elif not hasattr(self, "_intermediate_result"):
                # This really can only happen if we failed to join the thread.
                logger.error(f"thread[{self.log_name}] no result form thread received")
                result = BaseOutput(success=False, msg="failure to get thread result")
            else:
                intermediate_result = self._intermediate_result

        if result is not None:
            pass
        elif self._collect_action is None:
            assert self._thread_action
            result = intermediate_result
        elif self._thread_action:
            cb1 = typing.cast(Callable[[Any], BaseOutput], self._collect_action)
            result = cb1(intermediate_result)
        else:
            cb2 = typing.cast(Callable[[], BaseOutput], self._collect_action)
            result = cb2()

        assert isinstance(result, BaseOutput)

        logger.debug(f"thread[{self.log_name}]: got result {result}")
        return result


class Task(ABC):
    def __init__(
        self,
        *,
        ts: TestSettings,
        index: int,
        tenant: bool,
        task_role: TaskRole,
    ) -> None:
        self._lock = threading.Lock()
        self.task_role = task_role
        self.in_file_template = ""
        self.pod_name = ""
        self._setup_operation: Optional[TaskOperation] = None
        self._task_operation: Optional[TaskOperation] = None
        self._result: Optional[BaseOutput] = None
        self.lh = host.local
        self.index = index
        self.tenant = tenant
        self.ts = ts
        self.tc = ts.cfg_descr.tc
        self._resource_name: Optional[str] = None

        if not self.tenant and self.tc.mode == ClusterMode.SINGLE:
            raise ValueError("Cannot have non-tenant Task when cluster mode is single")

    @property
    def log_name(self) -> str:
        return self.__class__.__name__

    @property
    def log_name_setup(self) -> str:
        return f"{self.log_name}.setup"

    @property
    def node(self) -> testConfig.ConfNodeBase:
        if self.task_role == TaskRole.CLIENT:
            return self.ts.node_client
        if self.task_role == TaskRole.SERVER:
            return self.ts.node_server
        raise ValueError()

    @property
    def node_name(self) -> str:
        return self.node.name

    @property
    def node_location(self) -> str:
        return "same-node" if self.ts.test_case_id.info.is_same_node else "diff-node"

    def get_namespace(self) -> str:
        ns = self.ts.cfg_descr.get_tft().namespace
        if self.ts.test_case_id.is_udn:
            return f"{ns}-udn"
        return ns

    def get_duration(self) -> int:
        conn_duration = self.ts.cfg_descr.get_connection().duration
        if conn_duration is not None:
            return conn_duration
        return self.ts.cfg_descr.get_tft().duration

    @property
    def out_file_yaml(self) -> str:
        if not self.in_file_template or not self.pod_name:
            raise RuntimeError("task cannot generate a pod yaml")
        return tftbase.get_manifest_renderpath(self.pod_name + ".yaml")

    def _get_node_secondary_network_nad(self) -> Optional[str]:
        if self.task_role == TaskRole.CLIENT and self.ts.test_case_id.info.is_same_node:
            (c_client,) = self.ts.connection.client
            if c_client.secondary_network_nad is not None:
                return c_client.secondary_network_nad
        return self.node.secondary_network_nad

    def get_resource_name(self) -> str:
        with self._lock:
            if self._resource_name is not None:
                return self._resource_name
        resource_name_config = self.ts.connection.resource_name
        nad_for_detection = (
            self._get_node_secondary_network_nad()
            or self.ts.connection.secondary_network_nad
        )
        resource_name = (
            resource_name_config
            or _detect_default_resource_name(
                self.client,
                self.get_namespace(),
                nad_for_detection,
            )
            or ""
        )
        with self._lock:
            if self._resource_name is None:
                if not resource_name:
                    logger.debug("resource_name: unset and undetected")
                elif resource_name_config:
                    logger.debug(f"resource_name: from config as {repr(resource_name)}")
                else:
                    logger.info(f"resource_name: autodetected as {repr(resource_name)}")
                self._resource_name = resource_name
            else:
                resource_name = self._resource_name
        return resource_name

    def _get_template_args_test_image(self) -> str:
        """Get the test image, auto-selecting RDMA image for ib-* test types."""
        return tftbase.get_tft_test_image_for_type(self.ts.connection.test_type)

    def _get_effective_secondary_network_nad(self) -> str:
        if self.ts.test_case_id.is_udn:
            return f"{self.get_namespace()}/tft-secondary"
        nad = self._get_node_secondary_network_nad()
        if nad is not None:
            if "/" not in nad:
                nad = f"{self.get_namespace()}/{nad}"
            return nad
        return (
            self.node.effective_secondary_network_nad
            or self.ts.connection.effective_secondary_network_nad
        )

    def get_template_args(self) -> dict[str, str | list[str] | bool]:
        resource_name = self.get_resource_name()
        conn = self.ts.connection
        has_resources = any(
            v is not None
            for v in (
                conn.cpu_request,
                conn.cpu_limit,
                conn.mem_request,
                conn.mem_limit,
            )
        )
        return {
            "name_space": _j(self.get_namespace()),
            "test_image": _j(self._get_template_args_test_image()),
            "image_pull_policy": _j(tftbase.get_tft_image_pull_policy()),
            "command": _j(["/usr/bin/container-entry-point.sh"]),
            "args": _j(self._get_template_args_args()),
            "label_tft_tests": _j(f"{self.index}"),
            "node_name": _j(self.node_name),
            "pod_name": _j(self.pod_name),
            "privileged_pod": _j(self._get_template_args_privileged_pod()),
            "capabilities_pod": _j(self._get_template_args_capabilities_pod()),
            "port": self._get_template_args_port(),
            "secondary_network_nad": _j(self._get_effective_secondary_network_nad()),
            "use_secondary_network": (
                bool(self._get_node_secondary_network_nad())
                or bool(self.ts.connection.secondary_network_nad)
                or self.ts.test_case_id.is_udn_secondary
            ),
            "has_resource_name": bool(resource_name),
            "resource_name": _j(resource_name),
            "default_network": _j(self.node.default_network),
            "has_resources": has_resources,
            "cpu_request": conn.cpu_request or "",
            "cpu_limit": conn.cpu_limit or "",
            "mem_request": conn.mem_request or "",
            "mem_limit": conn.mem_limit or "",
        }

    def _get_template_args_privileged_pod(self) -> bool:
        v = self.node.privileged_pod
        if v is not None:
            # First, the per-node setting has precedence.
            return v
        v = tftbase.get_tft_privileged_pod()
        if v is not None:
            # Second, the setting from the environment variable is honored.
            return v
        # Finally, take the test-wide setting from the configuration file.
        return self.ts.cfg_descr.get_tft().privileged_pod

    def _get_template_args_capabilities_pod(self) -> Mapping[str, tuple[str, ...]]:
        # Per-node setting has precedence over test-wide setting.
        if self.node.capabilities_pod is not None:
            return self.node.capabilities_pod
        return self.ts.cfg_descr.get_tft().capabilities_pod

    def _get_template_args_port(self) -> str:
        return ""

    def _get_template_args_args(self) -> list[str]:
        return []

    @property
    def _svc_backend_label(self) -> str:
        return ""

    @property
    def _network_type(self) -> str:
        return "secondary" if self.ts.connection_mode in _SECONDARY_MODES else "primary"

    def render_pod_file(self, log_info: str) -> None:
        self.render_file(
            log_info,
            self.in_file_template,
            self.out_file_yaml,
        )

    def render_file(
        self,
        log_info: str,
        in_file_template: str,
        out_file_yaml: str,
        template_args: Optional[dict[str, str | list[str] | bool]] = None,
    ) -> None:
        if template_args is None:
            template_args = self.get_template_args()
        logger.info(
            f'Generate {log_info} "{out_file_yaml}" (from "{in_file_template}", for {self.log_name})'
        )

        rendered = kjinja2.render_file(
            in_file_template,
            template_args,
            out_file=out_file_yaml,
        )

        try:
            rendered_dict = yaml.safe_load(rendered)
        except Exception as e:
            logger.error(
                f'"{in_file_template}" rendered as {repr(rendered)} is not valid YAML: {e}'
            )
            raise

        logger.debug(f'"{in_file_template}" contains: {json.dumps(rendered_dict)}')

    def initialize(self) -> None:
        pass

    @property
    def client(self) -> K8sClient:
        return self.tc.client(tenant=self.tenant)

    def _get_run_oc_namespace(
        self,
        namespace: Optional[str] | common._MISSING_TYPE = common.MISSING,
    ) -> Optional[str]:
        if isinstance(namespace, common._MISSING_TYPE):
            # By default, set use self.get_namespace(). You can select another
            # namespace or no namespace (by setting to None).
            namespace = self.get_namespace()
        return namespace

    def run_oc(
        self,
        cmd: str | Iterable[str],
        *,
        may_fail: bool = False,
        die_on_error: bool = False,
        check_success: Optional[Callable[[host.Result], bool]] = None,
        namespace: Optional[str] | common._MISSING_TYPE = common.MISSING,
    ) -> host.Result:
        return self.client.oc(
            cmd,
            may_fail=may_fail,
            die_on_error=die_on_error,
            check_success=check_success,
            namespace=self._get_run_oc_namespace(namespace),
        )

    def run_oc_exec(
        self,
        cmd: str | Iterable[str],
        *,
        may_fail: bool = False,
        die_on_error: bool = False,
        pod_name: Optional[str] = None,
        namespace: Optional[str] | common._MISSING_TYPE = common.MISSING,
    ) -> host.Result:
        if pod_name is None:
            pod_name = self.pod_name
        return self.client.oc_exec(
            cmd,
            pod_name=pod_name,
            may_fail=may_fail,
            die_on_error=die_on_error,
            namespace=self._get_run_oc_namespace(namespace),
        )

    def run_oc_get(
        self,
        what: str,
        *,
        may_fail: bool = False,
        die_on_error: bool = False,
        namespace: Optional[str] | common._MISSING_TYPE = common.MISSING,
    ) -> typing.Optional[dict[str, typing.Any]]:
        return self.client.oc_get(
            what,
            may_fail=may_fail,
            die_on_error=die_on_error,
            namespace=self._get_run_oc_namespace(namespace),
        )

    def get_pod_ip(self) -> str:
        y = self.run_oc_get(f"pod/{self.pod_name}", die_on_error=True)
        pod_ip = None
        try:
            if y:
                pod_ip = y["status"]["podIP"]
                if self.ts.test_case_id.is_udn_primary:
                    pod_networks_str = y["metadata"]["annotations"].get(
                        "k8s.ovn.org/pod-networks", ""
                    )
                    if pod_networks_str:
                        pod_networks = json.loads(pod_networks_str)
                        for net_info in pod_networks.values():
                            if net_info.get("role") == "primary":
                                ip_with_cidr = net_info["ip_address"]
                                pod_ip = ip_with_cidr.split("/")[0]
                                break
                    if pod_ip is None:
                        logger.warning(
                            f"Could not find UDN primary IP for pod {self.pod_name} "
                            f"in k8s.ovn.org/pod-networks annotation, "
                            f"falling back to status.podIP"
                        )
                        pod_ip = y["status"]["podIP"]
                elif (
                    self._get_node_secondary_network_nad()
                    or self.ts.connection.secondary_network_nad
                ):
                    network_status_str = y["metadata"]["annotations"][
                        "k8s.v1.cni.cncf.io/network-status"
                    ]
                    network_status = json.loads(network_status_str)

                    nad = self._get_effective_secondary_network_nad()
                    for network in network_status:
                        if network["name"] == nad:
                            pod_ip = network["ips"][0]
                            break
        except Exception as e:
            logger.error(f"Error retrieving pod IP for {self.pod_name}: {e}")
        if not isinstance(pod_ip, str):
            raise RuntimeError(f"Failure to get pod IP for {self.pod_name}")
        return pod_ip

    def get_primary_ip(self) -> str:
        y = self.run_oc_get(f"pod/{self.pod_name}", die_on_error=True)
        pod_ip = None
        if y:
            pod_ip = y["status"]["podIP"]
        if not isinstance(pod_ip, str):
            raise RuntimeError(f"Failure to get podIP for {self.pod_name}")
        return pod_ip

    def get_secondary_ip(self) -> str:
        jsonpath = "{.metadata.annotations.k8s\\.ovn\\.org\\/pod-networks}"
        r = self.run_oc(
            f"get pod {self.pod_name} -o jsonpath='{jsonpath}'", die_on_error=True
        )

        y = yaml.safe_load(r.out)

        if self.ts.test_case_id.is_udn:
            if not isinstance(y, dict):
                raise RuntimeError(
                    f"k8s.ovn.org/pod-networks annotation for pod {self.pod_name} "
                    f"is not a valid dict (got {type(y).__name__})"
                )
            for net_info in y.values():
                if net_info.get("role") == "secondary":
                    ip_address_with_cidr = typing.cast(str, net_info["ip_address"])
                    ip_address = ip_address_with_cidr.split("/")[0]
                    logger.info(f"Secondary IP: {ip_address}")
                    return ip_address
            raise RuntimeError(
                f"Could not find UDN secondary IP for pod {self.pod_name} "
                f"in k8s.ovn.org/pod-networks annotation"
            )

        nad = self._get_effective_secondary_network_nad()
        ip_address_with_cidr = typing.cast(str, y[nad]["ip_address"])
        ip_address = ip_address_with_cidr.split("/")[0] if ip_address_with_cidr else ""
        logger.info(f"Secondary IP: {ip_address}")
        return ip_address

    def get_nad_cidrs(self, nad: str) -> list[str]:
        # Matches IPv4 and IPv6 CIDRs
        cidr_re = re.compile(
            r"^(\d{1,3}(?:\.\d{1,3}){3}/\d{1,3})(?:/\d+)?$"  # IPv4
            r"|"
            r"^([0-9a-fA-F:]{2,}/\d{1,3})(?:/\d+)?$"  # IPv6
        )

        def collect(val: object, out: list[str]) -> None:
            if isinstance(val, str):
                for token in re.split(r"[,\s]+", val.strip()):
                    token = token.strip()
                    if not token:
                        continue
                    m = cidr_re.match(token)
                    if m:
                        out.append(m.group(1) or m.group(2))
            elif isinstance(val, list):
                for item in val:
                    collect(item, out)
            elif isinstance(val, dict):
                for v in val.values():
                    collect(v, out)

        if "/" in nad:
            namespace, name = nad.split("/", 1)
        else:
            namespace = self.get_namespace()
            name = nad
        nad_data = self.run_oc_get(
            f"network-attachment-definition/{name}",
            die_on_error=True,
            namespace=namespace,
        )
        assert nad_data is not None
        config = json.loads(nad_data["spec"]["config"])
        cidrs: list[str] = []
        collect(config, cidrs)
        return cidrs

    def create_cluster_ip_service(self) -> None:
        in_file_template = tftbase.get_manifest("svc-cluster-ip.yaml.j2")
        out_file_yaml = tftbase.get_manifest_renderpath(
            f"svc-cluster-ip-{self._svc_backend_label}.yaml"
        )

        template_args = {
            **self.get_template_args(),
            "svc_label": self._svc_backend_label,
            "network_type": self._network_type,
        }
        self.render_file(
            "Cluster IP Service", in_file_template, out_file_yaml, template_args
        )
        self.run_oc(
            f"apply -f {out_file_yaml}",
            check_success=lambda r: r.success or "already exists" in r.err,
            die_on_error=True,
        )

    def get_cluster_ip(self) -> Optional[str]:
        svc_name = f"tft-clusterip-service-{self._svc_backend_label}"
        r = self.run_oc(
            f"get service {svc_name} -o=jsonpath='{{.spec.clusterIP}}'",
            may_fail=True,
        )
        return r.out if r.success else None

    def create_node_port_service(self) -> None:
        in_file_template = tftbase.get_manifest("svc-node-port.yaml.j2")
        out_file_yaml = tftbase.get_manifest_renderpath(
            f"svc-node-port-{self._svc_backend_label}.yaml"
        )

        template_args = {
            **self.get_template_args(),
            "svc_label": self._svc_backend_label,
            "network_type": self._network_type,
        }

        self.render_file(
            "Node Port Service", in_file_template, out_file_yaml, template_args
        )
        self.run_oc(
            f"apply -f {out_file_yaml}",
            check_success=lambda r: r.success or "already exists" in r.err,
            die_on_error=True,
        )

    def get_nodeport_ip(self) -> Optional[str]:
        svc_name = f"tft-nodeport-service-{self._svc_backend_label}"
        r = self.run_oc(
            f"get service {svc_name} -o=jsonpath='{{.spec.clusterIP}}'",
            may_fail=True,
        )
        return r.out if r.success else None

    def create_load_balancer_service(self) -> None:
        in_file_template = tftbase.get_manifest("svc-loadbalancer.yaml.j2")
        out_file_yaml = tftbase.get_manifest_renderpath(
            f"svc-loadbalancer-{self._svc_backend_label}.yaml"
        )

        template_args = {
            **self.get_template_args(),
            "svc_label": self._svc_backend_label,
            "network_type": self._network_type,
        }
        self.render_file(
            "LoadBalancer Service",
            in_file_template,
            out_file_yaml,
            template_args,
        )
        self.run_oc(
            f"apply -f {out_file_yaml}",
            check_success=lambda r: r.success or "already exists" in r.err,
            die_on_error=True,
        )

    def get_load_balancer_ip(self) -> Optional[str]:
        svc_name = f"tft-loadbalancer-service-{self._svc_backend_label}"
        r = self.run_oc(
            f"get service {svc_name} -o=jsonpath='{{.status.loadBalancer.ingress[0].ip}}'",
            may_fail=True,
        )
        return r.out if r.success and r.out else None

    def wait_load_balancer_ip(self, timeout: int = 120) -> str:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            ip = self.get_load_balancer_ip()
            if ip:
                logger.debug(f"LoadBalancer IP assigned: {ip}")
                time.sleep(10)
                return ip
            logger.debug("Waiting for LoadBalancer IP to be assigned...")
            time.sleep(5)
        raise RuntimeError(
            f"LoadBalancer IP not assigned within {timeout}s "
            f"for service tft-loadbalancer-service-{self._svc_backend_label}"
        )

    def _create_multi_network_policy(self, action: str, port: int) -> str:
        action = action.lower()
        mnp_name = f"{action}-mnp"
        in_file_template = tftbase.get_manifest("multi-network-policy.yaml.j2")
        out_file_yaml = tftbase.get_manifest_renderpath(
            f"mnp-{self.pod_name}-{action}.yaml"
        )

        client_cidrs: list[str] = []
        if action == NP_ACTION_ALLOW.lower():
            client_nad = (
                self.ts.connection.client[0].effective_secondary_network_nad
                or self.ts.connection.effective_secondary_network_nad
            )
            client_cidrs = self.get_nad_cidrs(client_nad)

        template_args: dict[str, typing.Any] = {
            **self.get_template_args(),
            "mnp_name": _j(mnp_name),
            "mnp_action": action,
            "mnp_port": _j(port),
            "mnp_client_cidrs": client_cidrs,
        }

        self.render_file(
            f"{action.title()} Multi Network Policy",
            in_file_template,
            out_file_yaml,
            template_args,
        )
        self.run_oc(
            f"apply -f {out_file_yaml}",
            check_success=lambda r: r.success or "already exists" in r.err,
            die_on_error=True,
        )
        return self.run_oc(
            f"get multi-networkpolicies {mnp_name}",
            die_on_error=True,
        ).out

    def create_admin_network_policy(self, action: str, priority: int, port: int) -> str:
        anp_name = f"tft-anp-{self.index}"
        in_file_template = tftbase.get_manifest("admin-network-policy.yaml.j2")
        out_file_yaml = tftbase.get_manifest_renderpath(f"anp-{self.pod_name}.yaml")

        template_args = {
            **self.get_template_args(),
            "anp_action": action,
            "anp_priority": str(priority),
            "anp_port": str(port),
            "anp_name": anp_name,
        }

        self.render_file(
            f"{action.title()} Admin Network Policy",
            in_file_template,
            out_file_yaml,
            template_args,
        )
        self.run_oc(
            f"apply -f {out_file_yaml}",
            namespace=None,  # ANP is cluster-scoped
            check_success=lambda r: r.success or "already exists" in r.err,
            die_on_error=True,
        )
        return self.run_oc(
            f"get adminnetworkpolicies {anp_name}",
            namespace=None,
            die_on_error=True,
        ).out

    def create_network_policy(self, port: int, action: str = NP_ACTION_DENY) -> str:
        action = action.lower()
        np_name = f"tft-np-{action}-{self.index}"
        in_file_template = tftbase.get_manifest("network-policy.yaml.j2")
        out_file_yaml = tftbase.get_manifest_renderpath(
            f"np-{self.pod_name}-{action}.yaml"
        )

        template_args = {
            **self.get_template_args(),
            "np_name": np_name,
            "np_action": action,
            "np_port": _j(port),
        }

        self.render_file(
            f"{action.title()} Network Policy",
            in_file_template,
            out_file_yaml,
            template_args,
        )
        self.run_oc(
            f"apply -f {out_file_yaml}",
            check_success=lambda r: r.success or "already exists" in r.err,
            die_on_error=True,
        )
        return self.run_oc(
            f"get networkpolicies {np_name}",
            die_on_error=True,
        ).out

    def start_setup(
        self, *, skip_pod_setup: bool = False, provisioning: bool = False
    ) -> None:
        assert self._setup_operation is None
        self._setup_operation = self._create_setup_operation(
            skip_pod_setup=skip_pod_setup, provisioning=provisioning
        )
        if self._setup_operation is not None:
            self._setup_operation.start()

    def _create_setup_operation(
        self, *, skip_pod_setup: bool = False, provisioning: bool = False
    ) -> Optional[TaskOperation]:
        if not skip_pod_setup:
            self.setup_pod()
        return None

    def finish_setup(self) -> None:
        if self._setup_operation is None:
            return
        to = self._setup_operation
        self._setup_operation = None
        to.finish(timeout=5)

    def setup_pod(self) -> None:
        # Check if pod already exists
        v = self.run_oc_get(f"pod/{self.pod_name}", may_fail=True)
        if v is None:
            logger.info(f"Creating Pod {self.pod_name}.")
            self.run_oc(f"apply -f {self.out_file_yaml}", die_on_error=True)
        else:
            logger.info(f"Pod {self.pod_name} already exists.")

        logger.info(f"Waiting for Pod {self.pod_name} to become ready.")
        self.run_oc(
            f"wait --for=condition=ready pod/{self.pod_name} --timeout=10m",
            die_on_error=True,
        )

    def start_task(self) -> None:
        assert self._task_operation is None
        self._task_operation = self._create_task_operation()
        if self._task_operation:
            self._task_operation.start()

    def _create_task_operation(self) -> Optional[TaskOperation]:
        return None

    def finish_task(self) -> None:
        if self._task_operation is None:
            return
        assert self._result is None
        logger.info(f"Completing execution on {self.log_name}")
        self._result = self._task_operation.finish(timeout=self.get_duration() * 1.5)

    def aggregate_output(self, tft_result_builder: tftbase.TftResultBuilder) -> None:
        if self._result is None:
            return
        if not isinstance(self._result, tftbase.AggregatableOutput):
            # This output has nothing to collect. We are done.
            return

        result = self._result

        if isinstance(result, tftbase.FlowTestOutput):
            tft_result_builder.set_flow_test(result)
            if result.success:
                log_level = logging.INFO
                log_msg = "success"
            else:
                log_level = logging.ERROR
                log_msg = "failure"
            logger.log(log_level, f"Results of {self.ts.get_test_str()}: {log_msg}")
            logger.debug(f"result: {common.dataclass_to_json(result)}")
        elif isinstance(result, PluginOutput):
            tft_result_builder.add_plugin(result)

        if not result.success:
            logger.warn(f"Result of {type(self).__name__} failed: {result.eval_msg}")
            logger.debug(
                f"Failure details: pod={self.pod_name}, "
                f"port={getattr(self, 'port', 'N/A')}, "
                f"node={self.node_name}, "
                f"test_type={getattr(self, 'test_type', 'N/A')}, "
                f"connection_mode="
                f"{getattr(self, 'connection_mode', 'N/A')}"
            )
        else:
            self._aggregate_output_log_success(result)

    def _aggregate_output_log_success(
        self,
        result: tftbase.AggregatableOutput,
    ) -> None:
        logger.info(f"Result of {type(self).__name__} is success")

    def pod_get_device_infos(
        self,
        pod_name: str,
        *,
        ifname: Optional[str] = None,
        pciaddr: Optional[str] = None,
        vf_rep_for_pciaddr: Optional[str] = None,
    ) -> Optional[list[dict[str, Any]]]:
        r = self.run_oc_exec(
            "ktoolbox-netdev get_device_infos",
            pod_name=pod_name,
        )

        if not r.success:
            return None

        return netdev.device_infos_parse_lst(
            r.out,
            ifname=ifname,
            pciaddr=pciaddr,
            vf_rep_for_pciaddr=vf_rep_for_pciaddr,
        )

    def pod_get_vf_rep(
        self,
        *,
        pod_name: str,
        ifname: str,
        host_pod_name: str,
    ) -> Optional[str]:

        lst_1 = self.pod_get_device_infos(pod_name=pod_name, ifname=ifname)
        pciaddr_1: Optional[str] = None
        if lst_1:
            dev_info_1 = common.iter_get_first(lst_1, unique=True)
            if dev_info_1 is not None:
                pciaddr_1 = dev_info_1.get("pciaddr")

        if pciaddr_1 is None:
            return None

        if logger.isEnabledFor(logging.DEBUG):
            # Only call the command, to have the podSandboxId in the debug logs. Then
            # It's useful to compare with the VR_REP, which was related in 4.14 (but no
            # longer in 4.15+).
            self.run_oc_exec(
                f"chroot /host crictl ps -a --name={shlex.quote(pod_name)} -o json",
                pod_name=host_pod_name,
            )

        lst_2 = self.pod_get_device_infos(
            pod_name=host_pod_name,
            vf_rep_for_pciaddr=pciaddr_1,
        )
        if lst_2:
            dev_info_2 = common.iter_get_first(lst_2, unique=True)
            if dev_info_2:
                return dev_info_2.get("ifname")

        return None


class ServerTask(Task, ABC):
    def __init__(self, ts: TestSettings):
        super().__init__(
            ts=ts,
            index=ts.server_index,
            tenant=ts.server_is_tenant,
            task_role=TaskRole.SERVER,
        )

        connection_mode = ts.connection_mode
        pod_type = ts.server_pod_type
        server_conf = ts.cfg_descr.get_server()
        port_base = (
            server_conf.host_port
            if pod_type == PodType.HOSTBACKED
            else server_conf.pod_port
        )
        port = port_base + self.index

        use_internet = (
            connection_mode == ConnectionMode.EXTERNAL_IP
            and tftbase.get_tft_external_url() is not None
            and ts.connection.test_type == TestType.HTTP
        )

        if use_internet:
            in_file_template = ""
            pod_name = ""
        elif connection_mode == ConnectionMode.EXTERNAL_IP:
            in_file_template = ""
            pod_name = EXTERNAL_PERF_SERVER
        elif connection_mode in (
            ConnectionMode.MULTI_HOME,
            ConnectionMode.MNP_2ND_DENY,
            ConnectionMode.MNP_2ND_ALLOW,
            ConnectionMode.MNP_PRIMARY_DENY,
        ):
            in_file_template = "pod-secondary-network.yaml.j2"
            pod_name = f"normal-pod-secondary-server-{port}"
        elif pod_type == PodType.SRIOV:
            in_file_template = "sriov-pod.yaml.j2"
            pod_name = f"sriov-pod-server-{port}"
        elif pod_type == PodType.NORMAL:
            in_file_template = "pod.yaml.j2"
            pod_name = f"normal-pod-server-{port}"
        elif pod_type == PodType.HOSTBACKED:
            in_file_template = "host-pod.yaml.j2"
            pod_name = f"host-pod-server-{port}"
        else:
            raise ValueError("Invalid pod_type {pod_type}")

        if in_file_template != "":
            in_file_template = tftbase.get_manifest(in_file_template)

        self.exec_persistent = ts.node_server.is_persistent_server
        self.pre_provisioning: bool = False
        self.port = port
        # external_port is discovered dynamically for EXTERNAL_IP mode
        self.external_port: int = 0
        self.pod_type = pod_type
        self.connection_mode = ts.connection_mode
        self.use_internet = use_internet
        self.in_file_template = in_file_template
        self.pod_name = pod_name

    def _get_template_args_port(self) -> str:
        return str(self.port)

    @property
    def _svc_backend_label(self) -> str:
        backend = "host" if self.pod_type == PodType.HOSTBACKED else "pod"
        prefix = (
            f"{self._network_type}-{backend}"
            if self._network_type == "secondary"
            else backend
        )
        return f"{prefix}-{self.port}"

    def ensure_services(self) -> None:
        if self.in_file_template != "":
            if self.get_cluster_ip() is None:
                self.create_cluster_ip_service()
            if self.get_nodeport_ip() is None:
                self.create_node_port_service()

    def initialize(self) -> None:
        super().initialize()

        if self.in_file_template != "":
            self.render_pod_file("Server Pod Yaml")
        self.ensure_services()

    def _get_template_args_args(self) -> list[str]:
        if not self.exec_persistent:
            return []
        return self.cmd_line_args(for_template=True)

    def confirm_server_alive(self) -> None:
        if self.use_internet:
            # No server to wait for; client curls external URL directly.
            self.ts.event_server_alive.set()
            return

        if self.connection_mode == ConnectionMode.EXTERNAL_IP:
            # Podman scenario
            end_time = time.monotonic() + 60
            while time.monotonic() < end_time:
                r = self.lh.run(
                    f"podman ps --filter status=running --filter name={self.pod_name} --format '{{{{.Names}}}}'"
                )
                if self.pod_name in r.out:
                    break
                time.sleep(5)
        else:
            # Kubernetes/OpenShift scenario
            r = self.run_oc(
                f"wait --for=condition=ready pod/{self.pod_name} --timeout=1m"
            )
        if not r:
            logger.error(f"Failed to start server {self.pod_name}: {r.err}")
            logger.debug(
                f"Server startup failure details: pod={self.pod_name}, "
                f"port={self.port}, node={self.node_name}, "
                f"connection_mode={self.connection_mode.name}"
            )
            raise RuntimeError(f"Failed to start server {self.pod_name}: {r.err}")

        if not self.pre_provisioning:
            self._wait_for_server_listening()
        self.ts.event_server_alive.set()

    # Override in subclasses to specify protocol (tcp/udp) to check.
    def _get_server_listen_protocol(self) -> Optional[str]:
        return None

    def _discover_podman_port(self) -> int:
        """Query podman to find the auto-assigned host port for EXTERNAL_IP mode."""
        # Use podman inspect with JSON output for reliable parsing
        r = self.lh.run(
            f"podman inspect --format '{{{{json .NetworkSettings.Ports}}}}' {self.pod_name}",
            log_level_fail=logging.DEBUG,
        )
        if r.success and r.out.strip():
            try:
                # Output format: {"5201/tcp":[{"HostIp":"0.0.0.0","HostPort":"12345"}]}
                ports = json.loads(r.out.strip())
                port_key = f"{self.port}/tcp"
                if port_key in ports and ports[port_key]:
                    return int(ports[port_key][0]["HostPort"])
            except (json.JSONDecodeError, KeyError, IndexError, ValueError):
                pass
        return 0

    def _wait_for_server_listening(self) -> None:
        protocol = self._get_server_listen_protocol()
        if protocol is None:
            return

        max_wait_time = 60
        start_time = time.monotonic()
        protocol_flag = "-t" if protocol == "tcp" else "-u"
        is_external = self.connection_mode == ConnectionMode.EXTERNAL_IP

        logger.info(f"Waiting for server to be ready (max {max_wait_time}s)")

        while time.monotonic() - start_time < max_wait_time:

            # For EXTERNAL_IP, first discover the port if not yet known
            if is_external and self.external_port == 0:
                self.external_port = self._discover_podman_port()
                if self.external_port > 0:
                    logger.info(f"Discovered external port: {self.external_port}")

            # Determine which port to check
            check_port = self.external_port if is_external else self.port

            if check_port == 0:
                continue

            # Check if server is listening
            check_cmd = f"ss -ln {protocol_flag} | grep ':{check_port}'"
            if is_external:
                r = self.lh.run(check_cmd)
            else:
                r = self.run_oc_exec(check_cmd, may_fail=True)

            if r.success and r.out.strip():
                logger.info(
                    f"Server is now listening on port {check_port} "
                    f"(took {time.monotonic() - start_time:.2f}s)"
                )
                return

            time.sleep(0.5)

        # Timeout - determine appropriate error message
        if is_external and self.external_port == 0:
            error_msg = f"Failed to discover podman port within {max_wait_time}s"
        else:
            check_port = self.external_port if is_external else self.port
            error_msg = f"Server failed to start listening on port {check_port} within {max_wait_time}s"
        logger.error(error_msg)
        logger.debug(
            f"Server listen timeout: pod={self.pod_name}, "
            f"port={self.port}, node={self.node_name}, "
            f"connection_mode={self.connection_mode.name}"
        )
        raise RuntimeError(error_msg)

    @abstractmethod
    def cmd_line_args(self, *, for_template: bool = False) -> list[str]:
        raise RuntimeError()

    def _create_setup_operation_get_thread_action_cmd(self) -> str:
        return shlex.join(self.cmd_line_args())

    @abstractmethod
    def _create_setup_operation_get_cancel_action_cmd(self) -> str:
        pass

    def _create_setup_operation(
        self, *, skip_pod_setup: bool = False, provisioning: bool = False
    ) -> Optional[TaskOperation]:
        # We don't chain up super()._create_setup_operation(). Depending on
        # the connection_mode we call setup_pod().

        self.pre_provisioning = provisioning

        if self.use_internet:
            # No server needed; client curls external URL directly.
            self.ts.event_server_alive.set()
            return None

        th_cmd = self._create_setup_operation_get_thread_action_cmd()

        if self.connection_mode == ConnectionMode.EXTERNAL_IP:
            pull_policy = ""
            if tftbase.get_tft_image_pull_policy() == "Always":
                pull_policy = " --pull=always"

            # Use -p {port} to let podman auto-assign a free host port
            test_image = tftbase.get_tft_test_image_for_type(
                self.ts.connection.test_type
            )
            cmd = f"podman run -it --replace --rm -p {self.port} --name={self.pod_name}{pull_policy} {test_image} {th_cmd}"
            cancel_cmd = f"podman rm --force {self.pod_name}"
        else:
            if not skip_pod_setup:
                self.setup_pod()
            ca_cmd = self._create_setup_operation_get_cancel_action_cmd()
            cmd = f"{th_cmd}"
            cancel_cmd = f"{ca_cmd}"

        if not provisioning:
            if self.connection_mode in (
                ConnectionMode.MNP_2ND_DENY,
                ConnectionMode.MNP_PRIMARY_DENY,
            ):
                self._create_multi_network_policy(NP_ACTION_DENY, self.port)
            elif self.connection_mode == ConnectionMode.MNP_2ND_ALLOW:
                self._create_multi_network_policy(NP_ACTION_ALLOW, self.port)
            elif self.connection_mode == ConnectionMode.ANP_ALLOW:
                self.create_admin_network_policy(
                    NP_ACTION_ALLOW, ANP_DEFAULT_PRIORITY, self.port
                )
            elif self.connection_mode == ConnectionMode.ANP_DENY:
                self.create_admin_network_policy(
                    NP_ACTION_DENY, ANP_DEFAULT_PRIORITY, self.port
                )
            elif self.connection_mode == ConnectionMode.ANP_PASS_NP_DENY:
                self.create_admin_network_policy(
                    NP_ACTION_PASS, ANP_DEFAULT_PRIORITY, self.port
                )
                self.create_network_policy(self.port, NP_ACTION_DENY)
            elif self.connection_mode == ConnectionMode.NP_DENY:
                self.create_network_policy(self.port, NP_ACTION_DENY)
            elif self.connection_mode == ConnectionMode.NP_ALLOW:
                self.create_network_policy(self.port, NP_ACTION_ALLOW)

            if self.connection_mode == ConnectionMode.LOAD_BALANCER:
                self.create_load_balancer_service()

        def _run_cmd(cmd: str) -> BaseOutput:
            # We ignore the exit code of the command, that is because this is
            # commonly a long running process that needs to get killed with the
            # "cancel_action".  In that case, the exit code will be non-zero, but
            # it's the normal termination of the command. Suppress such "failures".
            #
            # Or, it's the _cancel_action(), in which case we also ignore failures.
            may_fail = True

            if self.connection_mode == ConnectionMode.EXTERNAL_IP:
                logger.info(f"Running {cmd}")
                res = self.lh.run(
                    cmd,
                    log_level_fail=logging.DEBUG if may_fail else logging.ERROR,
                )
            elif self.exec_persistent:
                return BaseOutput(msg="Server is persistent")
            elif self.pre_provisioning:
                return BaseOutput(msg="Pre-provisioning")
            else:
                logger.info(f"Running {cmd}")
                res = self.run_oc_exec(cmd, may_fail=may_fail)
                if not res.success:
                    logger.debug(
                        f"oc exec '{cmd}' exited {res.returncode}"
                        + (f": {res.err.strip()}" if res.err.strip() else "")
                    )

            return BaseOutput.from_cmd(res, success=True if may_fail else None)

        def _thread_action() -> BaseOutput:
            return _run_cmd(cmd)

        def _cancel_action() -> None:
            _run_cmd(cancel_cmd)

        return TaskOperation(
            log_name=self.log_name_setup,
            thread_action=_thread_action,
            wait_ready=lambda: self.confirm_server_alive(),
            cancel_action=_cancel_action,
        )


class ClientTask(Task, ABC):
    def __init__(self, ts: TestSettings, server: ServerTask):
        super().__init__(
            ts=ts,
            index=ts.client_index,
            tenant=ts.client_is_tenant,
            task_role=TaskRole.CLIENT,
        )

        pod_type = ts.client_pod_type
        node_location = self.node_location
        port = server.port
        connection_mode = ts.connection_mode

        if connection_mode in (
            ConnectionMode.MULTI_HOME,
            ConnectionMode.MNP_2ND_DENY,
            ConnectionMode.MNP_2ND_ALLOW,
            ConnectionMode.MNP_PRIMARY_DENY,
        ):
            in_file_template = "pod-secondary-network.yaml.j2"
            pod_name = f"normal-pod-secondary-{node_location}-client"
        elif pod_type == PodType.SRIOV:
            in_file_template = "sriov-pod.yaml.j2"
            pod_name = f"sriov-pod-{node_location}-client"
        elif pod_type == PodType.NORMAL:
            in_file_template = "pod.yaml.j2"
            pod_name = f"normal-pod-{node_location}-client"
        elif pod_type == PodType.HOSTBACKED:
            in_file_template = "host-pod.yaml.j2"
            pod_name = f"host-pod-{node_location}-client"
        else:
            raise ValueError("Invalid pod_type {pod_type}")

        in_file_template = tftbase.get_manifest(in_file_template)

        self.server = server
        self.port = port
        self.pod_type = pod_type
        self.connection_mode = ts.connection_mode
        self.test_type = ts.connection.test_type
        self.test_case_id = ts.test_case_id
        self.reverse = ts.reverse
        self.in_file_template = in_file_template
        self.pod_name = pod_name

    def initialize(self) -> None:
        super().initialize()
        self.render_pod_file("Client Pod Yaml")

    def get_target_ip(self) -> str:
        if self.server.use_internet:
            # URL is used directly by testTypeHttp; no IP needed.
            return ""
        if self.connection_mode == ConnectionMode.CLUSTER_IP:
            ip = self.server.get_cluster_ip()
            if ip is None:
                raise RuntimeError(
                    f"ClusterIP service not found for server {self.server.pod_name}"
                )
            logger.debug(f"get_target_ip() ClusterIP connection to {ip}")
            return ip
        elif self.connection_mode == ConnectionMode.NODE_PORT_IP:
            ip = self.server.get_nodeport_ip()
            if ip is None:
                raise RuntimeError(
                    f"NodePort service not found for server {self.server.pod_name}"
                )
            logger.debug(f"get_target_ip() NodePortIP connection to {ip}")
            return ip
        elif self.connection_mode == ConnectionMode.EXTERNAL_IP:
            # For port forwarding, connect to the host's IP, not the container's internal IP
            host_ip = self.get_host_ip()
            logger.debug(f"get_target_ip() External connection to host {host_ip}")
            return host_ip
        elif self.connection_mode == ConnectionMode.LOAD_BALANCER:
            ip = self.server.wait_load_balancer_ip()
            logger.debug(f"get_target_ip() LoadBalancer connection to {ip}")
            return ip
        elif self.connection_mode in (
            ConnectionMode.MULTI_HOME,
            ConnectionMode.MNP_2ND_DENY,
            ConnectionMode.MNP_2ND_ALLOW,
        ):
            server_ip = self.server.get_secondary_ip()
            return server_ip
        elif self.connection_mode == ConnectionMode.MNP_PRIMARY_DENY:
            server_ip = self.server.get_primary_ip()
            return server_ip
        server_ip = self.server.get_pod_ip()
        logger.debug(f"get_target_ip() Connection to server at {server_ip}")
        return server_ip

    def get_target_port(self) -> int:
        """Get the port to connect to. For EXTERNAL_IP, use discovered external_port."""
        if self.server.use_internet:
            # URL is used directly by testTypeHttp; no port needed.
            return 0
        if self.connection_mode == ConnectionMode.EXTERNAL_IP:
            return self.server.external_port
        return self.port

    def get_host_ip(self) -> str:
        """Get the host's IP address for EXTERNAL_IP mode."""
        # Get the IP from default route using JSON output
        r = self.lh.run("ip -j route get 1")
        if r.success and r.out.strip():
            try:
                routes = json.loads(r.out.strip())
                if routes and "prefsrc" in routes[0]:
                    ip: str = routes[0]["prefsrc"]
                    logger.debug(f"get_host_ip() found: {ip}")
                    return ip
            except (json.JSONDecodeError, KeyError, IndexError):
                pass
        raise RuntimeError("Failed to get host IP address from default route")

    def get_podman_ip(self, pod_name: str) -> str:
        cmd = "podman inspect --format '{{.NetworkSettings.IPAddress}}' " + pod_name

        for _ in range(5):
            ret = self.lh.run(cmd)
            if ret.success:
                ip_address = ret.out.strip()
                if ip_address:
                    logger.debug(f"get_podman_ip({pod_name}) found: {ip_address}")
                    return ip_address

            time.sleep(2)

        raise Exception(
            f"get_podman_ip(): failed to get {pod_name} ip after 5 attempts"
        )


class PluginTask(Task, ABC):
    @property
    @abstractmethod
    def plugin(self) -> Plugin:
        pass

    def get_plugin_metadata(self) -> tftbase.PluginMetadata:
        return tftbase.PluginMetadata(
            plugin_name=self.plugin.PLUGIN_NAME,
            node_name=self.node_name,
            pod_name=self.pod_name,
            plugin_role=self.task_role.name.lower(),
        )
