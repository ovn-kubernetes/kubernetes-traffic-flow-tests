import abc
import dataclasses
import datetime
import json
import logging
import os
import pathlib
import shlex
import threading
import typing
import yaml

from collections.abc import Generator
from dataclasses import dataclass
from typing import Any
from typing import Optional
from typing import TypeVar

from ktoolbox import common
from ktoolbox import host
from ktoolbox.common import StructParseBase
from ktoolbox.common import StructParseBaseNamed
from ktoolbox.common import StructParseParseContext
from ktoolbox.common import strict_dataclass
from ktoolbox.k8sClient import K8sClient

from pluginbase import Plugin
from testType import TestTypeHandler
from tftbase import ClusterMode
from tftbase import PodType
from tftbase import TestCaseType
from tftbase import TestType


logger = common.ExtendedLogger("tft." + __name__)


T1 = TypeVar("T1")


def _check_plugin_name(
    pctx: StructParseParseContext, name: str, is_plain_name: bool
) -> Plugin:
    import pluginbase

    try:
        return pluginbase.get_by_name(name)
    except ValueError:
        raise pctx.value_error(
            f"unknown plugin {repr(name)} (valid: {[p.PLUGIN_NAME for p in pluginbase.get_all()]}",
            key=None if is_plain_name else "name",
        ) from None


T2 = TypeVar("T2", bound="ConfNodeServer | ConfNodeClient")


@strict_dataclass
@dataclass(frozen=True, kw_only=True)
class _ConfBaseConnectionItem(StructParseBaseNamed, abc.ABC):
    @property
    def connection(self) -> "ConfConnection":
        return self._owner_reference.get(ConfConnection)


@strict_dataclass
@dataclass(frozen=True, kw_only=True)
class ConfNodeBase(_ConfBaseConnectionItem, abc.ABC):
    sriov: bool
    pod_type: PodType
    default_network: str
    privileged_pod: Optional[bool]

    # Extra arguments for the client/server. Their actual meaning depend on the
    # "type". These might be command line arguments passed to the tool.
    args: Optional[tuple[str, ...]]

    @property
    def is_persistent_server(self) -> bool:
        return False

    def serialize(self) -> dict[str, Any]:
        d: dict[str, Any] = {}
        common.dict_add_optional(d, "privileged_pod", self.privileged_pod)
        if self.args is not None:
            d["args"] = list(self.args)
        return {
            **super().serialize(),
            "sriov": self.sriov,
            "default_network": self.default_network,
            **d,
        }

    @staticmethod
    def _parse(
        conf_type: type[T2],
        pctx: StructParseParseContext,
    ) -> T2:
        with pctx.with_strdict() as varg:

            name = common.structparse_pop_str_name(
                varg.for_name(),
                check=common.validate_dns_name,
            )

            sriov = common.structparse_pop_bool(
                varg.for_key("sriov"),
                default=False,
            )

            default_network = common.structparse_pop_str(
                varg.for_key("default_network"),
                default=None,
            )
            if default_network is None:
                default_network = common.structparse_pop_str(
                    varg.for_key("default-network"),
                    default="default/default",
                )

            privileged_pod = common.structparse_pop_bool(
                varg.for_key("privileged_pod"),
                default=None,
            )

            def _construct_args(pctx2: StructParseParseContext) -> tuple[str, ...]:
                if isinstance(pctx2.arg, str):
                    try:
                        lst = shlex.split(pctx2.arg)
                    except Exception:
                        raise pctx2.value_error(
                            f"cannot parse command line {repr(pctx2.arg)}"
                        )
                    return tuple(lst)
                if isinstance(pctx2.arg, list):
                    if not all(isinstance(x, str) for x in pctx2.arg):
                        raise pctx2.value_error(
                            f"expects a list of strings but got {repr(pctx2.arg)}"
                        )
                    return tuple(pctx2.arg)
                raise pctx2.value_error(
                    f"expects a string or a list of strings but got {repr(pctx2.arg)}"
                )

            args = common.structparse_pop_obj(
                varg.for_key("args"),
                construct=_construct_args,
                default=None,
            )

            type_specific_kwargs = {}

            if conf_type == ConfNodeServer:
                persistent = common.structparse_pop_bool(
                    varg.for_key("persistent"),
                    default=False,
                )
                type_specific_kwargs["persistent"] = persistent

        result = conf_type(
            yamlidx=pctx.yamlidx,
            yamlpath=pctx.yamlpath,
            name=name,
            pod_type=PodType.SRIOV if sriov else PodType.NORMAL,
            sriov=sriov,
            default_network=default_network,
            privileged_pod=privileged_pod,
            args=args,
            **type_specific_kwargs,
        )

        return typing.cast("T2", result)

    def _validate(self, test_type: TestType) -> None:
        if self.args is not None:
            if test_type not in (TestType.SIMPLE,):
                raise self.value_error(
                    f"not supported with test type {repr(test_type.name)}",
                    key="args",
                )


@strict_dataclass
@dataclass(frozen=True, kw_only=True)
class ConfPlugin(_ConfBaseConnectionItem):
    plugin: Plugin

    @staticmethod
    def parse(pctx: StructParseParseContext) -> "ConfPlugin":

        is_plain_name = isinstance(pctx.arg, str)

        if is_plain_name:
            # For convenience, we allow that the entry is a plain string instead
            # of a dictionary with "name" entry.
            name = pctx.arg
        else:
            with pctx.with_strdict() as varg:
                name = common.structparse_pop_str_name(varg.for_name())

        plugin = _check_plugin_name(pctx, name, is_plain_name)

        return ConfPlugin(
            yamlidx=pctx.yamlidx,
            yamlpath=pctx.yamlpath,
            name=name,
            plugin=plugin,
        )


@strict_dataclass
@dataclass(frozen=True, kw_only=True)
class ConfNodeServer(ConfNodeBase):
    persistent: bool

    @property
    def is_persistent_server(self) -> bool:
        return self.persistent

    def serialize(self) -> dict[str, Any]:
        return {
            **super().serialize(),
            "persistent": self.persistent,
        }

    @staticmethod
    def parse(pctx: StructParseParseContext) -> "ConfNodeServer":
        return ConfNodeBase._parse(ConfNodeServer, pctx)


@strict_dataclass
@dataclass(frozen=True, kw_only=True)
class ConfNodeClient(ConfNodeBase):
    @staticmethod
    def parse(pctx: StructParseParseContext) -> "ConfNodeClient":
        return ConfNodeBase._parse(ConfNodeClient, pctx)


@strict_dataclass
@dataclass(frozen=True, kw_only=True)
class ConfConnection(StructParseBaseNamed):
    test_type: TestType
    test_type_handler: TestTypeHandler
    instances: int
    server: tuple[ConfNodeServer, ...]
    client: tuple[ConfNodeClient, ...]
    plugins: tuple[ConfPlugin, ...]
    secondary_network_nad: Optional[str]
    resource_name: Optional[str]

    # This parameter is not expressed in YAML. It gets passed by the parent to
    # ConfConnection.parse()
    namespace: str

    def __post_init__(self) -> None:
        for s in self.server:
            s._owner_reference.init(self)
        for c in self.client:
            c._owner_reference.init(self)
        for p in self.plugins:
            p._owner_reference.init(self)

    @property
    def tft(self) -> "ConfTest":
        return self._owner_reference.get(ConfTest)

    def serialize(self) -> dict[str, Any]:
        extra: dict[str, Any] = {}
        common.dict_add_optional(
            extra, "secondary_network_nad", self.secondary_network_nad
        )
        common.dict_add_optional(extra, "resource_name", self.resource_name)
        return {
            **super().serialize(),
            "type": self.test_type.name,
            "instances": self.instances,
            "server": [s.serialize() for s in self.server],
            "client": [c.serialize() for c in self.client],
            "plugins": [p.serialize() for p in self.plugins],
            **extra,
        }

    @property
    def effective_secondary_network_nad(self) -> str:
        nad = self.secondary_network_nad
        if nad is None:
            nad = "tft-secondary"
        if "/" not in nad:
            nad = f"{self.namespace}/{nad}"
        return nad

    @staticmethod
    def parse(
        pctx: StructParseParseContext,
        *,
        test_name: str,
        namespace: str,
    ) -> "ConfConnection":
        with pctx.with_strdict() as varg:

            name = common.structparse_pop_str_name(
                varg.for_name(),
                default=f"Connection {test_name}/{pctx.yamlidx+1}",
            )

            test_type = common.structparse_pop_enum(
                varg.for_key("type"),
                enum_type=TestType,
                default=TestType.IPERF_TCP,
            )

            try:
                test_type_handler = TestTypeHandler.get(test_type)
            except ValueError:
                raise pctx.value_error(
                    f"{repr(test_type.name)} is not implemented", key="type"
                ) from None

            instances = common.structparse_pop_int(
                varg.for_key("instances"),
                default=1,
                check=lambda val: val > 0,
            )

            server = common.structparse_pop_objlist(
                varg.for_key("server"),
                construct=ConfNodeServer.parse,
            )

            client = common.structparse_pop_objlist(
                varg.for_key("client"),
                construct=ConfNodeClient.parse,
            )

            plugins = common.structparse_pop_objlist(
                varg.for_key("plugins"),
                construct=ConfPlugin.parse,
            )

            secondary_network_nad = common.structparse_pop_str(
                varg.for_key("secondary_network_nad"),
                default=None,
            )

            resource_name = common.structparse_pop_str(
                varg.for_key("resource_name"),
                default=None,
            )

        if len(server) > 1:
            raise pctx.value_error(
                "currently only one server entry is supported", key="server"
            )

        if len(client) > 1:
            raise pctx.value_error(
                "currently only one client entry is supported", key="client"
            )

        for s in server:
            s._validate(test_type)

        for c in client:
            c._validate(test_type)

        return ConfConnection(
            yamlidx=pctx.yamlidx,
            yamlpath=pctx.yamlpath,
            name=name,
            test_type=test_type,
            test_type_handler=test_type_handler,
            instances=instances,
            server=server,
            client=client,
            plugins=plugins,
            secondary_network_nad=secondary_network_nad,
            resource_name=resource_name,
            namespace=namespace,
        )


@strict_dataclass
@dataclass(frozen=True, kw_only=True)
class ConfTest(StructParseBaseNamed):
    namespace: str
    test_cases: tuple[TestCaseType, ...]
    duration: int
    privileged_pod: bool
    connections: tuple[ConfConnection, ...]
    logs: pathlib.Path

    def __post_init__(self) -> None:
        for c in self.connections:
            c._owner_reference.init(self)

    @property
    def config(self) -> "ConfConfig":
        return self._owner_reference.get(ConfConfig)

    def serialize(self) -> dict[str, Any]:
        return {
            **super().serialize(),
            "namespace": self.namespace,
            "test_cases": [t.name for t in self.test_cases],
            "duration": self.duration,
            "privileged_pod": self.privileged_pod,
            "connections": [c.serialize() for c in self.connections],
            "logs": str(self.logs),
        }

    @staticmethod
    def parse(pctx: StructParseParseContext) -> "ConfTest":

        with pctx.with_strdict() as varg:

            name = common.structparse_pop_str_name(
                varg.for_name(),
                default=f"Test {pctx.yamlidx+1}",
            )

            namespace = common.structparse_pop_str(
                varg.for_key("namespace"),
                default="default",
            )

            def _construct_test_cases(
                pctx2: StructParseParseContext,
            ) -> tuple[TestCaseType, ...]:
                arg = pctx2.arg
                if arg is None or (isinstance(arg, str) and arg == ""):
                    # By default, all test case are run.
                    arg = "*"
                try:
                    lst = common.enum_convert_list(TestCaseType, arg)
                except Exception:
                    raise pctx2.value_error(
                        "value is not a valid list of test cases"
                    ) from None
                return tuple(lst)

            test_cases = common.structparse_pop_obj(
                varg.for_key("test_cases"),
                construct=_construct_test_cases,
                construct_default=True,
            )

            duration = common.structparse_pop_int(
                varg.for_key("duration"),
                default=0,
                check=lambda val: val >= 0,
                description="a duration in seconds",
            )
            if duration == 0:
                duration = 3600

            privileged_pod = common.structparse_pop_bool(
                varg.for_key("privileged_pod"),
                default=False,
            )

            connections = common.structparse_pop_objlist(
                varg.for_key("connections"),
                construct=lambda pctx2: ConfConnection.parse(
                    pctx2,
                    test_name=name,
                    namespace=namespace,
                ),
                allow_empty=False,
            )

            logs = common.structparse_pop_str(
                varg.for_key("logs"),
                default="ft-logs",
            )

        return ConfTest(
            yamlidx=pctx.yamlidx,
            yamlpath=pctx.yamlpath,
            name=name,
            namespace=namespace,
            test_cases=tuple(test_cases),
            duration=duration,
            privileged_pod=privileged_pod,
            connections=connections,
            logs=pathlib.Path(logs),
        )

    @property
    def logs_abspath(self) -> pathlib.Path:
        return common.path_norm(
            self.logs,
            cwd=self.config.test_config.cwddir,
        )

    def get_output_file(self) -> pathlib.Path:
        output_base = self.config.test_config.output_base

        if output_base is None:
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
            return self.logs_abspath / f"{timestamp}.json"

        path = common.path_norm(
            output_base,
            cwd=self.config.test_config.cwddir,
            preserve_dir=True,
        )
        tft_idx = f"{self.config.tft_idx:03d}"
        if path[-1] == "/":
            base = "result-"
        else:
            base = os.path.basename(path)
            path = os.path.dirname(path)
        return pathlib.Path(os.path.join(path, f"{base}{tft_idx}.json"))


@strict_dataclass
@dataclass(frozen=True, kw_only=True)
class ConfConfig(StructParseBase):
    tft: tuple[ConfTest, ...]
    kubeconfig: Optional[str]
    kubeconfig_infra: Optional[str]

    def __post_init__(self) -> None:
        for t in self.tft:
            t._owner_reference.init(self)

    @property
    def test_config(self) -> "TestConfig":
        return self._owner_reference.get(TestConfig)

    @property
    def tft_idx(self) -> int:
        return self.yamlidx

    def serialize(self) -> dict[str, Any]:
        return {
            "tft": [c.serialize() for c in self.tft],
            "kubeconfig": self.kubeconfig,
            "kubeconfig_infra": self.kubeconfig_infra,
        }

    @staticmethod
    def parse(full_config: Any) -> "ConfConfig":
        pctx = StructParseParseContext(full_config)
        with pctx.with_strdict() as varg:

            tft = common.structparse_pop_objlist(
                varg.for_key("tft"),
                construct=ConfTest.parse,
                allow_empty=False,
            )

            kubeconfig = common.structparse_pop_str(
                varg.for_key("kubeconfig"),
                default=None,
            )

            kubeconfig_infra = common.structparse_pop_str(
                varg.for_key("kubeconfig_infra"),
                default=None,
            )

        if kubeconfig_infra is not None:
            if kubeconfig is None:
                raise pctx.value_error(
                    "missing parameter when kubeconfig_infra is given",
                    key="kubeconfig",
                )

        return ConfConfig(
            yamlidx=pctx.yamlidx,
            yamlpath=pctx.yamlpath,
            tft=tft,
            kubeconfig=kubeconfig,
            kubeconfig_infra=kubeconfig_infra,
        )


class TestConfig:
    KUBECONFIG_TENANT: str = "/root/kubeconfig.tenantcluster"
    KUBECONFIG_INFRA: str = "/root/kubeconfig.infracluster"
    KUBECONFIG_SINGLE: str = "/root/kubeconfig.nicmodecluster"
    KUBECONFIG_CX: str = "/root/kubeconfig.smartniccluster"

    full_config: dict[str, Any]
    config: ConfConfig
    configpath: Optional[str]
    configdir: str
    cwddir: str
    _kubeconfig_pair: Optional[tuple[str, Optional[str]]]
    _client_tenant: Optional[K8sClient]
    _client_infra: Optional[K8sClient]
    _lock: threading.Lock
    evaluator_config: Optional[str]
    output_base: Optional[str]

    @property
    def mode(self) -> ClusterMode:
        if self.kubeconfig_infra is None:
            return ClusterMode.SINGLE
        return ClusterMode.DPU

    @staticmethod
    def _detect_kubeconfigs(
        *,
        configpath: Optional[str],
        cwd: str,
    ) -> tuple[str, Optional[str]]:

        # Find out what type of cluster are we in.

        err_msg: Optional[str] = None
        kubeconfig: str = ""
        kubeconfig_infra: Optional[str] = None

        if host.local.file_exists(TestConfig.KUBECONFIG_SINGLE):
            kubeconfig = TestConfig.KUBECONFIG_SINGLE
        elif host.local.file_exists(TestConfig.KUBECONFIG_CX):
            kubeconfig = TestConfig.KUBECONFIG_CX
        elif host.local.file_exists(TestConfig.KUBECONFIG_TENANT):
            if host.local.file_exists(TestConfig.KUBECONFIG_INFRA):
                kubeconfig = TestConfig.KUBECONFIG_TENANT
                kubeconfig_infra = TestConfig.KUBECONFIG_INFRA
            else:
                err_msg = f"misses kubeconfig_infra file {repr(TestConfig.KUBECONFIG_INFRA)} while having a kubeconfig {repr(TestConfig.KUBECONFIG_TENANT)}"
        else:
            err_msg = f"neither have any of the default kubeconfig files {repr([TestConfig.KUBECONFIG_SINGLE, TestConfig.KUBECONFIG_CX, TestConfig.KUBECONFIG_TENANT])}"

        if err_msg is not None:
            prefix = f" {repr(configpath)}" if configpath else ""
            raise RuntimeError(
                f"kubeconfig not specified in configuration{prefix} and {err_msg}"
            )

        kubeconfig = common.path_norm(kubeconfig, cwd=cwd)
        kubeconfig_infra = common.path_norm(kubeconfig_infra, cwd=cwd)
        assert kubeconfig
        assert kubeconfig_infra is None or kubeconfig_infra
        return (kubeconfig, kubeconfig_infra)

    def __init__(
        self,
        *,
        full_config: Optional[dict[str, Any]] = None,
        config_path: Optional[str] = None,
        kubeconfigs: Optional[tuple[str, Optional[str]]] = None,
        evaluator_config: Optional[str] = None,
        output_base: Optional[str] = None,
        cwddir: str = ".",
    ) -> None:

        cwddir = common.path_norm(cwddir, cwd=os.getcwd())

        config_path = common.path_norm(config_path, cwd=cwddir)

        if config_path is not None:
            configdir = os.path.dirname(config_path)
        else:
            configdir = cwddir

        if config_path is not None:
            if full_config is not None:
                raise ValueError(
                    "Must either specify a full_config or a config_path argument"
                )
            try:
                with open(config_path, "r") as f:
                    full_config = yaml.safe_load(f)
            except Exception as e:
                raise ValueError(
                    f"Failure to read YAML configuration {repr(config_path)}: {e}"
                )

        if not isinstance(full_config, dict):
            raise ValueError(
                f"invalid config is not a dictionary but {type(full_config)}"
            )

        try:
            config = ConfConfig.parse(full_config)
        except Exception as e:
            p = (f" {repr(config_path)}") if config_path else ""
            raise ValueError(f"invalid configuration{p}: {e}")

        config._owner_reference.init(self)

        self.full_config = full_config
        self.config = config

        self.configdir = configdir
        self.cwddir = cwddir
        self.configpath = config_path

        self._lock = threading.Lock()
        self._client_tenant = None
        self._client_infra = None

        if not output_base:
            output_base = None
        self.output_base = output_base

        kubconfig_pair: tuple[Optional[str], Optional[str]]
        if kubeconfigs is not None:
            kubconfig_pair = kubeconfigs
            kubeconfigs_cwd = cwddir
        else:
            kubconfig_pair = (self.config.kubeconfig, self.config.kubeconfig_infra)
            kubeconfigs_cwd = configdir
        kubeconfig, kubeconfig_infra = kubconfig_pair
        assert kubeconfig is None or kubeconfig
        assert kubeconfig_infra is None or kubeconfig_infra
        assert kubeconfig_infra is None or kubeconfig is not None
        if kubeconfig is not None:
            self._kubeconfig_pair = (
                common.path_norm(kubeconfig, cwd=kubeconfigs_cwd),
                common.path_norm(kubeconfig_infra, cwd=kubeconfigs_cwd),
            )
        else:
            self._kubeconfig_pair = None

        self.evaluator_config = evaluator_config

    @property
    def kubeconfig(self) -> str:
        kubeconfig, kubeconfig_infra = self._get_kubeconfigs()
        return kubeconfig

    @property
    def kubeconfig_infra(self) -> Optional[str]:
        kubeconfig, kubeconfig_infra = self._get_kubeconfigs()
        return kubeconfig_infra

    def _get_kubeconfigs(self) -> tuple[str, Optional[str]]:
        with self._lock:
            return self._get_kubeconfigs_with_lock()

    def _get_kubeconfigs_with_lock(self) -> tuple[str, Optional[str]]:
        if self._kubeconfig_pair is None:
            self._kubeconfig_pair = TestConfig._detect_kubeconfigs(
                configpath=self.configpath,
                cwd=self.cwddir,
            )
        kubeconfig, kubeconfig_infra = self._kubeconfig_pair
        assert kubeconfig
        assert kubeconfig_infra is None or kubeconfig_infra
        assert kubeconfig_infra is None or kubeconfig is not None
        return kubeconfig, kubeconfig_infra

    def client(self, *, tenant: bool) -> K8sClient:
        with self._lock:
            kubeconfig, kubeconfig_infra = self._get_kubeconfigs_with_lock()
            if tenant:
                client = self._client_tenant
            else:
                if kubeconfig_infra is None:
                    raise RuntimeError("TestConfig has no infra client")
                client = self._client_infra

            if client is None:
                if tenant:
                    self._client_tenant = (client := K8sClient(kubeconfig))
                else:
                    self._client_infra = (client := K8sClient(kubeconfig_infra))

            return client

    @property
    def client_tenant(self) -> K8sClient:
        return self.client(tenant=True)

    @property
    def client_infra(self) -> K8sClient:
        return self.client(tenant=False)

    def _system_check_kubeconfig(
        self,
        tenant: bool,
    ) -> None:

        kubeconfig: Optional[str]
        if tenant:
            kubeconfig = self.kubeconfig
            config_name = "kubeconfig"
            config_value = self.config.kubeconfig
        else:
            kubeconfig = self.kubeconfig_infra
            if kubeconfig is None:
                return
            config_name = "kubeconfig_infra"
            config_value = self.config.kubeconfig_infra

        try:
            self.client(tenant=tenant)
        except Exception:
            if not os.path.exists(kubeconfig):
                fail_msg = f"file {repr(kubeconfig)} does not exist"
            else:
                fail_msg = f"file {repr(kubeconfig)} is not a valid KUBECONFIG"
        else:
            return

        msg_path = f" {repr(self.configpath)}" if self.configpath else ""
        msg_source = ""

        if config_value is not None:
            msg_source = f'key ".{config_name}"'
        else:
            msg_source = f"autodetected {config_name}"

        raise ValueError(
            f"configuration{msg_path} is invalid: {msg_source} fails because {fail_msg}"
        )

    def system_check(self) -> None:
        self._system_check_kubeconfig(tenant=True)
        self._system_check_kubeconfig(tenant=False)

        if self.evaluator_config is not None:
            if not os.path.exists(self.evaluator_config):
                raise ValueError(
                    f"evaluator_config file {shlex.quote(self.evaluator_config)} does not exist"
                )

    def log_config(self, *, logger: logging.Logger = logger) -> None:
        s = json.dumps(self.full_config["tft"])

        with self._lock:
            # In all other cases, accessing kubeconfig will initialize the
            # value.  But for logging, we look at it, and if it's not yet
            # initialized, log that it's unknown yet.
            kubeconfig: Optional[str] = None
            kubeconfig_infra: Optional[str] = None
            if self._kubeconfig_pair:
                kubeconfig, kubeconfig_infra = self._kubeconfig_pair
        if kubeconfig is not None:
            logger.info(f"config: KUBECONFIG={shlex.quote(kubeconfig)}")
        else:
            logger.info(
                "config: KUBECONFIG is not specified in YAML configuration and not (yet) detected"
            )
        if kubeconfig_infra is not None:
            logger.info(f"config: KUBECONFIG_INFRA={shlex.quote(kubeconfig_infra)}")

        if self.evaluator_config is not None:
            logger.info(f"config: EVAL_CONFIG={shlex.quote(self.evaluator_config)}")
        logger.info(f"config: {s}")
        logger.debug(f"config-full: {self.config.serialize_json()}")


@strict_dataclass
@dataclass(frozen=True)
class ConfigDescriptor:
    tc: TestConfig
    tft_idx: int = dataclasses.field(default=-1, kw_only=True)
    test_cases_idx: int = dataclasses.field(default=-1, kw_only=True)
    connections_idx: int = dataclasses.field(default=-1, kw_only=True)

    def _post_check(self) -> None:
        if self.tft_idx < -1 or self.tft_idx >= len(self.tc.config.tft):
            raise ValueError("tft_idx out of range")

        if self.test_cases_idx < -1:
            raise ValueError("test_cases_idx out of range")
        if self.test_cases_idx >= 0:
            if self.tft_idx < 0:
                raise ValueError("test_cases_idx requires tft_idx")
            if self.test_cases_idx >= len(self.tc.config.tft[self.tft_idx].test_cases):
                raise ValueError("test_cases_idx out or range")

        if self.connections_idx < -1:
            raise ValueError("connections_idx out of range")
        if self.connections_idx >= 0:
            if self.tft_idx < 0:
                raise ValueError("connections_idx requires tft_idx")
            if self.connections_idx >= len(
                self.tc.config.tft[self.tft_idx].connections
            ):
                raise ValueError("connections_idx out or range")

    def get_tft(self) -> ConfTest:
        if self.tft_idx < 0:
            raise RuntimeError("No tft_idx set")
        return self.tc.config.tft[self.tft_idx]

    def get_test_case(self) -> TestCaseType:
        if self.test_cases_idx < 0:
            raise RuntimeError("No test_cases_idx set")
        return self.get_tft().test_cases[self.test_cases_idx]

    def get_connection(self) -> ConfConnection:
        if self.connections_idx < 0:
            raise RuntimeError("No connections_idx set")
        return self.get_tft().connections[self.connections_idx]

    def get_server(self) -> ConfNodeServer:
        c = self.get_connection()
        assert len(c.server) == 1
        return c.server[0]

    def get_client(self) -> ConfNodeClient:
        c = self.get_connection()
        assert len(c.client) == 1
        return c.client[0]

    def describe_all_tft(self) -> Generator["ConfigDescriptor", None, None]:
        for tft_idx in range(len(self.tc.config.tft)):
            yield ConfigDescriptor(tc=self.tc, tft_idx=tft_idx)

    def describe_all_test_cases(self) -> Generator["ConfigDescriptor", None, None]:
        for test_cases_idx in range(len(self.get_tft().test_cases)):
            yield ConfigDescriptor(
                tc=self.tc,
                tft_idx=self.tft_idx,
                connections_idx=self.connections_idx,
                test_cases_idx=test_cases_idx,
            )

    def describe_all_connections(self) -> Generator["ConfigDescriptor", None, None]:
        for connections_idx in range(len(self.get_tft().connections)):
            yield ConfigDescriptor(
                tc=self.tc,
                tft_idx=self.tft_idx,
                test_cases_idx=self.test_cases_idx,
                connections_idx=connections_idx,
            )
