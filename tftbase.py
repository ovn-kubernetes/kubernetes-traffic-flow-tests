import dataclasses
import functools
import json
import math
import os
import re
import shlex
import typing

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any
from typing import Optional

from ktoolbox import common
from ktoolbox import host
from ktoolbox.common import strict_dataclass


logger = common.ExtendedLogger("tft." + __name__)


ENV_TFT_TEST_IMAGE = "TFT_TEST_IMAGE"
ENV_TFT_IMAGE_PULL_POLICY = "TFT_IMAGE_PULL_POLICY"

ENV_TFT_PRIVILEGED_POD = "TFT_PRIVILEGED_POD"

ENV_TFT_TEST_IMAGE_DEFAULT = (
    "ghcr.io/ovn-kubernetes/kubernetes-traffic-flow-tests:latest"
)

ENV_TFT_MANIFESTS_OVERRIDES = "TFT_MANIFESTS_OVERRIDES"
ENV_TFT_MANIFESTS_YAMLS = "TFT_MANIFESTS_YAMLS"


def get_environ(name: str) -> Optional[str]:
    # Some environment variables are honored as configuration.
    # Which ones? Run `git grep -w get_environ`!
    return os.environ.get(name, None)


@functools.cache
def get_tft_test_image() -> str:
    s = get_environ(ENV_TFT_TEST_IMAGE) or ENV_TFT_TEST_IMAGE_DEFAULT
    logger.info(f"env: {ENV_TFT_TEST_IMAGE}={shlex.quote(s)}")
    return s


@functools.cache
def get_tft_image_pull_policy() -> str:
    s: Optional[str] = None
    s_env = get_environ(ENV_TFT_IMAGE_PULL_POLICY)
    if s_env is not None:
        s0 = s_env.strip().lower()
        if s0 == "always":
            s = "Always"
        elif s0 == "ifnotpresent":
            s = "IfNotPresent"
        elif s0 == "never":
            s = "Never"
        else:
            logger.error(
                f'env: invalid environment variable in {ENV_TFT_IMAGE_PULL_POLICY}="{shlex.quote(s_env)}". Set to one of "IfNotPresent", "Always", "Never"'
            )
    if s is None:
        if get_environ(ENV_TFT_TEST_IMAGE):
            s = "Always"
        else:
            s = "IfNotPresent"
    logger.info(f"env: {ENV_TFT_IMAGE_PULL_POLICY}={shlex.quote(s)}")
    return s


@functools.cache
def get_tft_privileged_pod() -> Optional[bool]:
    d = get_environ(ENV_TFT_PRIVILEGED_POD)
    value = common.str_to_bool(d, on_default=None)
    logger.info(
        f"env: {ENV_TFT_PRIVILEGED_POD}={common.bool_to_str(value) if value is not None else ''}"
    )
    return value


@functools.cache
def get_tft_manifests_overrides() -> Optional[str]:
    d = get_environ(ENV_TFT_MANIFESTS_OVERRIDES)
    if d:
        path = common.path_norm(d, cwd=cwd)
    elif d == "":
        path = None
    else:
        path = tftfile("manifests/overrides")

    # We don't check whether the overrides directory exist. Since the
    # individual overrides files are optional, so is the entire directory.

    logger.info(f"env: {ENV_TFT_MANIFESTS_OVERRIDES}={shlex.quote(path or '')}")
    return path


@functools.cache
def get_tft_manifests_yamls() -> str:
    d = get_environ(ENV_TFT_MANIFESTS_YAMLS)
    if d:
        path = common.path_norm(d, cwd=cwd)
    else:
        path = tftfile("manifests/yamls")

    if not os.path.isdir(path):
        raise RuntimeError(
            f"{ENV_TFT_MANIFESTS_YAMLS} output directory {shlex.quote(path)} does not exist"
        )

    logger.info(f"env: {ENV_TFT_MANIFESTS_YAMLS}={shlex.quote(path)}")
    return path


TFT_TESTS = "tft-tests"


T = typing.TypeVar("T")


cwd = os.getcwd()

basedir = common.path_norm(os.path.dirname(__file__), cwd=cwd)


def tftfile(*components: str) -> str:
    f = basedir + "/" + "/".join(components)
    return common.path_norm(f)


@functools.cache
def get_manifest(filename: str) -> str:
    assert ".." not in filename.split("/")
    overrides = get_tft_manifests_overrides()
    if overrides is not None:
        f1 = common.path_norm(overrides + "/" + filename, cwd=cwd)
        if os.path.exists(f1):
            return f1
    f2 = tftfile("manifests", filename)
    if os.path.exists(f2):
        return f2
    msg = ""
    if overrides is not None:
        msg = f"{repr(f1)} and "
    raise ValueError(
        f"Could not find manifest file {repr(filename)}. Checked in {msg}{repr(f2)}."
    )


def str_sanitize(value: str) -> str:
    # Sanitize the string like a DNS label.
    #
    # The main purpose is to use this as a pod name.
    #
    # - reversible (in theory)
    # - only lower case characters, digits and '-'.
    # - encoding:
    #     "-"     => "--"
    #     "A"-"Z" => "-a" to "-z"
    #     "."     => "-0"
    #     *       => "-<hex>-"
    # - no '-' at begining or end of string. Leading or trailing "[-ps]" get
    #   prefix/suffix "p-"/"-s".
    def _repl(m: re.Match[str]) -> str:
        ch = m.group(0)
        if ch == "-":
            return "--"
        if "A" <= ch <= "Z":
            return f"-{ch.lower()}"
        if ch == ".":
            return "-0"
        return f"-{ord(ch):2x}-"

    v = re.sub(r"[^a-z0-9]", _repl, value)

    # No leading or tailing dash. Replace them with
    # "p-"/"-s".
    prefix = ""
    suffix = ""
    if v:
        if v[0] in ("-", "p", "s"):
            prefix = "p-"
        if len(v) > 1 and v[-1] in ("-", "p", "s"):
            suffix = "-s"
    return prefix + v + suffix


@functools.cache
def get_manifest_renderpath(filename: str) -> str:
    return common.path_norm(get_tft_manifests_yamls() + "/" + filename)


def eval_binary_opt_in(
    a: Optional[bool],
    b: Optional[bool],
) -> tuple[bool, bool]:
    if a is None and b is None:
        # If both are unset, we return True,True
        return True, True

    # Normalize values to a Optional[bool].
    if a is not None:
        a = bool(a)
    if b is not None:
        b = bool(b)

    # if one of the arguments is unset, it's the opposite
    # of the other.
    if a is None:
        a = not b
    if b is None:
        b = not a

    return a, b


class ClusterMode(Enum):
    SINGLE = 1
    DPU = 3


class TaskRole(Enum):
    CLIENT = 1
    SERVER = 2


class TestType(Enum):
    IPERF_TCP = 1
    IPERF_UDP = 2
    HTTP = 3
    NETPERF_TCP_STREAM = 4
    NETPERF_TCP_RR = 5
    SIMPLE = 6


class PodType(Enum):
    NORMAL = 1
    SRIOV = 2
    HOSTBACKED = 3


class TestCaseType(Enum):
    POD_TO_POD_SAME_NODE = 1
    POD_TO_POD_DIFF_NODE = 2
    POD_TO_HOST_SAME_NODE = 3
    POD_TO_HOST_DIFF_NODE = 4
    POD_TO_CLUSTER_IP_TO_POD_SAME_NODE = 5
    POD_TO_CLUSTER_IP_TO_POD_DIFF_NODE = 6
    POD_TO_CLUSTER_IP_TO_HOST_SAME_NODE = 7
    POD_TO_CLUSTER_IP_TO_HOST_DIFF_NODE = 8
    POD_TO_NODE_PORT_TO_POD_SAME_NODE = 9
    POD_TO_NODE_PORT_TO_POD_DIFF_NODE = 10
    HOST_TO_POD_SAME_NODE = 15
    HOST_TO_POD_DIFF_NODE = 16
    HOST_TO_CLUSTER_IP_TO_POD_SAME_NODE = 17
    HOST_TO_CLUSTER_IP_TO_POD_DIFF_NODE = 18
    HOST_TO_CLUSTER_IP_TO_HOST_SAME_NODE = 19
    HOST_TO_CLUSTER_IP_TO_HOST_DIFF_NODE = 20
    HOST_TO_NODE_PORT_TO_POD_SAME_NODE = 21
    HOST_TO_NODE_PORT_TO_POD_DIFF_NODE = 22
    HOST_TO_NODE_PORT_TO_HOST_SAME_NODE = 23
    HOST_TO_NODE_PORT_TO_HOST_DIFF_NODE = 24
    POD_TO_EXTERNAL = 25
    POD_TO_POD_2ND_INTERFACE_SAME_NODE = 27
    POD_TO_POD_2ND_INTERFACE_DIFF_NODE = 28
    POD_TO_POD_MULTI_NETWORK_POLICY = 29

    @property
    def info(self) -> "TestCaseTypInfo":
        return _test_case_typ_infos[self]


class ConnectionMode(Enum):
    POD_IP = 1
    CLUSTER_IP = 2
    NODE_PORT_IP = 3
    EXTERNAL_IP = 4
    MULTI_NETWORK = 5
    MULTI_HOME = 6


@strict_dataclass
@dataclass(frozen=True, kw_only=True)
class Bitrate:
    tx: Optional[float]
    rx: Optional[float]

    NA: typing.ClassVar["Bitrate"]

    def __init__(
        self,
        *,
        tx: None | int | float = None,
        rx: None | int | float = None,
    ) -> None:
        if isinstance(tx, int):
            tx = float(tx)
        if isinstance(rx, int):
            rx = float(rx)
        object.__setattr__(self, "tx", tx)
        object.__setattr__(self, "rx", rx)

    def _valid_x(self, f: Optional[float]) -> bool:
        return f is None or (f >= 0.0 and not math.isinf(f) and not math.isnan(f))

    def _post_init(self) -> None:
        if not self._valid_x(self.tx):
            raise ValueError("tx is not a valid bitrange")
        if not self._valid_x(self.rx):
            raise ValueError("rx is not a valid bitrange")

    @property
    def is_na(self) -> bool:
        return self.tx is None and self.rx is None

    def is_passing(
        self,
        threshold: Optional[float],
        *,
        rx: Optional[bool] = None,
        tx: Optional[bool] = None,
    ) -> bool:
        if threshold is None:
            return True
        rx, tx = eval_binary_opt_in(rx, tx)
        if tx:
            if self.tx is not None and self.tx < threshold:
                return False
        if rx:
            if self.rx is not None and self.rx < threshold:
                return False
        return True

    @property
    def pretty_str(self) -> str:
        return f"[rx={self.rx},tx={self.tx}]"

    @staticmethod
    def get_pretty_str(bitrate: Optional["Bitrate"]) -> str:
        if bitrate is None:
            return "None"
        return bitrate.pretty_str


Bitrate.NA = Bitrate()


@strict_dataclass
@dataclass(frozen=True, kw_only=True)
class PodInfo:
    name: str
    pod_type: PodType
    is_tenant: bool
    index: int


@strict_dataclass
@dataclass(frozen=True, kw_only=True)
class PluginMetadata:
    plugin_name: str
    node_name: str
    pod_name: str


@strict_dataclass
@dataclass(frozen=True, kw_only=True)
class TestMetadata:
    tft_idx: int
    test_cases_idx: int
    connections_idx: int
    test_case_id: TestCaseType
    test_type: TestType
    reverse: bool
    server: PodInfo
    client: PodInfo


@strict_dataclass
@dataclass(frozen=True, kw_only=True)
class EvalResult:
    success: bool
    msg: Optional[str] = None
    bitrate_threshold_rx: Optional[float] = None
    bitrate_threshold_tx: Optional[float] = None


@strict_dataclass
@dataclass(frozen=True, kw_only=True)
class BaseOutput:
    success: bool = True
    msg: Optional[str] = None

    @property
    def eval_success(self) -> bool:
        return self.eval_msg is None

    @property
    def eval_msg(self) -> Optional[str]:
        if self.success:
            return None
        if self.msg is not None:
            return self.msg
        return "unspecified failure"

    @staticmethod
    def from_cmd(
        result: host.Result, *, success: Optional[bool] = None
    ) -> "BaseOutput":
        if success is None:
            success = result.success
        return BaseOutput(
            success=success,
            msg=result.debug_msg(),
        )


@strict_dataclass
@dataclass(frozen=True, kw_only=True)
class AggregatableOutput(BaseOutput):
    pass


@strict_dataclass
@dataclass(frozen=True, kw_only=True)
class FlowTestOutput(AggregatableOutput):
    tft_metadata: TestMetadata
    command: str
    result: dict[str, Any]
    bitrate_gbps: Bitrate
    eval_result: Optional[EvalResult] = None

    def clone(
        self,
        *,
        eval_result: common._MISSING_TYPE | Optional[EvalResult] = common.MISSING,
    ) -> "FlowTestOutput":
        if isinstance(eval_result, common._MISSING_TYPE):
            eval_result = self.eval_result
        result: FlowTestOutput = dataclasses.replace(self, eval_result=eval_result)
        return result

    @property
    def eval_msg(self) -> Optional[str]:
        if not self.success:
            if self.msg is not None:
                return self.msg
        elif self.eval_result is not None and not self.eval_result.success:
            if self.eval_result.msg is not None:
                return self.eval_result.msg
        else:
            return None
        return "unspecified failure"


@strict_dataclass
@dataclass(frozen=True, kw_only=True)
class PluginOutput(AggregatableOutput):
    command: str
    result: dict[str, Any]
    plugin_metadata: PluginMetadata

    @property
    def plugin(self) -> "Plugin":
        import pluginbase

        return pluginbase.get_by_name(self.plugin_metadata.plugin_name)

    def result_get(self, key: str, vtype: type[T]) -> T:
        return common.dict_get_typed(self.result, key, vtype)


@strict_dataclass
@dataclass(kw_only=True)
class TftResultBuilder:
    _flow_test: Optional[FlowTestOutput] = None
    _plugins: list[PluginOutput] = dataclasses.field(default_factory=list)

    def set_flow_test(self, flow_test: FlowTestOutput) -> None:
        if self._flow_test is not None:
            raise RuntimeError("Cannot set multiple FlowTestOutput results")
        self._flow_test = flow_test

    def add_plugin(self, plugin_output: PluginOutput) -> None:
        assert isinstance(plugin_output, PluginOutput)
        self._plugins.append(plugin_output)

    def build(self) -> "TftResult":
        if self._flow_test is None:
            raise RuntimeError("Failed to collect a FlowTestOutput")
        return TftResult(
            flow_test=self._flow_test,
            plugins=tuple(self._plugins),
        )


@strict_dataclass
@dataclass(frozen=True, kw_only=True)
class TftResult:
    """Aggregated output of a single tft run. A single run of a trafficFlowTests._run_tests() will
    pass a reference to an instance of TftResult to each task to which the task will append
    it's respective output. A list of this class will be the expected format of input provided to
    evaluator.py.

    Attributes:
        flow_test: an object of type FlowTestOutput containing the results of a flow test run
        plugins: a list of objects derivated from type PluginOutput for each optional plugin to append
        resulting output to."""

    flow_test: FlowTestOutput
    plugins: tuple[PluginOutput, ...]

    @property
    def eval_flow_test_success(self) -> bool:
        return self.flow_test.eval_success

    @property
    def eval_plugins_success(self) -> bool:
        return all(p.eval_success for p in self.plugins)

    @property
    def eval_all_success(self) -> bool:
        return self.eval_flow_test_success and self.eval_plugins_success


@strict_dataclass
@dataclass(frozen=True, kw_only=True)
class TftResults:
    lst: tuple[TftResult, ...]
    filename: Optional[str] = None

    TFT_TESTS: typing.ClassVar[str] = "tft-tests"

    def __iter__(self) -> typing.Iterator[TftResult]:
        return iter(self.lst)

    def __len__(self) -> int:
        return len(self.lst)

    @property
    def log_detail(self) -> str:
        if self.filename is None:
            return ""
        return f" in {repr(self.filename)}"

    def serialize(self) -> dict[str, Any]:
        return {
            TftResults.TFT_TESTS: [common.dataclass_to_dict(o) for o in self],
        }

    def serialize_to_file(
        self,
        file: str | Path | typing.IO[str],
    ) -> None:
        common.json_dump(self.serialize(), file)

    @staticmethod
    def parse(
        data: Any,
        *,
        filename: Optional[str | Path] = None,
    ) -> "TftResults":

        err = "data"
        if filename is not None:
            # The filename is only used for the error message.
            err = f"file {repr(str(filename))}"

        if not isinstance(data, dict):
            raise RuntimeError(f"{err} needs to contain a dictionary")

        if TftResults.TFT_TESTS not in data:
            raise RuntimeError(f'{err} needs a top level key "{TftResults.TFT_TESTS}"')

        k = list(data)
        k.remove(TftResults.TFT_TESTS)
        if k:
            raise RuntimeError(f'{err} has unknown top level key "{k[0]}"')

        data_tft_tests = data[TftResults.TFT_TESTS]

        if not isinstance(data_tft_tests, list):
            raise RuntimeError(
                f'{err} needs a list at top level key "{TftResults.TFT_TESTS}" but has {type(data)}'
            )

        lst: list[TftResult] = []
        for data_tft_test in data_tft_tests:
            try:
                result = common.dataclass_from_dict(TftResult, data_tft_test)
            except Exception as e:
                raise RuntimeError(f"{err} has invalid data: {e}")
            lst.append(result)

        for r_idx, result in enumerate(lst):
            for plugin_output in result.plugins:
                try:
                    plugin_output.plugin
                except ValueError:
                    raise RuntimeError(
                        f'{err} has invalid plugin name "{plugin_output.plugin_metadata.plugin_name}" in result #{r_idx}'
                    )

        return TftResults(
            lst=tuple(lst),
            filename=(str(filename) if filename is not None else None),
        )

    @staticmethod
    def parse_from_file(filename: str | Path) -> "TftResults":
        try:
            f = open(filename, "r")
        except Exception as e:
            raise RuntimeError(f"cannot load file {filename}: {e}")
        try:
            data = json.load(f)
        except Exception:
            raise RuntimeError(f"File {filename} does not contain valid JSON")
        finally:
            f.close()

        return TftResults.parse(data, filename=filename)

    def group_by_success(self) -> tuple["TftResults", "TftResults"]:

        group_success = [o for o in self if o.eval_all_success]
        group_fail = [o for o in self if not o.eval_all_success]

        def _key_fcn(o: TftResult) -> int:
            comp_val = 0
            if o.eval_flow_test_success:
                comp_val += 10
            if o.eval_plugins_success:
                comp_val += 1
            return comp_val

        group_fail.sort(key=_key_fcn)

        return (
            TftResults(lst=tuple(group_success), filename=self.filename),
            TftResults(lst=tuple(group_fail), filename=self.filename),
        )

    def get_pass_fail_status(self) -> "PassFailStatus":
        tft_passing = 0
        tft_failing = 0
        plugin_passing = 0
        plugin_failing = 0
        for result in self:
            if result.eval_flow_test_success:
                tft_passing += 1
            else:
                tft_failing += 1
            for plugin in result.plugins:
                if plugin.eval_success:
                    plugin_passing += 1
                else:
                    plugin_failing += 1

        return PassFailStatus(
            result=tft_failing + plugin_failing == 0,
            num_tft_passed=tft_passing,
            num_tft_failed=tft_failing,
            num_plugin_passed=plugin_passing,
            num_plugin_failed=plugin_failing,
        )


@strict_dataclass
@dataclass(frozen=True, kw_only=True)
class PassFailStatus:
    """Pass/Fail ratio and result from evaluating a full tft Flow Test result

    Attributes:
        result: boolean representing whether the test was successful (100% passing)
        num_passed: int number of test cases passed
        num_failed: int number of test cases failed"""

    result: bool
    num_tft_passed: int
    num_tft_failed: int
    num_plugin_passed: int
    num_plugin_failed: int

    def log(
        self,
    ) -> None:
        logger.info(f"RESULT: Success = {self.result}.")
        logger.info(
            f"  FlowTest results: Passed {self.num_tft_passed}/{self.num_tft_passed + self.num_tft_failed}"
        )
        logger.info(
            f"  Plugin results: Passed {self.num_plugin_passed}/{self.num_plugin_passed + self.num_plugin_failed}"
        )


@strict_dataclass
@dataclass(frozen=True, kw_only=True)
class TestCaseTypInfo:
    test_case_type: TestCaseType
    connection_mode: ConnectionMode
    is_same_node: bool
    is_server_hostbacked: bool
    is_client_hostbacked: bool

    @property
    def node_location(self) -> str:
        if self.is_same_node:
            return "same-node"
        return "diff-node"

    def get_server_pod_type(self, pod_type: PodType) -> PodType:
        if self.is_server_hostbacked:
            return PodType.HOSTBACKED
        if pod_type == PodType.SRIOV:
            return PodType.SRIOV
        return PodType.NORMAL

    def get_client_pod_type(self, pod_type: PodType) -> PodType:
        if self.is_client_hostbacked:
            return PodType.HOSTBACKED
        if pod_type == PodType.SRIOV:
            return PodType.SRIOV
        return PodType.NORMAL


_test_case_typ_infos = {
    ti.test_case_type: ti
    for ti in (
        TestCaseTypInfo(
            test_case_type=TestCaseType.POD_TO_POD_SAME_NODE,
            connection_mode=ConnectionMode.POD_IP,
            is_same_node=True,
            is_server_hostbacked=False,
            is_client_hostbacked=False,
        ),
        TestCaseTypInfo(
            test_case_type=TestCaseType.POD_TO_POD_DIFF_NODE,
            connection_mode=ConnectionMode.POD_IP,
            is_same_node=False,
            is_server_hostbacked=False,
            is_client_hostbacked=False,
        ),
        TestCaseTypInfo(
            test_case_type=TestCaseType.POD_TO_HOST_SAME_NODE,
            connection_mode=ConnectionMode.POD_IP,
            is_same_node=True,
            is_server_hostbacked=True,
            is_client_hostbacked=False,
        ),
        TestCaseTypInfo(
            test_case_type=TestCaseType.POD_TO_HOST_DIFF_NODE,
            connection_mode=ConnectionMode.POD_IP,
            is_same_node=False,
            is_server_hostbacked=True,
            is_client_hostbacked=False,
        ),
        TestCaseTypInfo(
            test_case_type=TestCaseType.POD_TO_CLUSTER_IP_TO_POD_SAME_NODE,
            connection_mode=ConnectionMode.CLUSTER_IP,
            is_same_node=True,
            is_server_hostbacked=False,
            is_client_hostbacked=False,
        ),
        TestCaseTypInfo(
            test_case_type=TestCaseType.POD_TO_CLUSTER_IP_TO_POD_DIFF_NODE,
            connection_mode=ConnectionMode.CLUSTER_IP,
            is_same_node=False,
            is_server_hostbacked=False,
            is_client_hostbacked=False,
        ),
        TestCaseTypInfo(
            test_case_type=TestCaseType.POD_TO_CLUSTER_IP_TO_HOST_SAME_NODE,
            connection_mode=ConnectionMode.CLUSTER_IP,
            is_same_node=True,
            is_server_hostbacked=True,
            is_client_hostbacked=False,
        ),
        TestCaseTypInfo(
            test_case_type=TestCaseType.POD_TO_CLUSTER_IP_TO_HOST_DIFF_NODE,
            connection_mode=ConnectionMode.CLUSTER_IP,
            is_same_node=False,
            is_server_hostbacked=True,
            is_client_hostbacked=False,
        ),
        TestCaseTypInfo(
            test_case_type=TestCaseType.POD_TO_NODE_PORT_TO_POD_SAME_NODE,
            connection_mode=ConnectionMode.NODE_PORT_IP,
            is_same_node=True,
            is_server_hostbacked=False,
            is_client_hostbacked=False,
        ),
        TestCaseTypInfo(
            test_case_type=TestCaseType.POD_TO_NODE_PORT_TO_POD_DIFF_NODE,
            connection_mode=ConnectionMode.NODE_PORT_IP,
            is_same_node=False,
            is_server_hostbacked=False,
            is_client_hostbacked=False,
        ),
        TestCaseTypInfo(
            test_case_type=TestCaseType.HOST_TO_POD_SAME_NODE,
            connection_mode=ConnectionMode.POD_IP,
            is_same_node=True,
            is_server_hostbacked=False,
            is_client_hostbacked=True,
        ),
        TestCaseTypInfo(
            test_case_type=TestCaseType.HOST_TO_POD_DIFF_NODE,
            connection_mode=ConnectionMode.POD_IP,
            is_same_node=False,
            is_server_hostbacked=False,
            is_client_hostbacked=True,
        ),
        TestCaseTypInfo(
            test_case_type=TestCaseType.HOST_TO_CLUSTER_IP_TO_POD_SAME_NODE,
            connection_mode=ConnectionMode.CLUSTER_IP,
            is_same_node=True,
            is_server_hostbacked=False,
            is_client_hostbacked=True,
        ),
        TestCaseTypInfo(
            test_case_type=TestCaseType.HOST_TO_CLUSTER_IP_TO_POD_DIFF_NODE,
            connection_mode=ConnectionMode.CLUSTER_IP,
            is_same_node=False,
            is_server_hostbacked=False,
            is_client_hostbacked=True,
        ),
        TestCaseTypInfo(
            test_case_type=TestCaseType.HOST_TO_CLUSTER_IP_TO_HOST_SAME_NODE,
            connection_mode=ConnectionMode.CLUSTER_IP,
            is_same_node=True,
            is_server_hostbacked=True,
            is_client_hostbacked=True,
        ),
        TestCaseTypInfo(
            test_case_type=TestCaseType.HOST_TO_CLUSTER_IP_TO_HOST_DIFF_NODE,
            connection_mode=ConnectionMode.CLUSTER_IP,
            is_same_node=False,
            is_server_hostbacked=True,
            is_client_hostbacked=True,
        ),
        TestCaseTypInfo(
            test_case_type=TestCaseType.HOST_TO_NODE_PORT_TO_POD_SAME_NODE,
            connection_mode=ConnectionMode.NODE_PORT_IP,
            is_same_node=True,
            is_server_hostbacked=False,
            is_client_hostbacked=True,
        ),
        TestCaseTypInfo(
            test_case_type=TestCaseType.HOST_TO_NODE_PORT_TO_POD_DIFF_NODE,
            connection_mode=ConnectionMode.NODE_PORT_IP,
            is_same_node=False,
            is_server_hostbacked=False,
            is_client_hostbacked=True,
        ),
        TestCaseTypInfo(
            test_case_type=TestCaseType.HOST_TO_NODE_PORT_TO_HOST_SAME_NODE,
            connection_mode=ConnectionMode.NODE_PORT_IP,
            is_same_node=True,
            is_server_hostbacked=True,
            is_client_hostbacked=True,
        ),
        TestCaseTypInfo(
            test_case_type=TestCaseType.HOST_TO_NODE_PORT_TO_HOST_DIFF_NODE,
            connection_mode=ConnectionMode.NODE_PORT_IP,
            is_same_node=False,
            is_server_hostbacked=True,
            is_client_hostbacked=True,
        ),
        TestCaseTypInfo(
            test_case_type=TestCaseType.POD_TO_EXTERNAL,
            connection_mode=ConnectionMode.EXTERNAL_IP,
            is_same_node=False,
            is_server_hostbacked=False,
            is_client_hostbacked=True,
        ),
        TestCaseTypInfo(
            test_case_type=TestCaseType.POD_TO_POD_2ND_INTERFACE_SAME_NODE,
            connection_mode=ConnectionMode.MULTI_HOME,
            is_same_node=True,
            is_server_hostbacked=False,
            is_client_hostbacked=False,
        ),
        TestCaseTypInfo(
            test_case_type=TestCaseType.POD_TO_POD_2ND_INTERFACE_DIFF_NODE,
            connection_mode=ConnectionMode.MULTI_HOME,
            is_same_node=False,
            is_server_hostbacked=False,
            is_client_hostbacked=False,
        ),
        TestCaseTypInfo(
            test_case_type=TestCaseType.POD_TO_POD_MULTI_NETWORK_POLICY,
            connection_mode=ConnectionMode.MULTI_NETWORK,
            is_same_node=False,
            is_server_hostbacked=False,
            is_client_hostbacked=False,
        ),
    )
}


if typing.TYPE_CHECKING:
    from pluginbase import Plugin
