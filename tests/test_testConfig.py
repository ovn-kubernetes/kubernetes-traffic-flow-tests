import json
import os
import pathlib
import pytest
import re
import sys
import yaml

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from ktoolbox import common  # noqa: E402

import testConfig  # noqa: E402

from tftbase import TestCaseType  # noqa: E402
from tftbase import TestType  # noqa: E402

testConfigKubeconfigsArgs1 = ("/root/kubeconfig.x1", None)


def test_parse_test_cases() -> None:
    def _t(input_str: str) -> list[TestCaseType]:
        return common.enum_convert_list(TestCaseType, input_str)

    assert _t("1,2,3,6") == [
        TestCaseType.POD_TO_POD_SAME_NODE,
        TestCaseType.POD_TO_POD_DIFF_NODE,
        TestCaseType.POD_TO_HOST_SAME_NODE,
        TestCaseType.POD_TO_CLUSTER_IP_TO_POD_DIFF_NODE,
    ]
    assert _t("1,2,POD_TO_HOST_SAME_NODE,6") == [
        TestCaseType.POD_TO_POD_SAME_NODE,
        TestCaseType.POD_TO_POD_DIFF_NODE,
        TestCaseType.POD_TO_HOST_SAME_NODE,
        TestCaseType.POD_TO_CLUSTER_IP_TO_POD_DIFF_NODE,
    ]

    assert _t("1-9,15-19") == [
        TestCaseType.POD_TO_POD_SAME_NODE,
        TestCaseType.POD_TO_POD_DIFF_NODE,
        TestCaseType.POD_TO_HOST_SAME_NODE,
        TestCaseType.POD_TO_HOST_DIFF_NODE,
        TestCaseType.POD_TO_CLUSTER_IP_TO_POD_SAME_NODE,
        TestCaseType.POD_TO_CLUSTER_IP_TO_POD_DIFF_NODE,
        TestCaseType.POD_TO_CLUSTER_IP_TO_HOST_SAME_NODE,
        TestCaseType.POD_TO_CLUSTER_IP_TO_HOST_DIFF_NODE,
        TestCaseType.POD_TO_NODE_PORT_TO_POD_SAME_NODE,
        TestCaseType.HOST_TO_POD_SAME_NODE,
        TestCaseType.HOST_TO_POD_DIFF_NODE,
        TestCaseType.HOST_TO_CLUSTER_IP_TO_POD_SAME_NODE,
        TestCaseType.HOST_TO_CLUSTER_IP_TO_POD_DIFF_NODE,
        TestCaseType.HOST_TO_CLUSTER_IP_TO_HOST_SAME_NODE,
    ]
    assert _t("POD_TO_POD_SAME_NODE-9,15-19") == [
        TestCaseType.POD_TO_POD_SAME_NODE,
        TestCaseType.POD_TO_POD_DIFF_NODE,
        TestCaseType.POD_TO_HOST_SAME_NODE,
        TestCaseType.POD_TO_HOST_DIFF_NODE,
        TestCaseType.POD_TO_CLUSTER_IP_TO_POD_SAME_NODE,
        TestCaseType.POD_TO_CLUSTER_IP_TO_POD_DIFF_NODE,
        TestCaseType.POD_TO_CLUSTER_IP_TO_HOST_SAME_NODE,
        TestCaseType.POD_TO_CLUSTER_IP_TO_HOST_DIFF_NODE,
        TestCaseType.POD_TO_NODE_PORT_TO_POD_SAME_NODE,
        TestCaseType.HOST_TO_POD_SAME_NODE,
        TestCaseType.HOST_TO_POD_DIFF_NODE,
        TestCaseType.HOST_TO_CLUSTER_IP_TO_POD_SAME_NODE,
        TestCaseType.HOST_TO_CLUSTER_IP_TO_POD_DIFF_NODE,
        TestCaseType.HOST_TO_CLUSTER_IP_TO_HOST_SAME_NODE,
    ]

    all_cases = sorted(TestCaseType, key=lambda e: e.value)

    assert _t("*") == all_cases

    assert _t("2,*") == [
        TestCaseType.POD_TO_POD_DIFF_NODE,
        *all_cases,
    ]


def test_validate_test_type() -> None:
    def _t(input_str: str) -> TestType:
        return common.enum_convert(TestType, input_str, default=TestType.IPERF_TCP)

    assert _t("iperf-tcp") == TestType.IPERF_TCP
    assert _t("iperf-udp") == TestType.IPERF_UDP
    assert _t("IPERF_UDP") == TestType.IPERF_UDP
    assert _t("http ") == TestType.HTTP
    assert _t("HTTP") == TestType.HTTP


def _check_testConfig(tc: testConfig.TestConfig) -> None:

    assert isinstance(tc, testConfig.TestConfig)

    jdata = tc.config.serialize()

    tc2 = testConfig.TestConfig(
        full_config=jdata, kubeconfigs=testConfigKubeconfigsArgs1
    )

    assert isinstance(tc, testConfig.TestConfig)

    assert tc.config == tc2.config
    assert jdata == tc2.config.serialize()
    assert json.dumps(jdata) == tc2.config.serialize_json()


def test_config1() -> None:
    file = os.path.join(os.path.dirname(__file__), "..", "config.yaml")
    assert os.path.exists(file)

    tc = testConfig.TestConfig(config_path=file, kubeconfigs=testConfigKubeconfigsArgs1)
    assert isinstance(tc, testConfig.TestConfig)
    assert isinstance(tc.full_config, dict)
    assert list(tc.full_config.keys()) == ["tft", "kubeconfig", "kubeconfig_infra"]

    assert tc.config.kubeconfig is None
    assert tc.kubeconfig is not None
    assert tc.kubeconfig == testConfigKubeconfigsArgs1[0]

    assert tc.config.tft[0].name == "Test 1"
    assert tc.config.tft[0].connections[0].name == "Connection_1"

    assert tc.config.yamlpath == ""
    assert tc.config.tft[0].yamlpath == ".tft[0]"
    assert tc.config.tft[0].connections[0].yamlpath == ".tft[0].connections[0]"
    assert (
        tc.config.tft[0].connections[0].server[0].yamlpath
        == ".tft[0].connections[0].server[0]"
    )
    assert (
        tc.config.tft[0].connections[0].plugins[0].yamlpath
        == ".tft[0].connections[0].plugins[0]"
    )

    server0 = tc.config.tft[0].connections[0].server[0]
    assert server0.connection.tft.config.test_config is tc

    assert server0.connection is tc.config.tft[0].connections[0].plugins[0].connection
    assert server0.connection is tc.config.tft[0].connections[0].client[0].connection

    _check_testConfig(tc)


def test_config2() -> None:
    full_config_str = """
tft:
  - name: "Test 1"
    namespace: "default"
    test_cases:
      - "1"
      - 2
      - HOST_TO_POD_DIFF_NODE
      - HOST_TO_CLUSTER_IP_TO_POD_SAME_NODE - HOST_TO_CLUSTER_IP_TO_HOST_SAME_NODE
    privileged_pod: True
    connections:
     - name: con1
       plugins:
         - name: measure_cpu
         - measure_power
     - name: con2
       type: simple
       client:
         - name: client1.example.com
           args: "foo '-x x'"
           privileged_pod: False
           default-network: default--n
       server:
         - name: s1
           args:
             - "hi x"
kubeconfig: /path/to/kubeconfig
kubeconfig_infra: /path/to/kubeconfig_infra
"""
    full_config = yaml.safe_load(full_config_str)
    tc = testConfig.TestConfig(
        full_config=full_config,
        kubeconfigs=None,
        output_base="/tmp/",
    )
    assert isinstance(tc, testConfig.TestConfig)

    assert tc.config.kubeconfig == "/path/to/kubeconfig"
    assert tc.config.kubeconfig_infra == "/path/to/kubeconfig_infra"
    assert tc._kubeconfig_pair == (tc.config.kubeconfig, tc.config.kubeconfig_infra)
    assert tc.kubeconfig == tc.config.kubeconfig
    assert tc.kubeconfig_infra == tc.config.kubeconfig_infra

    assert tc.config.tft[0].test_cases == (
        TestCaseType(1),
        TestCaseType(2),
        TestCaseType.HOST_TO_POD_DIFF_NODE,
        TestCaseType.HOST_TO_CLUSTER_IP_TO_POD_SAME_NODE,
        TestCaseType.HOST_TO_CLUSTER_IP_TO_POD_DIFF_NODE,
        TestCaseType.HOST_TO_CLUSTER_IP_TO_HOST_SAME_NODE,
    )
    assert tc.config.tft[0].connections[1].name == "con2"
    assert tc.config.tft[0].connections[1].client[0].name == "client1.example.com"
    assert tc.config.tft[0].connections[0].test_type == TestType.IPERF_TCP
    assert tc.config.tft[0].connections[0].plugins[0].name == "measure_cpu"
    assert (
        tc.config.tft[0].connections[0].plugins[0].plugin.PLUGIN_NAME == "measure_cpu"
    )
    assert tc.config.tft[0].connections[0].plugins[1].name == "measure_power"

    assert tc.config.tft[0].connections[1].test_type == TestType.SIMPLE
    assert tc.config.tft[0].connections[1].client[0].privileged_pod is False
    assert tc.config.tft[0].connections[1].client[0].args == ("foo", "-x x")
    assert tc.config.tft[0].connections[1].client[0].default_network == "default--n"
    assert tc.config.tft[0].connections[1].server[0].args == ("hi x",)
    assert tc.config.tft[0].connections[1].server[0].privileged_pod is None
    assert tc.config.tft[0].get_output_file() == pathlib.Path("/tmp/result-000.json")
    assert tc.config.tft[0].privileged_pod is True

    _check_testConfig(tc)

    cfg_descr1 = testConfig.ConfigDescriptor(tc)
    t: list[TestCaseType] = []
    for cfg_descr2 in cfg_descr1.describe_all_tft():
        for cfg_descr3 in cfg_descr2.describe_all_test_cases():
            t.append(cfg_descr3.get_test_case())
    assert tc.config.tft[0].test_cases == tuple(t)

    # Fail with invalid node name (no valid DNS name).
    with pytest.raises(
        ValueError,
        match=re.escape(
            'invalid configuration: ".tft[0].connections[1].client[0].name": invalid string'
        ),
    ):
        testConfig.TestConfig(
            full_config=yaml.safe_load(
                full_config_str.replace(
                    "client1.example.com",
                    "client1.example..com",
                ),
            ),
            kubeconfigs=None,
            output_base="/tmp/",
        )

    # A minimal yaml.
    full_config = yaml.safe_load("""
tft:
  - connections:
    - {}
""")
    tc = testConfig.TestConfig(
        full_config=full_config,
        kubeconfigs=testConfigKubeconfigsArgs1,
        output_base="/tmp/result2-",
    )
    assert isinstance(tc, testConfig.TestConfig)

    assert tc.config.kubeconfig is None
    assert tc.config.kubeconfig_infra is None
    assert tc.kubeconfig == testConfigKubeconfigsArgs1[0]
    assert tc.kubeconfig_infra == testConfigKubeconfigsArgs1[1]

    assert tc.config.tft[0].name == "Test 1"
    assert tc.config.tft[0].namespace == "default"
    assert tc.config.tft[0].test_cases == tuple(
        common.enum_convert_list(TestCaseType, "*")
    )
    assert tc.config.tft[0].connections[0].name == "Connection Test 1/1"
    assert tc.config.tft[0].get_output_file() == pathlib.Path("/tmp/result2-000.json")

    _check_testConfig(tc)
