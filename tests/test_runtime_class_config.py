# SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
# SPDX-License-Identifier: Apache-2.0

import os
import sys
from unittest import mock

import pytest
import yaml

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import task  # noqa: E402
import testConfig  # noqa: E402
from tftbase import PodType  # noqa: E402
from tftbase import TaskRole  # noqa: E402

TEST_KUBECONFIGS = ("/root/kubeconfig.x1", None)


class _RuntimeClassProbe(task.Task):
    def cmd_line_args(self, *, for_template: bool = False) -> list[str]:
        return []

    def _create_setup_operation_get_cancel_action_cmd(self) -> str:
        return ""


def _parse_config(config: str) -> testConfig.TestConfig:
    return testConfig.TestConfig(
        full_config=yaml.safe_load(config),
        kubeconfigs=TEST_KUBECONFIGS,
    )


def test_runtime_class_config() -> None:
    tc = _parse_config("""
tft:
  - runtime_class_name: kata-qemu.v1
    connections:
    - {}
""")
    tft = tc.config.tft[0]
    assert tft.runtime_class_name == "kata-qemu.v1"
    assert tft.serialize()["runtime_class_name"] == "kata-qemu.v1"

    reparsed = testConfig.TestConfig(
        full_config=tc.config.serialize(),
        kubeconfigs=TEST_KUBECONFIGS,
    )
    assert reparsed.config == tc.config


@pytest.mark.parametrize("runtime_class_name", ("Kata", "kata_runtime", "kata."))
def test_runtime_class_config_rejects_invalid_name(runtime_class_name: str) -> None:
    with pytest.raises(ValueError, match="runtime_class_name.*invalid string"):
        _parse_config(f"""
tft:
  - runtime_class_name: {runtime_class_name}
    connections:
    - {{}}
""")


def test_validate_runtime_classes() -> None:
    tc = _parse_config("""
tft:
  - runtime_class_name: kata
    connections:
    - {}
  - runtime_class_name: gvisor
    connections:
    - {}
  - runtime_class_name: kata
    connections:
    - {}
""")
    tenant = mock.Mock()
    tenant.oc_get.return_value = {}
    tc._client_tenant = tenant

    tc._validate_runtime_classes()

    assert tenant.oc_get.call_args_list == [
        mock.call("runtimeclass.node.k8s.io/gvisor", may_fail=True),
        mock.call("runtimeclass.node.k8s.io/kata", may_fail=True),
    ]


def test_validate_runtime_class_missing() -> None:
    tc = _parse_config("""
tft:
  - runtime_class_name: kata
    connections:
    - {}
""")
    tenant = mock.Mock()
    tenant.oc_get.return_value = None
    tc._client_tenant = tenant

    with pytest.raises(RuntimeError, match='RuntimeClass "kata" does not exist'):
        tc._validate_runtime_classes()


def test_runtime_class_per_node_config() -> None:
    tc = _parse_config("""
tft:
  - runtime_class_name: kata
    connections:
    - server:
        - name: worker-1
      client:
        - name: dpu-worker-2
          runtime_class_name: kata-coldplug
""")
    server_node = tc.config.tft[0].connections[0].server[0]
    client_node = tc.config.tft[0].connections[0].client[0]
    assert server_node.runtime_class_name is None
    assert client_node.runtime_class_name == "kata-coldplug"
    assert client_node.serialize()["runtime_class_name"] == "kata-coldplug"
    assert "runtime_class_name" not in server_node.serialize()

    reparsed = testConfig.TestConfig(
        full_config=tc.config.serialize(),
        kubeconfigs=TEST_KUBECONFIGS,
    )
    assert reparsed.config == tc.config

    connection = tc.config.tft[0].connections[0]
    ts = mock.Mock()
    ts.cfg_descr = mock.Mock()
    ts.cfg_descr.get_tft.return_value = tc.config.tft[0]
    ts.node_server = connection.server[0]
    ts.node_client = connection.client[0]

    server_task = _RuntimeClassProbe(
        ts=ts,
        index=0,
        tenant=True,
        task_role=TaskRole.SERVER,
    )
    server_task.pod_type = PodType.NORMAL
    client_task = _RuntimeClassProbe(
        ts=ts,
        index=0,
        tenant=True,
        task_role=TaskRole.CLIENT,
    )
    client_task.pod_type = PodType.NORMAL

    assert server_task._get_pod_runtime_class_name() == "kata"
    assert client_task._get_pod_runtime_class_name() == "kata-coldplug"


def test_validate_runtime_classes_includes_node_override() -> None:
    tc = _parse_config("""
tft:
  - connections:
    - client:
        - name: dpu-worker
          runtime_class_name: kata-coldplug
""")
    tenant = mock.Mock()
    tenant.oc_get.return_value = {}
    tc._client_tenant = tenant

    tc._validate_runtime_classes()

    assert tenant.oc_get.call_args_list == [
        mock.call("runtimeclass.node.k8s.io/kata-coldplug", may_fail=True),
    ]
