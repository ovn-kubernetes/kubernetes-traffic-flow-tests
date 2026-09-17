# SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
# SPDX-License-Identifier: Apache-2.0

import os
import sys
from unittest import mock

import pytest
import yaml

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import testConfig  # noqa: E402

TEST_KUBECONFIGS = ("/root/kubeconfig.x1", None)


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
