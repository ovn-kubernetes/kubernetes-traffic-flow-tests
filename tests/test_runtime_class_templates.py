# SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
# SPDX-License-Identifier: Apache-2.0

import os
import sys
from typing import Any
from typing import cast

import yaml

from ktoolbox import kjinja2

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import tftbase  # noqa: E402


def _render_pod_template(template_name: str, runtime_class_name: str) -> dict[str, Any]:
    template_args: dict[str, str | bool] = {
        "name_space": '"default"',
        "pod_name": '"test-pod"',
        "label_tft_tests": '"0"',
        "has_runtime_class_name": bool(runtime_class_name),
        "runtime_class_name": f'"{runtime_class_name}"',
        "node_name": '"worker-1"',
        "test_image": '"example.invalid/tft:test"',
        "command": '["/usr/bin/container-entry-point.sh"]',
        "args": "[]",
        "image_pull_policy": '"Never"',
        "has_resources": False,
        "has_resource_name": False,
        "privileged_pod": "false",
        "capabilities_pod": '{"add": []}',
        "secondary_network_nad": '"default/test-network"',
        "secondary_network_nads": '"default/test-network"',
        "use_secondary_network": True,
        "default_network": '"default/default"',
    }
    rendered = kjinja2.render_file(
        tftbase.tftfile("manifests", template_name),
        template_args,
    )
    data = yaml.safe_load(rendered)
    assert isinstance(data, dict)
    return cast(dict[str, Any], data)


def test_runtime_class_rendered_for_eligible_traffic_pods() -> None:
    for template_name in (
        "pod.yaml.j2",
        "pod-secondary-network.yaml.j2",
        "sriov-pod.yaml.j2",
    ):
        pod = _render_pod_template(template_name, "kata")
        assert pod["spec"]["runtimeClassName"] == "kata"


def test_runtime_class_omitted_when_unset() -> None:
    for template_name in (
        "pod.yaml.j2",
        "pod-secondary-network.yaml.j2",
        "sriov-pod.yaml.j2",
    ):
        pod = _render_pod_template(template_name, "")
        assert "runtimeClassName" not in pod["spec"]


def test_runtime_class_not_applied_to_host_or_plugin_pods() -> None:
    for template_name in ("host-pod.yaml.j2", "tools-pod.yaml.j2"):
        pod = _render_pod_template(template_name, "kata")
        assert "runtimeClassName" not in pod["spec"]
