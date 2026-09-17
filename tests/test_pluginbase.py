# SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
# SPDX-License-Identifier: Apache-2.0

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pluginbase  # noqa: E402


def test_plugin_get() -> None:
    for plugin in pluginbase.get_all():
        assert plugin.PLUGIN_NAME
        assert plugin is pluginbase.get_by_name(plugin.PLUGIN_NAME)
