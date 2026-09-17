# SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
# SPDX-License-Identifier: Apache-2.0

import shlex
import typing

import pluginbase

from pluginValidateOffload import KEY_NAMES
from pluginValidateOffload import PluginValidateOffload


class PluginOvsDocaValidateOffload(PluginValidateOffload):
    PLUGIN_NAME = "ovs_doca_validate_offload"
    STATISTICS_BACKEND = "ovs"

    def statistics_command(self, vf_rep: str) -> str:
        return (
            "chroot /host ovs-vsctl get Interface "
            f"{shlex.quote(vf_rep)} statistics:sw_rx_packets "
            "statistics:tx_packets"
        )

    def statistics_get_startend(
        self,
        parsed_data: dict[str, int],
        output: str,
        suffix: typing.Literal["start", "end"],
    ) -> bool:
        values = output.splitlines()
        if len(values) != 2:
            return False
        try:
            rx_packets, tx_packets = (int(value.strip()) for value in values)
        except ValueError:
            return False
        parsed_data[KEY_NAMES[suffix]["rx"]] = rx_packets
        parsed_data[KEY_NAMES[suffix]["tx"]] = tx_packets
        return True


plugin = pluginbase.register_plugin(PluginOvsDocaValidateOffload())
