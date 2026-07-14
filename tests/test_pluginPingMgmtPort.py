import json
import os
import sys

import pytest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pluginPingMgmtPort  # noqa: E402


def test_mgmt_port_ip_from_node_subnets() -> None:
    assert (
        pluginPingMgmtPort.mgmt_port_ip_from_node_subnets(
            json.dumps({"default": ["10.131.0.0/23"]})
        )
        == "10.131.0.2"
    )
    assert (
        pluginPingMgmtPort.mgmt_port_ip_from_node_subnets(
            json.dumps({"default": ["10.130.0.0/24"]})
        )
        == "10.130.0.2"
    )


def test_mgmt_port_ip_from_node_subnets_invalid() -> None:
    with pytest.raises(json.JSONDecodeError):
        pluginPingMgmtPort.mgmt_port_ip_from_node_subnets("not json")
    with pytest.raises(KeyError):
        pluginPingMgmtPort.mgmt_port_ip_from_node_subnets(
            json.dumps({"other": ["10.0.0.0/24"]})
        )
    with pytest.raises(IndexError):
        pluginPingMgmtPort.mgmt_port_ip_from_node_subnets(json.dumps({"default": []}))
    with pytest.raises(ValueError):
        pluginPingMgmtPort.mgmt_port_ip_from_node_subnets(
            json.dumps({"default": ["not-a-subnet"]})
        )
