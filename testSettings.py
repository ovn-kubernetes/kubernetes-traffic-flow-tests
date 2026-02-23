import dataclasses
import threading
import typing

from ktoolbox import common

import testConfig
import tftbase

from tftbase import PodInfo
from tftbase import TestMetadata


@common.strict_dataclass
@dataclasses.dataclass(frozen=True, kw_only=True)
class TestSettings:
    """TestSettings will handle determining the logic require to configure the client/server for a given test"""

    cfg_descr: testConfig.ConfigDescriptor
    instance_index: int
    reverse: bool

    event_server_alive: threading.Event = dataclasses.field(
        init=False, default_factory=threading.Event
    )

    event_client_finished: threading.Event = dataclasses.field(
        init=False, default_factory=threading.Event
    )

    def _post_check(self) -> None:
        # As threading.Lock is not a regular type, @strict_dataclass
        # cannot handle fields of it. Set the attribute here.
        self._lock: threading.Lock
        object.__setattr__(self, "_lock", threading.Lock())

        # Access some properties here. We would get an exception early on, if
        # there is something wrong.
        self.connection
        self.test_case_id.info
        self._node_server
        self.node_client

    @property
    def _node_server(self) -> testConfig.ConfNodeServer:
        # For now, only one server/client is supported and TestConfig already
        # enforces that. Do tuple-unpacking here, to further assert that there
        # is only one server/client.
        (c_server,) = self.connection.server
        return c_server

    @property
    def node_client(self) -> testConfig.ConfNodeClient:
        # For now, only one server/client is supported and TestConfig already
        # enforces that. Do tuple-unpacking here, to further assert that there
        # is only one server/client.
        (c_client,) = self.connection.client
        return c_client

    @property
    def node_server(self) -> testConfig.ConfNodeBase:
        if self.test_case_id.info.is_same_node:
            return self.node_client
        else:
            return self._node_server

    @property
    def clmo_barrier(self) -> threading.Barrier:
        with self._lock:
            b = getattr(self, "_clmo_barrier", None)
            if b is None:
                raise RuntimeError(
                    "Cannot access the client-monitor barrier before calling initialize_clmo_barrier()"
                )
            return typing.cast(threading.Barrier, b)

    def initialize_clmo_barrier(self, parties: int) -> None:
        with self._lock:
            if hasattr(self, "_clmo_barrier"):
                raise RuntimeError("initialize_clmo_barrier() can only be called once")

            b = threading.Barrier(parties=parties)

            # TestSettings is for the most part an immutable, frozen object.
            # Here we lie about it. We do initialize the _clmo_barrier only
            # during initialize_clmo_barrier().
            #
            # Note that clmo_barrier will raise an exception if called before
            # initializing it. So you will only ever see one instance of the
            # barrier that never changes. That almost counts as "immutable".
            object.__setattr__(self, "_clmo_barrier", b)

    @property
    def connection(self) -> testConfig.ConfConnection:
        return self.cfg_descr.get_connection()

    @property
    def has_egress_ip(self) -> bool:
        """Returns True if egress_ip is configured for this connection."""
        return self.connection.has_egress_ip

    @property
    def egress_ip(self) -> typing.Optional[str]:
        """Returns the configured egress IP, or None if not configured."""
        return self.connection.egress_ip.ip if self.connection.egress_ip else None

    @property
    def egress_node(self) -> str:
        """Returns the effective egress node (configured or default to client node)."""
        return self.connection.get_effective_egress_node(self.node_client.name)

    @property
    def external_server_ip(self) -> typing.Optional[str]:
        """Returns the configured external server IP, or None if not configured."""
        return self.connection.external_server_ip

    @property
    def test_case_id(self) -> tftbase.TestCaseType:
        return self.cfg_descr.get_test_case()

    @property
    def server_is_tenant(self) -> bool:
        # TODO: Handle Case when not tenant
        return True

    @property
    def client_is_tenant(self) -> bool:
        # TODO: Handle Case when not tenant
        return True

    @property
    def server_index(self) -> int:
        # TODO: Add task indexing
        return self.instance_index

    @property
    def client_index(self) -> int:
        # TODO: Add task indexing
        return self.instance_index

    @property
    def server_pod_type(self) -> tftbase.PodType:
        return self.test_case_id.info.get_server_pod_type(self.node_server.pod_type)

    @property
    def client_pod_type(self) -> tftbase.PodType:
        return self.test_case_id.info.get_client_pod_type(self.node_client.pod_type)

    @property
    def connection_mode(self) -> tftbase.ConnectionMode:
        return self.test_case_id.info.connection_mode

    def get_test_info(self) -> str:
        return f"""type={self.connection.test_type.name}, test-case={self.test_case_id.name}: {self.client_pod_type.name} pod to {self.connection_mode.name} to {self.server_pod_type.name} pod - {self.test_case_id.info.node_location}
        Client Node: {self.node_client.name}
            Tenant={self.client_is_tenant}
            Index={self.client_index}
        Server Node: {self.node_server.name}
            Exec Persistence: {self.node_server.is_persistent_server}
            Tenant={self.server_is_tenant}
            Index={self.server_index}"""

    def get_test_str(self) -> str:
        direction = ""
        if self.reverse:
            direction = "-REV"
        return f"{self.test_case_id.name}-{self.client_pod_type.name}_TO_{self.connection_mode.name}_TO_{self.server_pod_type.name}-{self.test_case_id.info.node_location}{direction}"

    def get_test_metadata(self) -> TestMetadata:
        return TestMetadata(
            tft_idx=self.cfg_descr.tft_idx,
            test_cases_idx=self.cfg_descr.test_cases_idx,
            connections_idx=self.cfg_descr.connections_idx,
            test_case_id=self.test_case_id,
            test_type=self.connection.test_type,
            reverse=self.reverse,
            server=PodInfo(
                name=self.node_server.name,
                pod_type=self.server_pod_type,
                is_tenant=self.server_is_tenant,
                index=self.client_index,
            ),
            client=PodInfo(
                name=self.node_client.name,
                pod_type=self.client_pod_type,
                is_tenant=self.client_is_tenant,
                index=self.client_index,
            ),
        )
