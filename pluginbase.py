import threading
import typing

from abc import ABC
from abc import abstractmethod

from ktoolbox import common

from tftbase import PluginOutput
from tftbase import TestMetadata


logger = common.ExtendedLogger("tft." + __name__)


class Plugin(ABC):
    PLUGIN_NAME = ""

    @property
    def log_name(self) -> str:
        return f"plugin[{self.PLUGIN_NAME}"

    def enable(
        self,
        *,
        ts: "TestSettings",
        perf_server: "ServerTask",
        perf_client: "ClientTask",
        tenant: bool,
    ) -> list["PluginTask"]:
        tasks = self._enable(
            ts=ts,
            perf_server=perf_server,
            perf_client=perf_client,
            tenant=tenant,
        )
        logger.debug(f"{self.log_name}: enable ({len(tasks)} tasks, {tasks})")
        return tasks

    @abstractmethod
    def _enable(
        self,
        *,
        ts: "TestSettings",
        perf_server: "ServerTask",
        perf_client: "ClientTask",
        tenant: bool,
    ) -> list["PluginTask"]:
        pass

    def eval_plugin_output(
        self,
        md: TestMetadata,
        plugin_output: PluginOutput,
    ) -> PluginOutput:
        if not plugin_output.eval_success:
            logger.error(
                f"{self.PLUGIN_NAME} plugin failed for {common.dataclass_to_json(md)}: {plugin_output.eval_msg}"
            )
        else:
            logger.debug(
                f"{self.PLUGIN_NAME} plugin succeded for {common.dataclass_to_json(md)}"
            )
        # Currently this doesn't really do anything additionally. We already evaluated
        # for success.
        return plugin_output


_plugin_registry_lock = threading.Lock()

_plugin_registry: dict[str, Plugin] = {}


def _get_plugin_registry() -> dict[str, Plugin]:
    # Plugins self-register via pluginbase.register_plugin()
    # when being imported. Ensure they are imported.
    import pluginMeasureCpu  # noqa: F401
    import pluginMeasurePower  # noqa: F401
    import pluginValidateOffload  # noqa: F401
    import pluginValidateBandwidth  # noqa: F401

    return _plugin_registry


def register_plugin(plugin: Plugin) -> Plugin:
    name = plugin.PLUGIN_NAME
    with _plugin_registry_lock:
        p2 = _plugin_registry.setdefault(name, plugin)
    if p2 is not plugin:
        raise ValueError(f"plugin {repr(name)} is already registered")
    return plugin


def get_all() -> list[Plugin]:
    registry = _get_plugin_registry()
    with _plugin_registry_lock:
        return [registry[k] for k in sorted(registry)]


def get_by_name(plugin_name: str) -> Plugin:
    registry = _get_plugin_registry()
    with _plugin_registry_lock:
        plugin = registry.get(plugin_name)
    if plugin is None:
        raise ValueError(f'Plugin "{plugin_name}" does not exist')
    return plugin


if typing.TYPE_CHECKING:
    # "pluginbase" cannot import modules like perf, task or testSettings, because
    # those modules import "pluginbase" in turn. However, to forward declare
    # type annotations, we do need those module here. Import them with
    # TYPE_CHECKING, but otherwise avoid the cyclic dependency between
    # modules.
    from task import ClientTask
    from task import ServerTask
    from task import PluginTask
    from testSettings import TestSettings
