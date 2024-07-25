import json
import kubernetes  # type: ignore
import logging
import os
import random
import shlex
import sys
import typing
import yaml

from collections.abc import Iterable

import host

from logger import logger


class K8sClient:
    def __init__(self, kubeconfig: typing.Optional[str] = None):
        if kubeconfig is None:
            kubeconfig = os.getenv("KUBECONFIG")
            if not kubeconfig:
                raise RuntimeError(
                    "KUBECONFIG environment variable not set and no kubeconfig argument specified"
                )
        if not os.path.exists(kubeconfig):
            raise RuntimeError(
                f"KUBECONFIG={shlex.quote(kubeconfig)} file does not exist"
            )
        self._kc = kubeconfig
        with open(kubeconfig) as f:
            c = yaml.safe_load(f)
        self._api_client = kubernetes.config.new_client_from_config_dict(c)
        self._client = kubernetes.client.CoreV1Api(self._api_client)

    def get_nodes(
        self,
    ) -> list[str]:
        return [e.metadata.name for e in self._client.list_node().items]

    def get_nodes_with_label(self, label_selector: str) -> list[str]:
        return [
            e.metadata.name
            for e in self._client.list_node(label_selector=label_selector).items
        ]

    @staticmethod
    def _oc_get_cmd(cmd: str | Iterable[str]) -> list[str]:
        if isinstance(cmd, str):
            return shlex.split(cmd)
        return list(cmd)

    def _oc_get_full_cmd(
        self,
        *,
        cmd: str | Iterable[str],
        namespace: typing.Optional[str] = None,
    ) -> list[str]:
        namespace_args: tuple[str, ...]
        if namespace:
            namespace_args = ("-n", namespace)
        else:
            namespace_args = ()
        return [
            "kubectl",
            "--kubeconfig",
            self._kc,
            *namespace_args,
            *self._oc_get_cmd(cmd),
        ]

    def oc(
        self,
        cmd: str | Iterable[str],
        *,
        may_fail: bool = False,
        die_on_error: bool = False,
        namespace: typing.Optional[str] = None,
    ) -> host.Result:
        return host.local.run(
            self._oc_get_full_cmd(cmd=cmd, namespace=namespace),
            die_on_error=die_on_error,
            log_level_fail=logging.DEBUG if may_fail else logging.ERROR,
        )

    def oc_exec(
        self,
        cmd: str | Iterable[str],
        *,
        pod_name: str,
        may_fail: bool = False,
        die_on_error: bool = False,
        namespace: typing.Optional[str] = None,
    ) -> host.Result:
        return self.oc(
            ["exec", pod_name, "--", *self._oc_get_cmd(cmd)],
            may_fail=may_fail,
            die_on_error=die_on_error,
            namespace=namespace,
        )

    def oc_get(
        self,
        what: str,
        *,
        may_fail: bool = False,
        die_on_error: bool = False,
        namespace: typing.Optional[str] = None,
    ) -> typing.Optional[dict[str, typing.Any]]:
        cmd = ["get", what, "-o", "json"]
        ret = self.oc(
            cmd,
            may_fail=may_fail,
            die_on_error=die_on_error,
            namespace=namespace,
        )

        if not ret.success:
            # No need for extra logging in this failure case. self.oc() already
            # did all the logging we want.
            return None

        try:
            data = json.loads(ret.out)
        except ValueError:
            data = None

        if not isinstance(data, dict):
            cmd_s = shlex.join(self._oc_get_full_cmd(cmd=cmd, namespace=namespace))
            if not may_fail or die_on_error:
                logger.error(
                    f"Command {cmd_s} did not return a JSON dictionary but {ret.debug_str()}"
                )
            else:
                logger.debug(f"Command {cmd_s} did not return a JSON dictionary")
            if die_on_error:
                sys.exit(-1)
            return None

        return data

    def oc_debug(
        self,
        cmd: str | Iterable[str],
        *,
        node_name: str,
        test_image: str,
        namespace: str,
        may_fail: bool = False,
        die_on_error: bool = False,
    ) -> host.Result:
        container_name = f"ocp-tft-debug-{node_name}-{random.randint(0, 2**64-1):016x}"
        # We use `kubectl debug` and not `oc debug`. There are thus some differences.
        #
        # Optimally, we would use "--profile=sysadmin". But that is too new, so
        # we cannot use it and have no CAP_SYS_CHROOT.  That means, `cmd`
        # cannot be `chroot /host crictl ...` but needs to be
        # `/host/usr/bin/crictl --runtime-endpoint=unix:///host/run/crio/crio.sock ...`.
        #
        # Also, unlike `oc debug`, we need an "--image", which the caller must specify.
        #
        # Also, we must specify a namespace. And the container will linger around afterwards,
        # so we need to delete it (below).
        result = self.oc(
            [
                "debug",
                "-q",
                "-ti",
                "--profile=general",
                f"--container={container_name}",
                f"--image={test_image}",
                f"node/{node_name}",
                "--",
                *self._oc_get_cmd(cmd),
            ],
            may_fail=may_fail,
            die_on_error=die_on_error,
            namespace=namespace,
        )

        # We have to find and delete the pods we just created. As we used
        # a unique {container_name}, we can search for that.
        pod_names = []
        pdict = self.oc_get(
            "pods",
            may_fail=True,
            namespace=namespace,
        )
        pdict_items = []
        if pdict is not None:
            try:
                pdict_items = list(pdict["items"])
            except Exception:
                pass
        for pdict_item in pdict_items:
            try:
                for c in pdict_item["spec"]["containers"]:
                    if c["name"] == container_name:
                        pod_names.append(pdict_item["metadata"]["name"])
                        break
            except Exception:
                pass
        for pod_name in pod_names:
            self.oc(
                ["delete", f"pod/{pod_name}"],
                may_fail=True,
                namespace=namespace,
            )

        return result
