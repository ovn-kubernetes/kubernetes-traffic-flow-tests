import json
import logging
import print_results
import task
import time as _time

from pathlib import Path

from ktoolbox import common
from ktoolbox import host
from ktoolbox import kjinja2

import testConfig
import tftbase

from evaluator import Evaluator
from task import Task
from testConfig import ConfigDescriptor
from testSettings import TestSettings
from tftbase import ConnectionMode
from tftbase import TftResult
from tftbase import TftResults

logger = common.ExtendedLogger("tft." + __name__)


class TrafficFlowTests:
    def __init__(self, *, no_cleanup: int = 0) -> None:
        self._no_cleanup = no_cleanup
        self._udn_ns: str | None = None
        self._udn_ns_created: bool = False
        self._udn_setup_done: bool = False

    def _configure_namespace(
        self, cfg_descr: ConfigDescriptor, *, namespace: str | None = None
    ) -> bool:
        if namespace is None:
            namespace = cfg_descr.get_tft().namespace
        client = cfg_descr.tc.client_tenant

        existing = client.oc_get(
            f"namespace/{namespace}", may_fail=True, namespace=None
        )
        if not existing:
            logger.info(f"Namespace {namespace} not found, creating it")
            client.oc(f"create ns {namespace}", die_on_error=True, namespace=None)

        logger.info(f"Configuring namespace {namespace}")
        client.oc(
            f"label ns --overwrite {namespace} pod-security.kubernetes.io/enforce=privileged \
                                        pod-security.kubernetes.io/enforce-version=v1.24 \
                                        security.openshift.io/scc.podSecurityLabelSync=false",
            die_on_error=True,
        )
        return not existing

    def _setup_udn(self, cfg_descr: ConfigDescriptor) -> None:
        tft = cfg_descr.get_tft()
        needs_primary = any(tc.is_udn_primary for tc in tft.test_cases)
        needs_secondary = any(tc.is_udn_secondary for tc in tft.test_cases)
        needs_localnet = any(tc.is_udn_localnet for tc in tft.test_cases)

        if not needs_primary and not needs_secondary and not needs_localnet:
            return

        udn_ns = f"{tft.namespace}-udn"
        client = cfg_descr.tc.client_tenant

        logger.info(f"Setting up UDN in namespace {udn_ns}")

        # Only annotate the UDN with a resource name if every connection agrees
        # on the same value. A missing resource_name in any connection means that
        # connection doesn't need SR-IOV for the UDN.
        resource_names = {c.resource_name for c in tft.connections}
        resource_name = (
            resource_names.pop()
            if len(resource_names) == 1 and None not in resource_names
            else None
        )

        _j = json.dumps

        self._udn_ns = udn_ns

        ns_data = client.oc_get(f"namespace/{udn_ns}", may_fail=True)
        if ns_data is not None:
            if needs_primary:
                labels = ns_data.get("metadata", {}).get("labels", {})
                if "k8s.ovn.org/primary-user-defined-network" not in labels:
                    raise RuntimeError(
                        f"Namespace {udn_ns} exists without the "
                        f"k8s.ovn.org/primary-user-defined-network label. "
                        f"Delete it and retry."
                    )
        else:
            ns_yaml = tftbase.get_manifest_renderpath("udn-namespace.yaml")
            with open(ns_yaml, "w") as f:
                f.write(
                    f"apiVersion: v1\nkind: Namespace\nmetadata:\n  name: {udn_ns}\n"
                )
                if needs_primary:
                    f.write("  labels:\n")
                    f.write('    k8s.ovn.org/primary-user-defined-network: ""\n')
            client.oc(f"apply -f {ns_yaml}", die_on_error=True)
            self._udn_ns_created = True

        if needs_primary:
            in_template = tftbase.get_manifest("udn-primary.yaml.j2")
            out_yaml = tftbase.get_manifest_renderpath("udn-primary.yaml")
            kjinja2.render_file(
                in_template,
                {
                    "has_resource_name": resource_name is not None,
                    "resource_name": resource_name or "",
                    "name_space": _j(udn_ns),
                    "primary_cidr": _j(tftbase.get_udn_primary_cidr()),
                },
                out_file=out_yaml,
            )
            logger.info(
                f'Generate Primary UDN Yaml "{out_yaml}" (from "{in_template}")'
            )
            client.oc(f"apply -f {out_yaml}", die_on_error=True)

        if needs_secondary:
            in_template = tftbase.get_manifest("udn-secondary.yaml.j2")
            out_yaml = tftbase.get_manifest_renderpath("udn-secondary.yaml")
            kjinja2.render_file(
                in_template,
                {
                    "has_resource_name": resource_name is not None,
                    "resource_name": resource_name or "",
                    "name_space": _j(udn_ns),
                    "secondary_cidr": _j(tftbase.get_udn_secondary_cidr()),
                },
                out_file=out_yaml,
            )
            logger.info(
                f'Generate Secondary UDN Yaml "{out_yaml}" (from "{in_template}")'
            )
            client.oc(f"apply -f {out_yaml}", die_on_error=True)

        if needs_localnet:
            in_template = tftbase.get_manifest("udn-localnet.yaml.j2")
            out_yaml = tftbase.get_manifest_renderpath("udn-localnet.yaml")
            kjinja2.render_file(
                in_template,
                {
                    "has_resource_name": resource_name is not None,
                    "resource_name": resource_name or "",
                    "name_space": _j(udn_ns),
                    "localnet_cidr": _j(tftbase.get_udn_localnet_cidr()),
                    "physical_network_name": _j(
                        tftbase.get_udn_localnet_physical_network()
                    ),
                },
                out_file=out_yaml,
            )
            logger.info(
                f'Generate Localnet CUDN Yaml "{out_yaml}" (from "{in_template}")'
            )
            client.oc(f"apply -f {out_yaml}", die_on_error=True, namespace=None)

        self._configure_namespace(cfg_descr, namespace=udn_ns)
        self._udn_setup_done = True

    def _setup_secondary_nad(self, cfg_descr: ConfigDescriptor) -> None:
        tft = cfg_descr.get_tft()
        if not any(
            tc.info.connection_mode
            in (
                tftbase.ConnectionMode.MULTI_HOME,
                tftbase.ConnectionMode.MNP_2ND_ALLOW,
                tftbase.ConnectionMode.MNP_2ND_DENY,
                tftbase.ConnectionMode.MNP_PRIMARY_DENY,
            )
            for tc in tft.test_cases
        ) and not any(c.secondary_network_nad is not None for c in tft.connections):
            return

        namespace = tft.namespace
        client = cfg_descr.tc.client_tenant
        nad = cfg_descr.get_tft().connections[0].effective_secondary_network_nad
        nad_name = nad.split("/")[-1]

        existing = client.oc_get(
            f"network-attachment-definition/{nad_name}",
            namespace=namespace,
            may_fail=True,
        )
        if existing is not None:
            return

        logger.info(f"Creating secondary NAD {nad} in namespace {namespace}")

        resource_names = {c.resource_name for c in tft.connections}
        resource_name = (
            resource_names.pop()
            if len(resource_names) == 1 and None not in resource_names
            else None
        )

        _j = json.dumps
        in_template = tftbase.get_manifest("secondary-nad.yaml.j2")
        out_yaml = tftbase.get_manifest_renderpath("secondary-nad.yaml")
        kjinja2.render_file(
            in_template,
            {
                "nad_name": _j(nad_name),
                "name_space": _j(namespace),
                "net_attach_def_name": _j(nad),
                "subnets": _j(tftbase.get_secondary_nad_subnets()),
                "mtu": tftbase.get_secondary_nad_mtu(),
                "topology": _j(tftbase.get_secondary_nad_topology()),
                "has_resource_name": resource_name is not None,
                "resource_name": _j(resource_name or ""),
            },
            out_file=out_yaml,
        )
        client.oc(f"apply -f {out_yaml}", die_on_error=True)

    def _cleanup_secondary_nad(self, cfg_descr: ConfigDescriptor) -> None:
        namespace = cfg_descr.get_tft().namespace
        client = cfg_descr.tc.client_tenant
        client.oc(
            "delete network-attachment-definition -l tft-tests=secondary-nad",
            namespace=namespace,
            may_fail=True,
        )

    def _cleanup_multi_network_policies(self, cfg_descr: ConfigDescriptor) -> None:
        namespace = cfg_descr.get_tft().namespace
        client = cfg_descr.tc.client_tenant
        logger.info(
            f"Cleaning multi-networkpolicies and admin-networkpolicies with label tft-tests in namespace {namespace}"
        )
        client.oc(
            "delete multi-networkpolicies -l tft-tests",
            namespace=namespace,
            may_fail=True,
            check_success=client.check_success_delete_ignore_noexist(
                "multi-networkpolicies"
            ),
        )
        client.oc(
            "delete networkpolicies -l tft-tests",
            namespace=namespace,
            may_fail=True,
            check_success=client.check_success_delete_ignore_noexist("networkpolicies"),
        )

        client.oc(
            "delete adminnetworkpolicies -l tft-tests",
            namespace=None,
            may_fail=True,
            check_success=client.check_success_delete_ignore_noexist(
                "adminnetworkpolicies"
            ),
        )

    def _cleanup_stale_udn(self, cfg_descr: ConfigDescriptor) -> None:
        tft = cfg_descr.get_tft()
        udn_ns = f"{tft.namespace}-udn"
        client = cfg_descr.tc.client_tenant
        client.oc(
            "delete clusteruserdefinednetwork -l tft-tests",
            namespace=None,
            may_fail=True,
        )
        if client.oc_get(f"namespace/{udn_ns}", may_fail=True) is None:
            return
        logger.info(
            f"Found existing UDN namespace {udn_ns} from a previous run, cleaning up"
        )
        client.oc(f"delete userdefinednetwork -l tft-tests -n {udn_ns}", may_fail=True)
        client.oc(f"delete namespace {udn_ns}", may_fail=True)
        client.oc(
            f"wait --for=delete namespace/{udn_ns} --timeout=120s",
            may_fail=True,
        )

    def _cleanup_previous_testspace(
        self, cfg_descr: ConfigDescriptor, force_cleanup: bool = False
    ) -> None:
        pre_provision = cfg_descr.get_tft().pre_provision
        namespace = cfg_descr.get_tft().namespace
        client = cfg_descr.tc.client_tenant
        if not pre_provision or force_cleanup:
            logger.info(
                f"Cleaning pods and services with label tft-tests in namespace {namespace}"
            )
            client.oc("delete pods -l tft-tests", namespace=namespace)
            client.oc("delete services -l tft-tests", namespace=namespace)
            self._cleanup_multi_network_policies(cfg_descr)
            if force_cleanup:
                self._cleanup_secondary_nad(cfg_descr)

            if self._udn_setup_done:
                udn_ns = f"{namespace}-udn"
                client.oc("delete pods -l tft-tests", namespace=udn_ns)
                client.oc("delete services -l tft-tests", namespace=udn_ns)

            logger.info(
                f"Cleaning external containers {task.EXTERNAL_PERF_SERVER} (if present)"
            )
            host.local.run(
                f"podman rm --force {task.EXTERNAL_PERF_SERVER}",
                log_level_fail=logging.DEBUG,
                check_success=lambda r: (
                    r.success
                    or (r.returncode == 1 and "no container with name or ID" in r.err)
                    or r.returncode == 127
                ),
            )
        else:
            connection_mode = cfg_descr.get_test_case().info.connection_mode
            if connection_mode in (
                ConnectionMode.MNP_2ND_DENY,
                ConnectionMode.MNP_2ND_ALLOW,
                ConnectionMode.MNP_PRIMARY_DENY,
                ConnectionMode.ANP_ALLOW,
                ConnectionMode.ANP_DENY,
                ConnectionMode.ANP_PASS_NP_DENY,
                ConnectionMode.NP_DENY,
                ConnectionMode.NP_ALLOW,
            ):
                self._cleanup_multi_network_policies(cfg_descr)
            if connection_mode == ConnectionMode.LOAD_BALANCER:
                logger.info(f"Cleaning LoadBalancer services in namespace {namespace}")
                client.oc(
                    "delete services -l tft-svc-type=loadbalancer",
                    namespace=namespace,
                )

    def _cleanup_udn(self, cfg_descr: ConfigDescriptor) -> None:
        if self._udn_ns is None:
            return
        udn_ns = self._udn_ns
        client = cfg_descr.tc.client_tenant
        client.oc(
            "delete clusteruserdefinednetwork -l tft-tests",
            namespace=None,
            may_fail=True,
        )
        client.oc(f"delete userdefinednetwork -l tft-tests -n {udn_ns}", may_fail=True)
        if self._udn_ns_created:
            logger.info(f"Deleting UDN namespace {udn_ns}")
            client.oc(f"delete namespace {udn_ns}", may_fail=True)
        self._udn_ns = None
        self._udn_ns_created = False
        self._udn_setup_done = False

    def _create_log_paths_from_tests(self, test: testConfig.ConfTest) -> Path:
        log_file = test.get_output_file()
        log_file.parent.mkdir(parents=True, exist_ok=True)
        logger.info(f"Logs will be written to {log_file}")
        return log_file

    def _final_cleanup(self, cfg_descr: ConfigDescriptor, ns_created: bool) -> None:
        if self._no_cleanup == -1:
            logger.info("Skipping cleanup")
            return
        if self._no_cleanup > 0:
            logger.info(
                f"Deferring cleanup: waiting {self._no_cleanup} seconds before cleanup"
            )
            _time.sleep(self._no_cleanup)
        self._cleanup_previous_testspace(cfg_descr, force_cleanup=True)
        self._cleanup_udn(cfg_descr)
        if ns_created:
            logger.info(f"Deleting namespace {cfg_descr.get_tft().namespace}")
            cfg_descr.tc.client_tenant.oc(
                f"delete ns {cfg_descr.get_tft().namespace}",
                may_fail=True,
                namespace=None,
            )

    def _run_test_case_instance(
        self,
        cfg_descr: ConfigDescriptor,
        instance_index: int,
        reverse: bool = False,
    ) -> TftResult:
        connection = cfg_descr.get_connection()

        servers: list[task.ServerTask] = []
        clients: list[task.ClientTask] = []
        monitors: list[Task] = []

        ts = TestSettings(
            cfg_descr=cfg_descr,
            instance_index=instance_index,
            reverse=reverse,
        )
        logger.info(f"Starting test {ts.get_test_info()}")
        s, c = connection.test_type_handler.create_server_client(ts)
        servers.append(s)
        clients.append(c)
        current_test_case = cfg_descr.get_test_case()
        for plugin in connection.plugins:
            if not plugin.applies_to_test_case(current_test_case):
                continue
            m = plugin.plugin.enable(
                ts=ts,
                perf_server=servers[-1],
                perf_client=clients[-1],
                tenant=True,
            )
            monitors.extend(m)

        ts.initialize_clmo_barrier(len(clients) + len(monitors))

        pre_provision = cfg_descr.get_tft().pre_provision
        for sc_task in servers + clients:
            if not pre_provision:
                sc_task.initialize()
            sc_task.start_setup(skip_pod_setup=pre_provision)
        for mon in monitors:
            mon.initialize()
            mon.start_setup()

        ts.event_server_alive.wait()

        for tasks in servers + clients + monitors:
            tasks.start_task()

        ts.event_client_finished.wait()

        for tasks in servers + clients + monitors:
            tasks.finish_task()

        for tasks in servers + clients + monitors:
            tasks.finish_setup()

        tft_result_builder = tftbase.TftResultBuilder()

        for tasks in servers + clients + monitors:
            tasks.aggregate_output(tft_result_builder)

        return tft_result_builder.build()

    def _provision_all_resources(self, cfg_descr: ConfigDescriptor) -> None:
        logger.info("Pre-provisioning all pods and services for the test run")

        seen_server_pods: set[str] = set()
        seen_client_pods: set[str] = set()

        for cfg_descr2 in cfg_descr.describe_all_test_cases():
            if cfg_descr2.get_test_case().is_udn and not self._udn_setup_done:
                self._setup_udn(cfg_descr2)
            for cfg_descr3 in cfg_descr2.describe_all_connections():
                connection = cfg_descr3.get_connection()
                for instance_index in range(connection.instances):
                    ts = TestSettings(
                        cfg_descr=cfg_descr3,
                        instance_index=instance_index,
                        reverse=False,
                    )
                    s, c = connection.test_type_handler.create_server_client(ts)

                    # EXTERNAL_IP runs via podman; leave those per-test.
                    if s.pod_name not in seen_server_pods:
                        seen_server_pods.add(s.pod_name)
                        s.initialize()
                        s.start_setup(provisioning=True)
                    else:
                        s.ensure_services()

                    if c.pod_name not in seen_client_pods:
                        seen_client_pods.add(c.pod_name)
                        c.initialize()
                        c.start_setup(provisioning=True)

    def _run_test_case(self, cfg_descr: ConfigDescriptor) -> list[TftResult]:
        # TODO Allow for multiple connections / instances to run simultaneously
        tft_results: list[TftResult] = []
        for cfg_descr2 in cfg_descr.describe_all_connections():
            connection = cfg_descr2.get_connection()
            logger.info(f"Starting {connection.name}")
            logger.info(f"Number Of Simultaneous connections {connection.instances}")
            for instance_index in range(connection.instances):
                tft_results.append(
                    self._run_test_case_instance(
                        cfg_descr2,
                        instance_index=instance_index,
                    )
                )
                if connection.test_type_handler.can_run_reverse(connection):
                    tft_results.append(
                        self._run_test_case_instance(
                            cfg_descr2,
                            instance_index=instance_index,
                            reverse=True,
                        )
                    )
                self._cleanup_previous_testspace(cfg_descr2)
        return tft_results

    def _run_test_cases(self, cfg_descr: ConfigDescriptor) -> TftResults:
        tft_results_lst: list[TftResult] = []
        for cfg_descr2 in cfg_descr.describe_all_test_cases():
            if cfg_descr2.get_test_case().is_udn and not self._udn_setup_done:
                self._setup_udn(cfg_descr)
            tft_results_lst.extend(self._run_test_case(cfg_descr2))
        return TftResults(lst=tuple(tft_results_lst))

    def test_run(
        self,
        cfg_descr: ConfigDescriptor,
        evaluator: Evaluator,
    ) -> TftResults:
        test = cfg_descr.get_tft()
        ns_created = self._configure_namespace(cfg_descr)
        self._cleanup_stale_udn(cfg_descr)
        self._cleanup_previous_testspace(cfg_descr, force_cleanup=True)
        self._setup_secondary_nad(cfg_descr)

        try:
            if test.pre_provision:
                self._provision_all_resources(cfg_descr)

            logger.info(f"Running test {test.name} for {test.duration} seconds")
            tft_results = self._run_test_cases(cfg_descr)

            logger.info("Evaluating results of tests")
            tft_results = evaluator.eval(tft_results=tft_results)

            result_status = tft_results.get_pass_fail_status()
            result_status.log()

            log_file = self._create_log_paths_from_tests(test)

            logger.info(f"Write results to {log_file}")
            tft_results.serialize_to_file(log_file)
            # For backward compatiblity, still write the "-RESULTS" file. It's
            # mostly useless now as it's identical to the main file.
            tft_results.serialize_to_file(
                log_file.parent / (str(log_file.stem) + "-RESULTS")
            )

            if not result_status.result:
                logger.error(f"Failure detected in {cfg_descr.get_tft().name} results")

            print_results.process_results(tft_results, log=logger.info)

            return TftResults(
                lst=tft_results.lst,
                filename=str(log_file),
            )
        finally:
            self._final_cleanup(cfg_descr, ns_created)
