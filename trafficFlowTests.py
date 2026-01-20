import logging
import task

from pathlib import Path

from ktoolbox import common
from ktoolbox import host

import testConfig
import tftbase

from evaluator import Evaluator
from task import Task
from testConfig import ConfigDescriptor
from testSettings import TestSettings
from tftbase import TftResult
from tftbase import TftResults

logger = common.ExtendedLogger("tft." + __name__)


class TrafficFlowTests:
    def _configure_namespace(self, cfg_descr: ConfigDescriptor) -> None:
        namespace = cfg_descr.get_tft().namespace
        logger.info(f"Configuring namespace {namespace}")
        cfg_descr.tc.client_tenant.oc(
            f"label ns --overwrite {namespace} pod-security.kubernetes.io/enforce=privileged \
                                        pod-security.kubernetes.io/enforce-version=v1.24 \
                                        security.openshift.io/scc.podSecurityLabelSync=false",
            die_on_error=True,
        )

    def _cleanup_previous_testspace(self, cfg_descr: ConfigDescriptor) -> None:
        namespace = cfg_descr.get_tft().namespace
        client = cfg_descr.tc.client_tenant
        logger.info(
            f"Cleaning pods, services and multi-networkpolicies with label tft-tests in namespace {namespace}"
        )
        client.oc("delete pods -l tft-tests", namespace=namespace)
        client.oc("delete services -l tft-tests", namespace=namespace)
        client.oc(
            "delete multi-networkpolicies -l tft-tests",
            namespace=namespace,
            check_success=client.check_success_delete_ignore_noexist(
                "multi-networkpolicies"
            ),
        )

        logger.info(
            f"Cleaning external containers {task.EXTERNAL_PERF_SERVER} (if present)"
        )
        host.local.run(
            f"podman rm --force {task.EXTERNAL_PERF_SERVER}",
            log_level_fail=logging.WARN,
            check_success=lambda r: (
                r.success
                or (r.returncode == 1 and "no container with name or ID" in r.err)
            ),
        )

    def _create_log_paths_from_tests(self, test: testConfig.ConfTest) -> Path:
        log_file = test.get_output_file()
        log_file.parent.mkdir(parents=True, exist_ok=True)
        logger.info(f"Logs will be written to {log_file}")
        return log_file

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
        s, c = connection.test_type_handler.create_server_client(ts)
        servers.append(s)
        clients.append(c)
        for plugin in connection.plugins:
            m = plugin.plugin.enable(
                ts=ts,
                perf_server=servers[-1],
                perf_client=clients[-1],
                tenant=True,
            )
            monitors.extend(m)

        for t in servers + clients + monitors:
            t.initialize()

        ts.initialize_clmo_barrier(len(clients) + len(monitors))

        for tasks in servers + clients + monitors:
            tasks.start_setup()

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
                if connection.test_type_handler.can_run_reverse():
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
            tft_results_lst.extend(self._run_test_case(cfg_descr2))
        return TftResults(lst=tuple(tft_results_lst))

    def test_run(
        self,
        cfg_descr: ConfigDescriptor,
        evaluator: Evaluator,
    ) -> TftResults:
        test = cfg_descr.get_tft()
        self._configure_namespace(cfg_descr)
        self._cleanup_previous_testspace(cfg_descr)

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

        return TftResults(
            lst=tft_results.lst,
            filename=str(log_file),
        )
