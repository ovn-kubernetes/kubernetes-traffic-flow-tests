#!/usr/bin/env python3

import argparse
import shlex
import time

from pathlib import Path
from typing import Optional

from ktoolbox import common

import print_results
import tftbase

from evaluator import Evaluator
from testConfig import ConfigDescriptor
from testConfig import TestConfig
from trafficFlowTests import TrafficFlowTests

logger = common.ExtendedLogger("tft." + __name__)


def parse_args() -> argparse.Namespace:

    parser = argparse.ArgumentParser(
        description="Test Traffic Flows in an OVN-Kubernetes cluster."
    )
    parser.add_argument(
        "config",
        metavar="config",
        type=str,
        help='YAML file with test configuration (see "config.yaml").',
    )
    parser.add_argument(
        "evaluator_config",
        nargs="?",
        metavar="evaluator_config",
        type=str,
        help='YAML file with configuration for scoring test results (see "eval-config.yaml"). '
        "The configuration can also contain only a subset of the relevant configurations. "
        "The evaluation will successfully pass if thresholds as missing. "
        "Also, the entire configuration can be empty (either an empty "
        "YAML file or only '{}') or the filename can be '' to indicate a completely "
        "empty configuration. You can later run "
        "`./evaluator.py ${evaluator_config} ${test_result} ${evaluator_result}` "
        "to update the evaluation with a different config.",
    )
    parser.add_argument(
        "-o",
        "--output-base",
        type=str,
        default=None,
        help="The base name for the result files. If specified, the result will be "
        'written to "${output_base}$(printf \'%%03d\' "$number").json" where ${number} is the '
        "zero-based index of the test. This can include the directory name and is relative to "
        'the current directory. If unspecified, the files are written to "${logs}/${timestamp}.json" '
        'where "${logs}" can be specified in the config file (and defaults to "./ft-logs/").',
    )
    parser.add_argument(
        "-c",
        "--check",
        action=argparse.BooleanOptionalAction,
        default=False,
        help='By default, the program only runs the tests and writes the results. It is not expected to fail unless a serious error happened. In that case, you usually want to run `print_results.py` command afterwards. Passing "--check" combines those two steps in one and the `tft.py` command succeeds only if all tests pass.',
    )
    parser.add_argument(
        "--kubeconfig",
        type=str,
        default=None,
        help='The kubeconfig for the tenant cluster. If unspecified, defaults to "$TFT_KUBECONFIG" variable. If still unspecified, taken from the configuration file. If still unspecified, detect based on files in "/root/kubeconfig*".',
    )
    parser.add_argument(
        "--kubeconfig-infra",
        type=str,
        default=None,
        help='The kubeconfig for the infra cluster. If unspecified, defaults to "$TFT_KUBECONFIG_INFRA" variable. If still unspecified, taken from the configuration file. If still unspecified, detect based on files in "/root/kubeconfig*". Note that this option is tightly coupled with "--kubeconfig". This means, if we find a configuration from a certain source (environment, command line, config), then both values must come from the same source.',
    )

    common.log_argparse_add_argument_verbosity(parser)

    args = parser.parse_args()

    common.log_config_logger(args.verbosity, "tft", "ktoolbox")

    if not Path(args.config).exists():
        raise ValueError("Must provide a valid config.yaml file (see config.yaml)")

    return args


def parse_args_full() -> tuple[TestConfig, bool]:
    args = parse_args()

    args_check = args.check
    tc = TestConfig(
        config_path=args.config,
        evaluator_config=args.evaluator_config,
        kubeconfigs=option_get_kubeconfigs(
            args.kubeconfig,
            args.kubeconfig_infra,
        ),
        output_base=args.output_base,
    )

    return tc, args_check


def option_get_kubeconfigs(
    kubeconfig: Optional[str], kubeconfig_infra: Optional[str]
) -> Optional[tuple[str, Optional[str]]]:
    kubeconfigs: Optional[tuple[str, Optional[str]]] = None
    source = "none"
    kubeconfig_source = ""
    kubeconfig_infra_source = ""
    if kubeconfig is not None or kubeconfig_infra is not None:
        source = "command-line"
        kubeconfig_source = '"--kubeconfig"'
        kubeconfig_infra_source = '"--kubeconfig-infra"'
        if kubeconfig is None:
            raise ValueError(
                f"Setting {source} {kubeconfig_infra_source} requires also the {kubeconfig_source} from {source}"
            )
        kubeconfigs = (kubeconfig, kubeconfig_infra)
    if kubeconfigs is None:
        kubeconfig = tftbase.get_environ("TFT_KUBECONFIG")
        kubeconfig_infra = tftbase.get_environ("TFT_KUBECONFIG_INFRA")
        if kubeconfig is not None or kubeconfig_infra is not None:
            source = "environment variable"
            kubeconfig_source = '"$TFT_KUBECONFIG"'
            kubeconfig_infra_source = '"$TFT_KUBECONFIG_INFRA"'
            if kubeconfig is None:
                raise ValueError(
                    f"Setting {source} {kubeconfig_infra_source} requires also setting the {kubeconfig_source} {source}"
                )
            kubeconfigs = (kubeconfig, kubeconfig_infra)

    if kubeconfigs is not None:
        kubeconfig, kubeconfig_infra = kubeconfigs
        logger.info(
            f"KUBECONFIG from {source} {kubeconfig_source}: {shlex.quote(common.unwrap(kubeconfig))}"
        )
        logger.info(
            f"KUBECONFIG_INFRA from {source} {kubeconfig_infra_source}: {shlex.quote(kubeconfig_infra) if kubeconfig_infra is not None else '<MISSING>'}"
        )

    return kubeconfigs


def main() -> int:
    time_start = time.monotonic()

    tc, args_check = parse_args_full()

    tc.system_check()
    tc.log_config()
    tft = TrafficFlowTests()

    evaluator = Evaluator(tc.evaluator_config)

    tft_results_lst = [
        tft.test_run(cfg_descr, evaluator)
        for cfg_descr in ConfigDescriptor(tc).describe_all_tft()
    ]

    exit_code = 0

    def results_log_fcn(msg: str) -> None:
        if args_check:
            logger.info(msg)

    if not print_results.process_results_all(
        tft_results_lst,
        log=results_log_fcn,
    ):
        exit_code = print_results.EXIT_CODE_VALIDATION

    duration = time.monotonic() - time_start
    logger.info(
        f"test completed with {'success' if exit_code == 0 else 'failure'} (duration: {common.format_duration(duration)})"
    )

    if not args_check:
        return 0
    return exit_code


if __name__ == "__main__":
    common.run_main(main)
