#!/usr/bin/env python3

import argparse

from pathlib import Path

from ktoolbox import common

import print_results

from evaluator import Evaluator
from testConfig import ConfigDescriptor
from testConfig import TestConfig
from trafficFlowTests import TrafficFlowTests


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
        type=bool,
        default=False,
        help='By default, the program only runs the tests and writes the results. It is not expected to fail unless a serious error happened. In that case, you usually want to run `print_results.py` command afterwards. Passing "--check" combines those two steps in one and the `tft.py` command succeeds only if all tests pass.',
    )

    common.log_argparse_add_argument_verbosity(parser)

    args = parser.parse_args()

    common.log_config_logger(args.verbosity, "tft", "ktoolbox")

    if not Path(args.config).exists():
        raise ValueError("Must provide a valid config.yaml file (see config.yaml)")

    return args


def main() -> int:
    args = parse_args()

    tc = TestConfig(
        config_path=args.config,
        evaluator_config=args.evaluator_config,
        output_base=args.output_base,
    )
    tc.system_check()
    tc.log_config()
    tft = TrafficFlowTests()

    evaluator = Evaluator(tc.evaluator_config)

    tft_results_lst = []

    for cfg_descr in ConfigDescriptor(tc).describe_all_tft():
        tft_results = tft.test_run(cfg_descr, evaluator)
        tft_results_lst.append(tft_results)

    if args.check:
        if not print_results.process_results_all(tft_results_lst):
            return print_results.EXIT_CODE_VALIDATION

    return 0


if __name__ == "__main__":
    common.run_main(main)
