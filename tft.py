#!/usr/bin/env python3

import argparse

from pathlib import Path

from ktoolbox import common

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

    common.log_argparse_add_argument_verbosity(parser)

    args = parser.parse_args()

    common.log_config_logger(args.verbosity, "tft", "ktoolbox")

    if not Path(args.config).exists():
        raise ValueError("Must provide a valid config.yaml file (see config.yaml)")

    return args


def main() -> None:
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

    for cfg_descr in ConfigDescriptor(tc).describe_all_tft():
        tft.test_run(cfg_descr, evaluator)


if __name__ == "__main__":
    common.run_main(main)
