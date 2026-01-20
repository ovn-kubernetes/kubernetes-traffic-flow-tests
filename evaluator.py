#!/usr/bin/env python3

import argparse
import pathlib
import sys

from pathlib import Path
from typing import Optional

from ktoolbox import common

import evalConfig
import tftbase

from tftbase import FlowTestOutput
from tftbase import TftResult
from tftbase import TftResults

logger = common.ExtendedLogger("tft." + __name__)


class Evaluator:
    eval_config: evalConfig.Config

    def __init__(self, config: Optional[evalConfig.Config | str | pathlib.Path]):
        if not isinstance(config, evalConfig.Config):
            config = evalConfig.Config.parse_from_file(config)
        self.eval_config = config

    def eval_flow_test_output(self, flow_test: FlowTestOutput) -> FlowTestOutput:

        item = self.eval_config.get_item(
            test_type=flow_test.tft_metadata.test_type,
            test_case_id=flow_test.tft_metadata.test_case_id,
            is_reverse=flow_test.tft_metadata.reverse,
        )

        bitrate_threshold_rx: Optional[float] = None
        bitrate_threshold_tx: Optional[float] = None
        if item is not None:
            bitrate_threshold_rx = item.get_threshold(rx=True)
            bitrate_threshold_tx = item.get_threshold(tx=True)

        success = True
        msg: Optional[str] = None
        if not flow_test.success:
            success = False
            if flow_test.msg is not None:
                msg = f"Run failed: {flow_test.msg}"
            else:
                msg = "Run failed for unspecified reason"
        elif not flow_test.bitrate_gbps.is_passing(bitrate_threshold_rx, rx=True):
            success = False
            msg = f"Run succeeded but {flow_test.bitrate_gbps} is below RX threshold {bitrate_threshold_rx}"
        elif not flow_test.bitrate_gbps.is_passing(bitrate_threshold_tx, tx=True):
            success = False
            msg = f"Run succeeded but {flow_test.bitrate_gbps} is below TX threshold {bitrate_threshold_tx}"

        return flow_test.clone(
            eval_result=tftbase.EvalResult(
                success=success,
                msg=msg,
                bitrate_threshold_rx=bitrate_threshold_rx,
                bitrate_threshold_tx=bitrate_threshold_tx,
            ),
        )

    def eval_test_result(self, tft_result: TftResult) -> TftResult:
        new_flow_test = self.eval_flow_test_output(tft_result.flow_test)

        new_plugins = [
            plugin_output.plugin.eval_plugin_output(
                tft_result.flow_test.tft_metadata,
                plugin_output,
            )
            for plugin_output in tft_result.plugins
        ]

        return TftResult(
            flow_test=new_flow_test,
            plugins=tuple(new_plugins),
        )

    def eval(
        self,
        tft_results: TftResults,
    ) -> TftResults:
        lst = [self.eval_test_result(tft_result) for tft_result in tft_results]
        return TftResults(
            lst=tuple(lst),
            filename=tft_results.filename,
        )

    def eval_from_file(
        self,
        filename: str | Path,
    ) -> TftResults:
        return self.eval(
            TftResults.parse_from_file(filename),
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Tool to evaluate TFT Flow test results"
    )
    parser.add_argument(
        "config",
        metavar="config",
        type=str,
        help='YAML configuration file with tft test thresholds. See "eval-config.yaml". '
        "The configuration can also contain only a subset of the relevant configurations. "
        "Evaluation will successfully pass if thresholds as missing. "
        "Also, the entire configuration can be empty (either an empty "
        "YAML file or only '{}') or the filename can be '' to indicate a completely "
        "empty configuration.",
    )
    parser.add_argument(
        "logs",
        type=str,
        help='Result file from a traffic flow test run. The "tft.py" tool by default writes this as file '
        '"./ft-logs/$TIMESTAMP.json". Also, the test always already performs an evaluation with  '
        "the provided eval config YAML (which can be empty or omitted). The input format is the same as the "
        "output format and the same as the test produces.",
    )
    parser.add_argument(
        "output",
        type=str,
        help="Output file to write evaluation results to. This is the same format as the input argument "
        "'logs'. You can pass the output to evaluator.py again for updating the evaluation.",
    )
    common.log_argparse_add_argument_verbose(parser)

    args = parser.parse_args()

    common.log_config_logger(args.verbose, "tft", "ktoolbox")

    if args.config and not Path(args.config).exists():
        logger.error(f"No config file found at {args.config}, exiting")
        sys.exit(common.EX_CONFIG)

    if not args.logs or not Path(args.logs).exists():
        logger.error(f"Log file {args.logs} does not exist")
        sys.exit(common.EX_CONFIG)

    return args


def main() -> None:
    args = parse_args()
    evaluator = Evaluator(args.config)
    tft_results = evaluator.eval_from_file(args.logs)
    tft_results.serialize_to_file(args.output)
    tft_results.get_pass_fail_status().log()


if __name__ == "__main__":
    common.run_main(main)
