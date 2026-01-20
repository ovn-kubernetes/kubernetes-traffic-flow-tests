#!/usr/bin/env python3

import argparse
import typing

from collections.abc import Iterable
from typing import Optional

from ktoolbox import common

import tftbase

EXIT_CODE_VALIDATION = 1


def print_flow_test_output(
    test_output: Optional[tftbase.FlowTestOutput],
    *,
    log: typing.Callable[[str], None] = print,
) -> None:
    if test_output is None:
        log("Test ID: Unknown test")
        return
    if not test_output.eval_success:
        msg = f"failed: {test_output.eval_msg}"
    else:
        msg = "succeeded"
    log(
        f"Test ID: {test_output.tft_metadata.test_case_id.name}, "
        f"Test Type: {test_output.tft_metadata.test_type.name}, "
        f"Reverse: {common.bool_to_str(test_output.tft_metadata.reverse)}, "
        f"TX Bitrate: {test_output.bitrate_gbps.tx} Gbps, "
        f"RX Bitrate: {test_output.bitrate_gbps.rx} Gbps, "
        f"{msg}"
    )


def print_plugin_output(
    plugin_output: tftbase.PluginOutput,
    *,
    log: typing.Callable[[str], None] = print,
) -> None:
    msg = f"failed: {plugin_output.eval_msg}"
    if not plugin_output.eval_success:
        msg = f"failed: {plugin_output.eval_msg}"
    else:
        msg = "succeeded"
    log("     " f"plugin {plugin_output.plugin_metadata.plugin_name}, " f"{msg}")


def print_tft_result(
    tft_result: tftbase.TftResult,
    *,
    log: typing.Callable[[str], None] = print,
) -> None:
    print_flow_test_output(tft_result.flow_test, log=log)
    for plugin_output in tft_result.plugins:
        print_plugin_output(plugin_output, log=log)


def print_tft_results(
    tft_results: tftbase.TftResults,
    *,
    log: typing.Callable[[str], None] = print,
) -> None:
    for tft_result in tft_results:
        print_tft_result(tft_result, log=log)


def process_results(
    tft_results: tftbase.TftResults,
    *,
    log: typing.Callable[[str], None] = print,
) -> bool:

    group_success, group_fail = tft_results.group_by_success()

    log(
        f"There are {len(group_success)} passing flows{tft_results.log_detail}.{' Details:' if group_success else ''}"
    )
    print_tft_results(group_success, log=log)

    log(
        f"There are {len(group_fail)} failing flows{tft_results.log_detail}.{' Details:' if group_fail else ''}"
    )
    print_tft_results(group_fail, log=log)

    log("")
    return not group_fail


def process_results_all(
    tft_results_lst: Iterable[tftbase.TftResults],
    *,
    log: typing.Callable[[str], None] = print,
) -> bool:
    failed_files: list[str] = []

    for tft_results in common.iter_eval_now(tft_results_lst):
        if not process_results(tft_results, log=log):
            failed_files.append(common.unwrap(tft_results.filename))

    log("")
    if failed_files:
        log(f"Failures detected in {repr(failed_files)}")
        return False

    log("No failures detected in results")
    return True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Tool to prettify the TFT Flow test results"
    )
    parser.add_argument(
        "result",
        nargs="+",
        help="The JSON result file(s) from TFT Flow test.",
    )
    common.log_argparse_add_argument_verbose(parser)

    args = parser.parse_args()

    common.log_config_logger(args.verbose, "tft", "ktoolbox")

    return args


def main() -> int:
    args = parse_args()
    success = process_results_all(
        tftbase.TftResults.parse_from_file(file) for file in args.result
    )
    return 0 if success else EXIT_CODE_VALIDATION


if __name__ == "__main__":
    common.run_main(main)
