#!/usr/bin/env python3

import argparse
import os
import sys
import typing

from collections.abc import Iterable
from typing import Optional

from ktoolbox import common

import tftbase

EXIT_CODE_VALIDATION = 1

_force_no_color: bool = False


def _color_enabled() -> bool:
    return (
        not _force_no_color and not os.environ.get("NO_COLOR") and sys.stdout.isatty()
    )


def _green(text: str) -> str:
    return f"\033[32m{text}\033[0m" if _color_enabled() else text


def _red(text: str) -> str:
    return f"\033[31m{text}\033[0m" if _color_enabled() else text


def print_flow_test_output(
    test_output: Optional[tftbase.FlowTestOutput],
    *,
    log: typing.Callable[[str], None] = print,
) -> None:
    if test_output is None:
        log("Test ID: Unknown test")
        return
    if not test_output.eval_success:
        msg = _red(f"failed: {test_output.eval_msg}")
    else:
        msg = _green("succeeded")
    test_case_id = test_output.tft_metadata.test_case_id
    log(
        f"Test ID: ({test_case_id.value}) {test_case_id.name}, "
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
    if not plugin_output.eval_success:
        msg = _red(f"failed: {plugin_output.eval_msg}")
    else:
        msg = _green("succeeded")
    role = plugin_output.plugin_metadata.plugin_role
    role_str = f" ({role})" if role else ""
    log(f"     plugin {plugin_output.plugin_metadata.plugin_name}{role_str}, {msg}")


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

    log("====== Test Case Summary ======")
    log("")

    log(f"--- Passing Flows ({len(group_success)}{tft_results.log_detail}) ---")
    print_tft_results(group_success, log=log)
    log("")

    log(f"--- Failing Flows ({len(group_fail)}{tft_results.log_detail}) ---")
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
    parser.add_argument(
        "--no-color",
        action="store_true",
        default=False,
        help="Disable color codes in output.",
    )
    common.log_argparse_add_argument_verbose(parser)

    args = parser.parse_args()

    common.log_config_logger(args.verbose, "tft", "ktoolbox")

    return args


def main() -> int:
    global _force_no_color
    args = parse_args()
    _force_no_color = args.no_color
    success = process_results_all(
        tftbase.TftResults.parse_from_file(file) for file in args.result
    )
    return 0 if success else EXIT_CODE_VALIDATION


if __name__ == "__main__":
    common.run_main(main)
