#!/usr/bin/env python3

import argparse
import os
import sys

from collections.abc import Iterable
from typing import Any
from typing import Optional

from ktoolbox import common

import tftbase

from evalConfig import Config
from evalConfig import EvalIdentity
from tftbase import Bitrate
from tftbase import TftResults

logger = common.ExtendedLogger("tft." + __name__)


def load_config(config: Optional[str]) -> Optional[Config]:
    if not config:
        return None
    return Config.parse_from_file(config)


@common.iter_tuplify
def load_logs(
    logs: Iterable[str],
    *,
    skip_invalid_logs: bool = False,
) -> Iterable[TftResults]:
    for log in list(logs):
        try:
            tft_results = TftResults.parse_from_file(log)
        except Exception as e:
            if not skip_invalid_logs:
                raise
            # Failures are not fatal here. That is because the output format is
            # not stable, so if we change the format, we may be unable to parse
            # certain older logs. Skip.
            logger.warning(f"Skip invalid file {repr(log)}: {e}")
            continue
        yield tft_results


def collect_all_bitrates(
    config: Optional[Config],
    all_tft_results: Iterable[TftResults],
) -> dict[EvalIdentity, list[Bitrate]]:
    result: dict[EvalIdentity, list[Bitrate]]

    if config is not None:
        result = {ei: [] for ei in config.get_items()}
    else:
        result = {}

    for tft_results in all_tft_results:
        for tft_result in tft_results:
            if not tft_result.eval_all_success:
                # This result is not valid. We don't consider it for
                # calculating the new thresholds.
                #
                # Note that this also includes eval_success fields, that is the
                # result of a previous evaluation (with another eval-config).
                # If you don't want that, run first ./evaluator.py on the
                # input file with an empty eval-config, to clean out all
                # previous evaluations.
                continue
            flow_test = tft_result.flow_test
            ei = EvalIdentity.from_metadata(flow_test.tft_metadata)
            lst = result.get(ei)
            if lst is None:
                if config is not None:
                    # we only collect the items that we have in config too. Don't create a new one.
                    continue
                lst = []
                result[ei] = lst
            lst.append(flow_test.bitrate_gbps)
    return result


def calc_mean_stddev(data: list[float]) -> tuple[float, float]:
    mean = sum(data) / len(data)
    variance = sum((x - mean) ** 2 for x in data) / (len(data))
    stddev: float = variance**0.5
    return mean, stddev


def accumulate_rate(
    rate: Iterable[Optional[float]],
    *,
    quorum: int,
) -> Optional[float]:
    data = [x for x in rate if x is not None]

    if not data or len(data) < quorum:
        return None

    mean, stddev = calc_mean_stddev(data)

    # Filter out outliers outside 3 stddev.
    data2 = [x for x in data if x > mean - 3 * stddev and x < mean + 3 * stddev]

    if not data2 or len(data2) < quorum:
        return None

    mean, stddev = calc_mean_stddev(data2)

    return max(
        mean - 2.0 * stddev,
        mean * 0.8,
    )


def accumulate_bitrates(
    bitrates: list[Bitrate],
    *,
    quorum: int,
) -> Bitrate:
    rx = accumulate_rate((bitrate.rx for bitrate in bitrates), quorum=quorum)
    tx = accumulate_rate((bitrate.tx for bitrate in bitrates), quorum=quorum)
    return Bitrate(rx=rx, tx=tx)


def _tighten_rate(
    a: Optional[float], *, base: Optional[float], tighten_only: bool
) -> Optional[float]:
    if base is None:
        return None
    if a is None:
        return base
    if tighten_only:
        return max(a, base)
    return a


@common.iter_dictify
def accumulate_all_bitrates(
    config: Optional[Config],
    all_bitrates: dict[EvalIdentity, list[Bitrate]],
    *,
    tighten_only: bool,
    quorum: int,
) -> Iterable[tuple[EvalIdentity, Bitrate]]:
    if config is not None:
        assert list(all_bitrates) == list(config.get_items())
    for ei, bitrates in all_bitrates.items():
        bitrate = accumulate_bitrates(bitrates, quorum=quorum)
        if config is not None:
            item = config.get_item_for_id(ei)
            assert item is not None
            bitrate2 = item.bitrate

            rx = _tighten_rate(bitrate.rx, base=bitrate2.rx, tighten_only=tighten_only)
            tx = _tighten_rate(bitrate.tx, base=bitrate2.tx, tighten_only=tighten_only)
            bitrate = Bitrate(rx=rx, tx=tx)
        yield ei, bitrate


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Tool to generate eval-config.yaml TFT Flow test results"
    )
    parser.add_argument(
        "logs",
        nargs="*",
        help="Result file(s) from a traffic flow test run.",
    )
    parser.add_argument(
        "-o",
        "--output",
        default=None,
        help="Output file to write new eval-config.yaml to.",
    )
    parser.add_argument(
        "-S",
        "--skip-invalid-logs",
        action="store_true",
        help='If set any invalid "--logs" files are ignored. This is useful because the output format is not stable, so your last logs might have been generated with an incompatible version and we want to skip those errors.',
    )
    parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        help="For overwriting output file if it exists.",
    )
    parser.add_argument(
        "-Q",
        "--quorum",
        default=0,
        type=int,
        help="If specified, require at least that many successful measurements for calculating a new threshold.",
    )
    parser.add_argument(
        "-c",
        "--config",
        help="The base eval-config. If given, the result will contain all the entries from this input file. Values are updated with the measurementns from the logs.",
    )
    parser.add_argument(
        "-T",
        "--tighten-only",
        action="store_true",
        help="With '--config' the values are only updated if they tighten (increase) the thresholds.",
    )
    common.log_argparse_add_argument_verbose(parser)

    args = parser.parse_args()

    common.log_config_logger(args.verbose, "tft", "ktoolbox")

    if args.tighten_only and not args.config:
        logger.error(
            'Option "--tighten-only" requires a "--config" base configuration.'
        )
        sys.exit(4)

    return args


def log_data(
    config: Optional[Config],
    all_bitrates: dict[EvalIdentity, list[Bitrate]],
    new_bitrates: dict[EvalIdentity, Bitrate],
) -> None:
    for ei, bitrates in all_bitrates.items():
        config_bitrate: Optional[Bitrate] = None
        if config is not None:
            item = common.unwrap(config.get_item_for_id(ei))
            config_bitrate = item.bitrate
        new_bitrate = new_bitrates.get(ei)

        if config is None:
            msg = f"new={Bitrate.get_pretty_str(new_bitrate)}"
        else:
            msg = f"config={Bitrate.get_pretty_str(config_bitrate)}"
            if config_bitrate != new_bitrate:
                msg += f" ; new={Bitrate.get_pretty_str(new_bitrate)}"

        bitrates_msg = (
            "[rx=["
            + ",".join(str(r.rx) for r in bitrates)
            + "],tx=["
            + ",".join(str(r.tx) for r in bitrates)
            + "]]"
        )
        logger.debug(f"{ei.pretty_str}: {msg} ; bitrates={bitrates_msg}")


def bitrate_to_yaml(bitrate: Bitrate) -> dict[str, Any]:
    dd: dict[str, Any] = {}
    common.dict_add_optional(dd, "threshold_rx", bitrate.rx)
    common.dict_add_optional(dd, "threshold_tx", bitrate.tx)
    return dd


def generate_result_config(
    config: Optional[Config],
    bitrates: dict[EvalIdentity, Bitrate],
) -> Config:
    new_config: dict[str, list[dict[str, Any]]] = {}
    handled: set[EvalIdentity] = set()
    for ei in bitrates:
        ei, ei_reverse = ei.both_directions()

        if ei in handled:
            continue
        handled.add(ei)

        if config is not None:
            assert config.get_item_for_id(ei) or config.get_item_for_id(ei_reverse)

        bitrate = bitrates.get(ei, Bitrate.NA)
        bitrate_reverse = bitrates.get(ei_reverse, Bitrate.NA)

        if config is None and bitrate.is_na and bitrate_reverse.is_na:
            continue

        lst = new_config.get(ei.test_type.name)
        if lst is None:
            lst = []
            new_config[ei.test_type.name] = lst

        list_entry: dict[str, Any] = {
            "id": ei.test_case_id.name,
        }
        if not bitrate.is_na:
            list_entry["Normal"] = bitrate_to_yaml(bitrate)
        if not bitrate_reverse.is_na:
            list_entry["Reverse"] = bitrate_to_yaml(bitrate_reverse)

        lst.append(list_entry)

    # Normalize the generated dictionary by sorting.
    for lst in new_config.values():
        lst.sort(key=lambda x: tftbase.TestCaseType[x["id"]].value)
    keys = [
        tftbase.TestType(n)
        for n in sorted(tftbase.TestType[n].value for n in new_config)
    ]
    new_config = {test_type.name: new_config[test_type.name] for test_type in keys}

    return Config.parse(new_config)


def write_to_file(
    config: Config,
    *,
    output: Optional[str],
    force: bool,
) -> None:

    if not output:
        config.serialize_to_file(sys.stdout)
        return

    if not force and os.path.exists(output):
        logger.error(
            f"The output file {repr(output)} already exists. Run with '--force' to overwrite"
        )
        sys.exit(55)

    config.serialize_to_file(output)


def main() -> None:
    args = parse_args()

    config = load_config(args.config)

    all_tft_results = load_logs(
        args.logs,
        skip_invalid_logs=args.skip_invalid_logs,
    )

    all_bitrates = collect_all_bitrates(config, all_tft_results)

    new_bitrates = accumulate_all_bitrates(
        config,
        all_bitrates,
        tighten_only=args.tighten_only,
        quorum=args.quorum,
    )

    log_data(config, all_bitrates, new_bitrates)

    result_config = generate_result_config(config, new_bitrates)

    write_to_file(
        result_config,
        output=args.output,
        force=args.force or args.config == args.output,
    )


if __name__ == "__main__":
    common.run_main(main)
