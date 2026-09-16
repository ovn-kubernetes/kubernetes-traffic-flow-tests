# SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
# SPDX-License-Identifier: Apache-2.0

import os
import pathlib
import typing
import yaml

from collections.abc import Mapping
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any
from typing import Optional

from ktoolbox import common
from ktoolbox import kyaml
from ktoolbox.common import StructParseBase
from ktoolbox.common import StructParseParseContext
from ktoolbox.common import strict_dataclass

import testType
import tftbase

from tftbase import Bitrate
from tftbase import TestCaseType
from tftbase import TestType


@strict_dataclass
@dataclass(frozen=True, kw_only=True)
class EvalIdentity:
    test_type: TestType
    test_case_id: TestCaseType
    is_reverse: bool

    def clone(
        self,
        test_type: Optional[TestType] = None,
        test_case_id: Optional[TestCaseType] = None,
        is_reverse: Optional[bool] = None,
    ) -> "EvalIdentity":
        if test_type is None:
            test_type = self.test_type
        if test_case_id is None:
            test_case_id = self.test_case_id
        if is_reverse is None:
            is_reverse = self.is_reverse
        return EvalIdentity(
            test_type=test_type,
            test_case_id=test_case_id,
            is_reverse=is_reverse,
        )

    @staticmethod
    def from_metadata(tft_metadata: tftbase.TestMetadata) -> "EvalIdentity":
        return EvalIdentity(
            test_type=tft_metadata.test_type,
            test_case_id=tft_metadata.test_case_id,
            is_reverse=tft_metadata.reverse,
        )

    def both_directions(self) -> tuple["EvalIdentity", "EvalIdentity"]:
        reverse = self.clone(is_reverse=not self.is_reverse)
        if self.is_reverse:
            return reverse, self
        else:
            return self, reverse

    @property
    def pretty_str(self) -> str:
        return f"[{self.test_type.name}, {self.test_case_id.name}, {'R' if self.is_reverse else 'N'}]"


@strict_dataclass
@dataclass(frozen=True, kw_only=True)
class TestItem(StructParseBase):
    threshold_rx: Optional[float]
    threshold_tx: Optional[float]

    def _post_check(self) -> None:
        object.__setattr__(
            self,
            "_bitrate",
            Bitrate(rx=self.threshold_rx, tx=self.threshold_tx),
        )

    @property
    def has_thresholds(self) -> bool:
        return self.threshold_rx is not None or self.threshold_tx is not None

    @property
    def bitrate(self) -> Bitrate:
        bitrate: Bitrate = getattr(self, "_bitrate")
        return bitrate

    def get_threshold(
        self,
        *,
        rx: Optional[bool] = None,
        tx: Optional[bool] = None,
    ) -> Optional[float]:
        rx, tx = tftbase.eval_binary_opt_in(rx, tx)
        if rx and tx:
            # Either rx/tx is requested. We return the maximum for both.
            if not self.has_thresholds:
                return None
            return max(
                v for v in (self.threshold_rx, self.threshold_tx) if v is not None
            )
        elif rx:
            return self.threshold_rx
        elif tx:
            return self.threshold_tx
        else:
            return None

    @staticmethod
    def parse(pctx: StructParseParseContext) -> "TestItem":
        if pctx.arg is None:
            return TestItem(
                yamlidx=pctx.yamlidx,
                yamlpath=pctx.yamlpath,
                threshold_rx=None,
                threshold_tx=None,
            )

        with pctx.with_strdict() as varg:
            threshold_rx = common.structparse_pop_float(
                varg.for_key("threshold_rx"),
                default=None,
            )
            threshold_tx = common.structparse_pop_float(
                varg.for_key("threshold_tx"),
                default=None,
            )
            threshold = common.structparse_pop_float(
                varg.for_key("threshold"),
                default=None,
            )

            if threshold_rx is None and threshold_tx is None:
                if threshold is not None:
                    threshold_rx = threshold
                    threshold_tx = threshold
            else:
                if threshold is not None:
                    raise pctx.value_error(
                        f"Cannot set together with '{'threshold_rx' if threshold_rx is not None else 'threshold_tx'}'",
                        key="threshold",
                    )

        return TestItem(
            yamlidx=pctx.yamlidx,
            yamlpath=pctx.yamlpath,
            threshold_rx=threshold_rx,
            threshold_tx=threshold_tx,
        )

    def serialize(self) -> dict[str, Any]:
        extra: dict[str, Any] = {}
        if self.has_thresholds:

            def _normalize(x: Optional[float]) -> Optional[int | float]:
                if x is None:
                    return None
                if x == int(x):
                    return int(x)
                return x

            if self.threshold_rx == self.threshold_tx:
                common.dict_add_optional(
                    extra, "threshold", _normalize(self.threshold_rx)
                )
            else:
                common.dict_add_optional(
                    extra, "threshold_rx", _normalize(self.threshold_rx)
                )
                common.dict_add_optional(
                    extra, "threshold_tx", _normalize(self.threshold_tx)
                )
        return extra


@strict_dataclass
@dataclass(frozen=True, kw_only=True)
class TestCaseData(StructParseBase):
    test_case_type: TestCaseType
    normal: TestItem
    reverse: TestItem

    def get_item(
        self,
        *,
        is_reverse: bool,
    ) -> TestItem:
        if is_reverse:
            return self.reverse
        return self.normal

    @staticmethod
    def parse(pctx: StructParseParseContext) -> "TestCaseData":
        with pctx.with_strdict() as varg:

            test_case_type = common.structparse_pop_enum(
                varg.for_key("id"),
                enum_type=TestCaseType,
            )

            normal = common.structparse_pop_obj(
                varg.for_key("Normal"),
                construct=TestItem.parse,
                construct_default=True,
            )

            reverse = common.structparse_pop_obj(
                varg.for_key("Reverse"),
                construct=TestItem.parse,
                construct_default=True,
            )

        return TestCaseData(
            yamlidx=pctx.yamlidx,
            yamlpath=pctx.yamlpath,
            test_case_type=test_case_type,
            normal=normal,
            reverse=reverse,
        )

    def serialize(self) -> dict[str, Any]:
        extra: dict[str, Any] = {}
        if self.normal.has_thresholds:
            common.dict_add_optional(extra, "Normal", self.normal.serialize())
        if self.reverse.has_thresholds:
            common.dict_add_optional(extra, "Reverse", self.reverse.serialize())
        return {
            "id": self.test_case_type.name,
            **extra,
        }


@strict_dataclass
@dataclass(frozen=True, kw_only=True)
class TestTypeData(StructParseBase):
    test_type: TestType
    test_cases: Mapping[TestCaseType, TestCaseData]
    test_type_handler: testType.TestTypeHandler

    @staticmethod
    def parse(yamlidx: int, yamlpath_base: str, key: Any, arg: Any) -> "TestTypeData":
        try:
            test_type = common.enum_convert(TestType, key)
        except Exception:
            raise ValueError(
                f'"{yamlpath_base}.[{yamlidx}]": expects a test type as key like iperf-tcp but got {key}'
            )

        yamlpath = f"{yamlpath_base}.{test_type.name}"

        try:
            test_type_handler = testType.TestTypeHandler.get(test_type)
        except Exception:
            raise ValueError(
                f'"{yamlpath}": test type "{test_type}" has no handler implementation'
            )

        if not isinstance(arg, list):
            raise ValueError(f'"{yamlpath}": expects a list of test cases')
        test_cases = {}
        for pctx2 in StructParseParseContext.enumerate_list(yamlpath, arg):
            c = TestCaseData.parse(pctx2)
            if c.test_case_type in test_cases:
                raise ValueError(
                    f'"{pctx2.yamlpath}": duplicate key {c.test_case_type.name}'
                )
            test_cases[c.test_case_type] = c

        return TestTypeData(
            yamlidx=yamlidx,
            yamlpath=yamlpath,
            test_type=test_type,
            test_type_handler=test_type_handler,
            test_cases=test_cases,
        )

    def serialize(self) -> list[Any]:
        return [v.serialize() for k, v in self.test_cases.items()]


@strict_dataclass
@dataclass(frozen=True, kw_only=True)
class Config(StructParseBase):
    configs: Mapping[TestType, TestTypeData]
    config_path: Optional[str]
    configdir: str
    cwddir: str

    @staticmethod
    def parse(
        arg: Any,
        *,
        config_path: Optional[str] = None,
        cwddir: str = ".",
    ) -> "Config":

        if not config_path:
            config_path = None

        cwddir = common.path_norm(cwddir, make_absolute=True)
        config_path = common.path_norm(config_path, cwd=cwddir)

        if config_path is not None:
            configdir = os.path.dirname(config_path)
        else:
            configdir = cwddir

        yamlpath = ""

        if arg is None:
            # An empty file is valid too. That shows up here as None.
            vdict = {}
        else:
            vdict = common.structparse_check_strdict(arg, yamlpath)

        configs = {}

        for yamlidx2, key in enumerate(vdict):
            c = TestTypeData.parse(
                yamlidx=yamlidx2,
                yamlpath_base=yamlpath,
                key=key,
                arg=vdict[key],
            )
            if c.test_type in configs:
                raise ValueError(
                    f'"{yamlpath}.[{yamlidx2}]": duplicate key {c.test_type.name}'
                )
            configs[c.test_type] = c

        return Config(
            yamlidx=0,
            yamlpath=yamlpath,
            configs=configs,
            config_path=config_path,
            configdir=configdir,
            cwddir=cwddir,
        )

    @staticmethod
    def parse_from_file(
        config_path: Optional[str | pathlib.Path] = None,
        *,
        cwddir: str = ".",
    ) -> "Config":
        yamldata: Any = None
        errmsg_detail = ""

        if not config_path:
            config_path = None

        cwddir = common.path_norm(cwddir, make_absolute=True)
        config_path = common.path_norm(config_path, cwd=cwddir)

        if config_path is not None:
            config_path = str(config_path)
            errmsg_detail = f" {repr(config_path)}"
            try:
                with open(config_path) as file:
                    yamldata = yaml.safe_load(file)
            except Exception as e:
                raise RuntimeError(f"Failure reading{errmsg_detail}: {e}")

        try:
            return Config.parse(yamldata, config_path=config_path, cwddir=cwddir)
        except ValueError as e:
            raise ValueError(f"Failure parsing{errmsg_detail}: {e}")
        except Exception as e:
            raise RuntimeError(f"Failure loading{errmsg_detail}: {e}")

    def serialize(self) -> dict[str, Any]:
        return {k.name: v.serialize() for k, v in self.configs.items()}

    def serialize_to_file(
        self,
        filename: str | pathlib.Path | typing.IO[str],
    ) -> None:
        kyaml.dump(
            self.serialize(),
            filename,
        )

    @common.iter_dictify
    def get_items(self) -> Iterable[tuple[EvalIdentity, TestItem]]:
        for test_type, test_type_data in self.configs.items():
            for test_case_id, test_case_data in test_type_data.test_cases.items():
                item = test_case_data.get_item(is_reverse=False)
                if item is not None:
                    ei = EvalIdentity(
                        test_type=test_type,
                        test_case_id=test_case_id,
                        is_reverse=False,
                    )
                    yield ei, item
                item = test_case_data.get_item(is_reverse=True)
                if item is not None:
                    ei = EvalIdentity(
                        test_type=test_type,
                        test_case_id=test_case_id,
                        is_reverse=True,
                    )
                    yield ei, item

    def get_item(
        self,
        *,
        test_type: TestType,
        test_case_id: TestCaseType,
        is_reverse: bool,
    ) -> Optional[TestItem]:
        test_type_data = self.configs.get(test_type)
        if test_type_data is None:
            return None
        test_case_data = test_type_data.test_cases.get(test_case_id)
        if test_case_data is None:
            return None
        return test_case_data.get_item(is_reverse=is_reverse)

    def get_item_for_id(
        self,
        ei: EvalIdentity,
    ) -> Optional[TestItem]:
        return self.get_item(
            test_type=ei.test_type,
            test_case_id=ei.test_case_id,
            is_reverse=ei.is_reverse,
        )
