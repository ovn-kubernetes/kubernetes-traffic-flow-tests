# SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
# SPDX-License-Identifier: Apache-2.0

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import testType  # noqa: E402
import tftbase  # noqa: E402


def test_handlers() -> None:
    for test_type in tftbase.TestType:
        h = testType.TestTypeHandler.get(test_type)
        assert isinstance(h, testType.TestTypeHandler)
        assert h.test_type == test_type
