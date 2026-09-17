# SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
# SPDX-License-Identifier: Apache-2.0

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pluginMeasurePower  # noqa: E402


def test_extract() -> None:
    out = "\n    Instantaneous power reading:                   346 Watts\n    Minimum during sampling period:                 12 Watts\n    Maximum during sampling period:                703 Watts\n    Average power reading over sample period:      346 Watts\n    IPMI timestamp:                           Tue Jul 16 14:57:49 2024\n    Sampling period:                          00000001 Seconds.\n    Power reading state is:                   activated\n\n\n"

    r = pluginMeasurePower._extract(out)
    assert r == 346
