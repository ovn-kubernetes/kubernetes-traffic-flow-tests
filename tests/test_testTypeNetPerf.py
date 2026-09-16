# SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
# SPDX-License-Identifier: Apache-2.0

import os
import pytest
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import testTypeNetPerf  # noqa: E402
import tftbase  # noqa: E402


def test_netperf_parse_tcp_rr() -> None:

    data = """MIGRATED TCP REQUEST/RESPONSE TEST from 0.0.0.0 (0.0.0.0) port 0 AF_INET to 10.131.0.169 () port 0 AF_INET : demo : first burst 0
Local /Remote
Socket Size   Request  Resp.   Elapsed  Trans.
Send   Recv   Size     Size    Time     Rate         
bytes  Bytes  bytes    bytes   secs.    per sec   

16384  131072 1        1       30.00    43642.12   
16384  131072
"""

    assert testTypeNetPerf.netperf_parse(tftbase.TestType.NETPERF_TCP_RR, data) == {
        "Socket Send Bytes": 16384.0,
        "Size Receive Bytes": 131072.0,
        "Request Size Bytes": 1.0,
        "Response Size Bytes": 1.0,
        "Elapsed Time Seconds": 30.0,
        "Transaction Rate Per Second": 43642.12,
    }

    data = """MIGRATED TCP REQUEST/RESPONSE TEST from 0.0.0.0 (0.0.0.0) port 0 AF_INET to 10.131.0.169 () port 0 AF_INET : demo : first burst 0
Local /Remote
Socket Size   Request  Resp.   Elapsed  Trans.
Send   Recv   Size     Size    Time     Rate         
bytes  Bytes  bytes    bytes   secs.    per sec   

16384  131072 1     2   1       30.00    43642.12   
16384  131072
"""

    with pytest.raises(ValueError):
        testTypeNetPerf.netperf_parse("TCP_RR", data)


def test_netperf_parse_tcp_stream() -> None:

    data = """MIGRATED TCP STREAM TEST from 0.0.0.0 (0.0.0.0) port 0 AF_INET to 10.131.0.167 () port 0 AF_INET : demo
Recv   Send    Send                          
Socket Socket  Message  Elapsed              
Size   Size    Size     Time     Throughput  
bytes  bytes   bytes    secs.    10^6bits/sec  

131072  16384  16384    30.00    35710.68   
"""

    assert testTypeNetPerf.netperf_parse("TCP_STREAM", data) == {
        "Receive Socket Size Bytes": 131072.0,
        "Send Socket Size Bytes": 16384.0,
        "Send Message Size Bytes": 16384.0,
        "Elapsed Time Seconds": 30.0,
        "Throughput 10^6bits/sec": 35710.68,
    }

    with pytest.raises(TypeError):
        testTypeNetPerf.netperf_parse("bogus-name", data)

    data = """MIGRATED TCP STREAM TEST from 0.0.0.0 (0.0.0.0) port 0 AF_INET to 10.131.0.167 () port 0 AF_INET : demo
Recv   Send    Send                          
Socket Socket  Message  Elapsed              
Size   Size    Size     Time     Throughput  
bytes  bytes   bytes    secs.    10^6bits/sec  

16384  16384    30.00    35710.68   
"""
    with pytest.raises(ValueError):
        testTypeNetPerf.netperf_parse("TCP_STREAM", data)
