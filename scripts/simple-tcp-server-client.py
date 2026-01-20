#!/usr/bin/env python3

import argparse
import os
import random
import shlex
import socket
import string
import sys
import time

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from typing import Any
from typing import Callable
from typing import Optional

DEFAULT_ADDR = "127.0.0.1"
DEFAULT_BUFSIZE = 65495
DEFAULT_DURATION = 0.0
DEFAULT_NUM_CLIENTS = 1
DEFAULT_PORT = 5201
DEFAULT_SLEEP = 0.001

global_start_time = time.monotonic()


def _print(msg: str) -> None:
    now = datetime.now()
    timestamp = now.strftime("%H:%M:%S.") + f"{now.microsecond // 100:04d}"
    print(f"[{timestamp}] {msg}")


def create_socket() -> socket.socket:
    return socket.socket(socket.AF_INET, socket.SOCK_STREAM)


@contextmanager
def socket_timeout(
    log_name: str,
    s: socket.socket,
    start_time: float,
    duration: float,
    *,
    done_msg: Optional[Callable[[], None]] = None,
) -> Iterator[socket.socket]:
    if duration <= 0.0:
        t = None
    else:
        t = (start_time + duration) - time.monotonic()
        if t <= 0.0:
            _print(f"{log_name}: duration expired. Quit")
            sys.exit(0)
    s.settimeout(t)

    try:
        yield s
    except socket.timeout:
        if done_msg is not None:
            done_msg()
        _print(f"{log_name}: duration expired. Quit")
        sys.exit(0)


def sleep_timeout(
    log_name: str,
    start_time: float,
    duration: float,
    sleep_time: float,
    *,
    exit_code: Optional[int] = None,
    done_msg: Optional[Callable[[], None]] = None,
) -> bool:
    if sleep_time <= 0.0:
        if duration <= 0.0:
            return True
        t = (start_time + duration) - time.monotonic()
    else:
        if duration <= 0.0:
            time.sleep(sleep_time)
            return True
        t = (start_time + duration) - time.monotonic()
        if t > 0.0:
            time.sleep(min(t, sleep_time))
            t = (start_time + duration) - time.monotonic()

    if t > 0.0:
        return True

    if done_msg is not None:
        done_msg()
    if exit_code is None:
        exit_code = 0
    _print(f"{log_name}: duration expired. Quit")
    sys.exit(exit_code)


def run_server(
    *,
    s_addr: str = DEFAULT_ADDR,
    port: int = DEFAULT_PORT,
    sleep: float = DEFAULT_SLEEP,
    bufsize: int = DEFAULT_BUFSIZE,
    duration: float = DEFAULT_DURATION,
    num_clients: int = DEFAULT_NUM_CLIENTS,
    verbose: bool = False,
) -> None:

    start_time = global_start_time

    s = create_socket()

    _print(f"server: listen on {s_addr}:{port}")
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((s_addr, port))
    s.listen(1)

    client_count = 0

    while 1:
        if num_clients > 0 and client_count >= num_clients:
            _print(f"server: number of clients {num_clients} reached. Quit")
            sys.exit(0)
        with socket_timeout("server", s, start_time, duration):
            conn, addr = s.accept()
        client_count += 1
        _print(
            f"server: new connection #{client_count} on port {port} from addr {addr}."
        )
        msg_count = 0
        last_time = -1000.0
        rcv_len = 0
        while 1:

            def done_msg() -> None:
                _print(
                    f"server: {msg_count} chunks received and returned ({rcv_len} bytes) in total"
                )

            with socket_timeout(
                "server", conn, start_time, duration, done_msg=done_msg
            ):
                try:
                    data = conn.recv(bufsize)
                except Exception:
                    data = b""
            if not data:
                done_msg()
                _print(f"server: connection {addr} closed")
                break
            msg_count += 1
            rcv_len += len(data)
            with socket_timeout(
                "server", conn, start_time, duration, done_msg=done_msg
            ):
                try:
                    conn.sendall(data)
                except ConnectionResetError:
                    done_msg()
                    _print(f"server: connection {addr} closed")
                    break
            now_time = time.monotonic()
            if now_time - last_time >= 1.0:
                _print(
                    f"server: {msg_count} chunks received and returned ({rcv_len} bytes)"
                )
                last_time = now_time
            sleep_timeout("server", start_time, duration, sleep, done_msg=done_msg)
        conn.close()


printable = (string.ascii_letters + string.digits).encode("ascii")


def _random_ascii(n: int) -> bytes:
    return bytes(random.choice(printable) for _ in range(n))


def run_client(
    *,
    s_addr: str = DEFAULT_ADDR,
    port: int = DEFAULT_PORT,
    sleep: float = DEFAULT_SLEEP,
    bufsize: int = DEFAULT_BUFSIZE,
    duration: float = DEFAULT_DURATION,
    verbose: bool = False,
    echo_ascii: bool = False,
) -> None:

    start_time = global_start_time

    _print(f"client: connecting to {s_addr}:{port}")
    s = create_socket()

    first_attempt = True
    connected = False
    while not connected:
        try:
            with socket_timeout("client", s, start_time, duration):
                s.connect((s_addr, port))
        except (ConnectionRefusedError, ConnectionAbortedError):
            if first_attempt:
                first_attempt = False
                _print("client: connection refused. Retry")
            sleep_timeout("client", start_time, min(duration, 60.0), 0.5, exit_code=1)
        else:
            connected = True
    _print(f"client: connected to {s_addr}:{port}")

    msg_count = 0
    rcv_len = 0
    while 1:

        def done_msg() -> None:
            _print(
                f"client: {msg_count} chunks send and received ({rcv_len} bytes) in total"
            )

        i_bufsize = random.randint(1, bufsize)
        if echo_ascii:
            snd_data = _random_ascii(i_bufsize)
        else:
            snd_data = os.urandom(i_bufsize)
        if verbose:
            _print(f"client: echo random chunk of {i_bufsize} bytes")
        with socket_timeout("client", s, start_time, duration, done_msg=done_msg):
            s.sendall(snd_data)
        msg_count += 1

        # We first read all the data we sent back.
        rcv_data = b""
        while len(rcv_data) < len(snd_data):
            timeout_sec = 1
            s.settimeout(timeout_sec)
            try:
                r = s.recv(bufsize)
            except socket.timeout:
                s.settimeout(20)
                try:
                    r = s.recv(bufsize)
                except socket.timeout:
                    _print(
                        f"client: unexpected response. Timeout after {timeout_sec} seconds to receive a response. Even after waiting additional 20 seconds no response was received"
                    )
                else:
                    _print(
                        f"client: unexpected response. Timeout after {timeout_sec} seconds to receive a response. Aftware waiting some more, {len(r)} bytes were received"
                    )
                sys.exit(1)
            assert r
            rcv_data += r

        if rcv_data != snd_data:
            if verbose:
                _print("client: was expecting    {repr(snd_data)}")
                _print("client: received instead {repr(rcv_data)}")
            _print("client: unexpected response. Expect an echo of the data we sent")
            sys.exit(1)

        rcv_len += len(rcv_data)

        if msg_count % 10000 == 0:
            _print(
                f"client: {msg_count} chunks send and received ({rcv_len} bytes) for {s.getsockname()}->{s_addr}:{port}"
            )
        sleep_timeout("client", start_time, duration, sleep, done_msg=done_msg)


def run_exec(
    exec_url: str,
    exec_args: list[str],
    exec_insecure: bool,
    server: bool,
    s_addr: str,
    port: int,
    duration: float,
) -> None:
    log_prefix = "server:" if server else "client: "

    import urllib.request
    import urllib.parse

    path = urllib.parse.urlparse(exec_url).path
    basename = os.path.basename(path)

    filename = f"/tmp/simple-exec{'.'+basename if basename else ''}"

    if exec_insecure:
        import ssl

        # Hack up ssl._create_default_https_context so that urlretrieve()
        # ignores SSL errors.
        ssl_untyped: Any = ssl
        ssl_untyped._create_default_https_context = (
            ssl_untyped._create_unverified_context
        )

    _print(f"{log_prefix}downloading exec URL {repr(exec_url)} to {filename}")
    urllib.request.urlretrieve(exec_url, filename)

    os.chmod(filename, 0o755)

    env = os.environ.copy()
    env["SERVER"] = "1" if server else "0"
    env["ADDR"] = s_addr
    env["PORT"] = str(port)
    env["DURATION"] = str(duration)
    env["ORIG_ARGS_N"] = str(len(sys.argv))
    for idx, a in enumerate(sys.argv):
        env[f"ORIG_ARGS_{idx}"] = sys.argv[idx]

    os.execve(filename, [filename] + exec_args, env)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Simple TCP echo server/client")
    parser.add_argument(
        "-p",
        "--port",
        type=int,
        help="TCP port to listen/connect (default: {DEFAULT_PORT})",
        default=DEFAULT_PORT,
    )
    parser.add_argument(
        "-a",
        "--addr",
        type=str,
        help=f"IP address to listen/connect (default: {DEFAULT_ADDR}",
        default=DEFAULT_ADDR,
    )
    parser.add_argument(
        "-s",
        "--server",
        action="store_true",
        help="Whether to run as server or client (default)",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        help="How many second to sleep between client send or server receive (default: {DEFAULT_SLEEP})",
        default=DEFAULT_SLEEP,
    )
    parser.add_argument(
        "--bufsize",
        type=int,
        help="The maximum size of the chunk send at once on the TCP stream (default: {DEFAULT_BUFSIZE})",
        default=DEFAULT_BUFSIZE,
    )
    parser.add_argument(
        "-d",
        "--duration",
        type=float,
        help=f"How long before quitting (0 means infinity) (default: {DEFAULT_DURATION})",
        default=DEFAULT_DURATION,  # noqa: E225
    )
    parser.add_argument(
        "--num-clients",
        type=int,
        help=f"For the server, how many clients are accepted (server can only handle one client at a time) (default: {DEFAULT_NUM_CLIENTS})",
        default=DEFAULT_NUM_CLIENTS,  # noqa: E225
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose output",
    )
    parser.add_argument(
        "--echo-ascii",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="For the echo random data, only generate printable ASCII characters",
    )
    parser.add_argument(
        "--exec",
        default=None,
        help='A HTTP URL to a script. If set, this script is downloaded and executed (set a shebang!). Environment variables SERVER, ADDR, PORT, DURATION are set and "--exec-args" options are passed. This allows to easily hack the code that runs by injecting a script from the internet.',
    )
    parser.add_argument(
        "-k",
        "--exec-insecure",
        action="store_true",
        help='If set to true, ignore SSL errors for downloading "--exec" script',
    )

    class AppendExecArgs(argparse.Action):
        def __call__(
            self,
            parser: argparse.ArgumentParser,
            namespace: argparse.Namespace,
            values: Any,
            option_string: Optional[str] = None,
        ) -> None:
            if option_string == "--exec-args":
                namespace.exec_args.extend(shlex.split(values))
            else:
                namespace.exec_args.append(values)

    parser.add_argument(
        "--exec-args",
        action=AppendExecArgs,
        dest="exec_args",
        default=[],
        help='If "--exec" is set, specify the command line argument passed to the script. The parameter is parsed with shlex.split() (use shlex.quote() to ensure it is not split or use "--exec-arg" option). Can be specified multiple times and combined with "--exec-arg", in which case all entries are concatenated.',
    )
    parser.add_argument(
        "-E",
        "--exec-arg",
        action=AppendExecArgs,
        dest="exec_args",
        help='If "--exec" is set, specify the command line argument passed to the script. Similar to "--exec-args", but this is a single command line argument used as-is. Can be specified multiple times and combined with "--exec-args", in which all case entries are concatenated.',
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.exec:
        run_exec(
            exec_url=args.exec,
            exec_args=args.exec_args,
            exec_insecure=args.exec_insecure,
            server=args.server,
            s_addr=args.addr,
            port=args.port,
            duration=args.duration,
        )
    elif args.server:
        run_server(
            s_addr=args.addr,
            port=args.port,
            sleep=args.sleep,
            bufsize=args.bufsize,
            duration=args.duration,
            num_clients=args.num_clients,
            verbose=args.verbose,
        )
    else:
        run_client(
            s_addr=args.addr,
            port=args.port,
            sleep=args.sleep,
            bufsize=args.bufsize,
            duration=args.duration,
            verbose=args.verbose,
            echo_ascii=args.echo_ascii,
        )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
