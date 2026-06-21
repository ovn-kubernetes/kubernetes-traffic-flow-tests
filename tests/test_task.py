import json
import os
import pytest
import sys
import threading

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import task  # noqa: E402
import tftbase  # noqa: E402

BaseOutput = tftbase.BaseOutput
TaskOperation = task.TaskOperation


MAIN_THREAD = threading.get_native_id()


def test_task_operation_thread() -> None:

    call_count = [0]

    def action() -> BaseOutput:
        assert call_count == [0]
        call_count[0] += 1
        assert threading.get_native_id() != MAIN_THREAD
        return BaseOutput(msg="test1")

    op = task.TaskOperation(log_name="test", thread_action=action)
    op.start()
    res = op.finish()
    assert res == BaseOutput(msg="test1")
    assert call_count == [1]


def test_task_operation_nothread() -> None:

    call_count = [0]

    def action() -> BaseOutput:
        assert call_count == [0]
        call_count[0] += 1
        assert threading.get_native_id() == MAIN_THREAD
        return BaseOutput(msg="test1")

    op = task.TaskOperation(log_name="test", collect_action=action)
    op.start()
    res = op.finish()
    assert res == BaseOutput(msg="test1")
    assert call_count == [1]


def test_task_operation_thread_with_collect() -> None:

    call_count = [0]

    def action() -> str:
        assert call_count == [0]
        call_count[0] += 1
        assert threading.get_native_id() != MAIN_THREAD
        return "foo1"

    def collect(arg: str) -> BaseOutput:
        assert arg == "foo1"
        assert call_count == [1]
        call_count[0] += 1
        assert threading.get_native_id() == MAIN_THREAD
        return BaseOutput(msg="test1")

    op = task.TaskOperation(
        log_name="test",
        thread_action=action,
        collect_action=collect,
    )
    op.start()
    res = op.finish()
    assert res == BaseOutput(msg="test1")
    assert call_count == [2]


def test_task_operation_thread_with_collect_wrong() -> None:

    call_count = [0]

    def action() -> str:
        assert call_count == [0]
        call_count[0] += 1
        assert threading.get_native_id() != MAIN_THREAD
        return "foo1"

    def collect() -> BaseOutput:
        assert False, "should not be reached"

    # When we specify both a thread_action and a collect_action,
    # the former must return a value that is passed on. "collect"
    # here is broken, because it doesn't take the argument.
    #
    # Typing would catch this too.
    op = task.TaskOperation(
        log_name="test",
        thread_action=action,
        collect_action=collect,  # type: ignore
    )
    op.start()
    with pytest.raises(TypeError):
        op.finish()


def test_extract_server_remote_host_valid() -> None:
    server_json = json.dumps(
        {
            "start": {
                "connected": [
                    {
                        "socket": 5,
                        "local_host": "0.0.0.0",
                        "local_port": 5201,
                        "remote_host": "10.0.0.100",
                        "remote_port": 12345,
                    }
                ]
            }
        }
    )
    assert task.extract_server_remote_host(server_json) == "10.0.0.100"


def test_extract_server_remote_host_invalid_json() -> None:
    assert task.extract_server_remote_host("not json") is None


def test_extract_server_remote_host_missing_fields() -> None:
    assert task.extract_server_remote_host(json.dumps({"start": {}})) is None
    assert task.extract_server_remote_host(json.dumps({})) is None
    assert (
        task.extract_server_remote_host(json.dumps({"start": {"connected": []}}))
        is None
    )
