"""JSON-RPC parsing: dialect corners, batch, notifications, hostile ids."""

import json

import pytest

from celestia_plugin.jsonrpc import (
    INVALID_REQUEST,
    PARSE_ERROR,
    RpcError,
    parse_message,
)


def test_single_request_parses():
    (req,) = parse_message(json.dumps({"jsonrpc": "2.0", "id": 7, "method": "m", "params": {"a": 1}}))
    assert req.method == "m"
    assert req.id == 7
    assert req.params == {"a": 1}
    assert req.is_notification is False


def test_missing_id_is_notification():
    (req,) = parse_message(json.dumps({"jsonrpc": "2.0", "method": "m"}))
    assert req.is_notification is True
    assert req.id is None


def test_null_id_is_a_request_not_a_notification():
    # JSON-RPC 2.0 §4.1: id MAY be null on the wire — it is still a request.
    (req,) = parse_message(json.dumps({"jsonrpc": "2.0", "id": None, "method": "m"}))
    assert req.is_notification is False


def test_parse_error_on_garbage():
    with pytest.raises(RpcError) as excinfo:
        parse_message(b"\xff\xfe not json")
    assert excinfo.value.code == PARSE_ERROR


@pytest.mark.parametrize(
    "entry",
    [
        {"id": 1, "method": "m"},                       # jsonrpc missing
        {"jsonrpc": "1.0", "id": 1, "method": "m"},     # wrong version
        {"jsonrpc": "2.0", "id": 1},                    # method missing
        {"jsonrpc": "2.0", "id": 1, "method": ""},      # empty method
        {"jsonrpc": "2.0", "id": 1.5, "method": "m"},   # fractional id
        {"jsonrpc": "2.0", "id": [1], "method": "m"},   # array id
        {"jsonrpc": "2.0", "id": True, "method": "m"},  # bool id (int subclass)
        {"jsonrpc": "2.0", "id": 1, "method": "m", "params": [1, 2]},  # positional
        {"jsonrpc": "2.0", "id": 1, "method": "m", "params": "x"},     # string params
    ],
)
def test_invalid_entries_rejected(entry):
    with pytest.raises(RpcError) as excinfo:
        parse_message(json.dumps(entry))
    assert excinfo.value.code == INVALID_REQUEST


def test_batch_parses_to_list():
    batch = json.dumps(
        [
            {"jsonrpc": "2.0", "id": 1, "method": "a"},
            {"jsonrpc": "2.0", "method": "b"},  # notification
        ]
    )
    reqs = parse_message(batch)
    assert [r.method for r in reqs] == ["a", "b"]
    assert reqs[1].is_notification is True


def test_empty_batch_is_invalid():
    with pytest.raises(RpcError) as excinfo:
        parse_message("[]")
    assert excinfo.value.code == INVALID_REQUEST


def test_batch_with_invalid_entry_is_invalid():
    with pytest.raises(RpcError):
        parse_message(json.dumps([{"jsonrpc": "2.0", "id": 1, "method": "a"}, {"nope": True}]))


def test_params_default_to_empty_dict():
    (req,) = parse_message(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "m"}))
    assert req.params == {}
