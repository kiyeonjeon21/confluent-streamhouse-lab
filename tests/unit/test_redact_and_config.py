from __future__ import annotations

import base64

import pytest

from streamhouse_lab import config
from streamhouse_lab.redact import MASK, ApiKey, Secret, scrub
from tests.conftest import FAKE_SECRET, fake_outputs


def test_secret_never_renders(api_key: ApiKey) -> None:
    rendered = " ".join([str(api_key.secret), repr(api_key.secret), repr(api_key), f"{api_key}"])
    assert FAKE_SECRET not in rendered
    assert MASK in rendered


def test_basic_token_round_trips(api_key: ApiKey) -> None:
    decoded = base64.b64decode(api_key.basic_token().reveal()).decode()
    assert decoded == f"KEY:{FAKE_SECRET}"


def test_scrub_removes_secret_and_its_basic_token(api_key: ApiKey) -> None:
    text = f"error: auth {FAKE_SECRET} header Basic {api_key.basic_token().reveal()}"
    cleaned = scrub(text, api_key, None)
    assert FAKE_SECRET not in cleaned
    assert api_key.basic_token().reveal() not in cleaned


def test_config_repr_hides_secrets(lab_config_with_context: config.LabConfig) -> None:
    assert FAKE_SECRET not in repr(lab_config_with_context)


def test_config_without_context_reader(lab_config: config.LabConfig) -> None:
    assert lab_config.context_reader is None
    assert lab_config.lightning_query_endpoint == "https://sql.us-east-1.aws.confluent.cloud/query/v1"


def test_missing_required_output_is_a_clear_error() -> None:
    outputs = fake_outputs()
    del outputs["flink_compute_pool_id"]
    with pytest.raises(config.ConfigError, match="flink_compute_pool_id"):
        config.load(lambda: outputs)


def test_secret_is_falsy_when_empty() -> None:
    assert not Secret("")
    assert Secret("x")
