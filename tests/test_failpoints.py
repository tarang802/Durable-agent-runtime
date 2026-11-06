import pytest

from durably.failpoints import parse


@pytest.mark.parametrize(("spec", "expected"), [
    ("after_claim", ("after_claim", 1, "exit")),
    ("after_claim:pause", ("after_claim", 1, "pause")),
    ("after_record_before_ack@3", ("after_record_before_ack", 3, "exit")),
    ("before_append@2:pause", ("before_append", 2, "pause")),
])
def test_parse(spec, expected):
    assert parse(spec) == expected
