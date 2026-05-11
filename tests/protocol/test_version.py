from __future__ import annotations

from contextlib import nullcontext
from typing import TYPE_CHECKING

import pytest

from coilmq.protocol.version import Version

if TYPE_CHECKING:
    from contextlib import AbstractContextManager


@pytest.mark.parametrize(
    ("value", "expectation"),
    [
        ("1.0", nullcontext(Version(1, 0))),
        ("1.1", nullcontext(Version(1, 1))),
        ("1.2", nullcontext(Version(1, 2))),
        ("10.0", nullcontext(Version(10, 0))),
        ("", pytest.raises(ValueError, match="Malformed version")),
        ("1", pytest.raises(ValueError, match="Malformed version")),
        ("1..2", pytest.raises(ValueError, match="Malformed version")),
    ],
)
def test_version_from_str(
    value: str,
    expectation: AbstractContextManager[Version] | pytest.RaisesExc[ValueError],
) -> None:
    """Verify :meth:`Version.from_str` behavior."""
    with expectation as e:
        actual = Version.from_str(value)
        assert actual == e


@pytest.mark.parametrize(
    ("value", "expectation"),
    (
        (Version(1, 0), "1.0"),
        (Version(1, 1), "1.1"),
        (Version(1, 2), "1.2"),
    ),
)
def test_version_to_str(
    value: Version,
    expectation: str,
) -> None:
    """Verify correct conversion from :class:`Version` to :class:`str`."""
    actual = str(value)

    assert actual == expectation


@pytest.mark.parametrize(
    ("value", "expectation"),
    [
        ({Version(1, 0)}, Version(1, 0)),
        ({Version(1, 0), Version(1, 2)}, Version(1, 2)),
    ],
)
def test_max_version(value: set[Version], expectation: Version) -> None:
    """Verify ordering of :class:`Version` instances."""
    actual = max(value)

    assert actual == expectation
