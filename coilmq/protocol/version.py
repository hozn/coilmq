from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from typing_extensions import Self


@dataclass(frozen=True, order=True)
class Version:
    """Representation of a STOMP version."""

    major: int
    minor: int

    @classmethod
    def from_str(cls, value: str) -> Self:
        """Parse the string representation of a STOMP version.

        :param value: a version, like ``"1.0"``

        :raises ValueError: if the value is not a valid version

        :return: structured representation of the version

        """
        match = re.match(r"(?P<major>\d+)\.(?P<minor>\d+)$", value)

        if not match:
            msg = "Malformed version"
            raise ValueError(msg)

        groups = match.groupdict()
        major = int(cast("str", groups["major"]))
        minor = int(cast("str", groups["minor"]))

        return cls(major, minor)

    def __str__(self) -> str:
        return f"{self.major}.{self.minor}"
