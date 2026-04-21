"""Tests for authenticators."""

import re

try:
    from importlib.resources import as_file, files
except ImportError:  # pragma: no cover
    from importlib_resources import as_file, files
from io import StringIO

import pytest

from coilmq.auth.simple import SimpleAuthenticator

__authors__ = ['"Hans Lellelid" <hans@xmpl.org>']
__copyright__ = "Copyright 2009 Hans Lellelid"
__license__ = """Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License."""


class TestSimpleAuthenticator:
    def test_constructor(self) -> None:
        """Test the with passing auth store in constructor."""
        auth = SimpleAuthenticator({"user": "pass"})
        assert auth.authenticate("user", "pass") == True
        assert auth.authenticate("user1", "pass") == False
        assert auth.authenticate("user", "pass2") == False

    def test_from_configfile(self) -> None:
        """Test the loading store from config file path."""
        auth = SimpleAuthenticator()
        with as_file(files("tests.resources").joinpath("auth.ini")) as filename:
            auth.from_configfile(filename)
        assert auth.authenticate("juniper", "b3rr1es") == True
        assert auth.authenticate("oak", "ac$rrubrum") == True
        assert auth.authenticate("pinetree", "str0bus") == True
        assert auth.authenticate("foo", "bar") == False

    def test_from_configfile_fp(self) -> None:
        """Test loading store from file-like object."""
        auth = SimpleAuthenticator()
        with as_file(
            files("tests.resources").joinpath("auth.ini")
        ) as filename, filename.open() as fp:
            auth.from_configfile(fp)

        assert auth.authenticate("juniper", "b3rr1es") == True
        assert auth.authenticate("oak", "ac$rrubrum") == True
        assert auth.authenticate("pinetree", "str0bus") == True
        assert auth.authenticate("foo", "bar") == False

    def test_from_configfile_invalid(self) -> None:
        """Test loading store with invalid file path."""
        auth = SimpleAuthenticator()
        with as_file(files("tests.resources").joinpath("auth-invalid.ini")) as filename:  # noqa: SIM117
            with pytest.raises(
                ValueError, match=r"Could not parse auth file: .+?/auth-invalid\.ini"
            ):
                auth.from_configfile(filename)

    def test_from_configfile_fp_invalid(self) -> None:
        """Test loading store with missing section in config."""
        fp = StringIO("[invalid]\nusername=password")
        auth = SimpleAuthenticator()
        with pytest.raises(
            ValueError, match=re.escape("Config file contains no [auth] section.")
        ):
            auth.from_configfile(fp)
