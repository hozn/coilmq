"""Functional tests that use a SQLite storage backends and default
scheduler implementations.
"""

import os
import os.path

import pytest
from sqlalchemy import engine_from_config

from coilmq.store.sa import SAQueue, init_model
from tests.functional.test_memory import (
    TestServerWithDefaultClasses as _TestServerWithDefaultClasses,
)

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


@pytest.fixture
def store() -> SAQueue:
    """Returns the :func:`server` fixture's ``store`` argument."""
    data_dir = os.path.join(os.getcwd(), "data")
    os.makedirs(data_dir, exist_ok=True)
    configuration = {"qstore.sqlalchemy.url": "sqlite:///data/coilmq.db"}
    engine = engine_from_config(configuration, "qstore.sqlalchemy.")
    init_model(engine, drop=True)

    return SAQueue()


class TestServerWithSAQueue(_TestServerWithDefaultClasses):
    """Run all the tests from :class:`TestServerWithDefaultClasses` using a :class:`SAQueue` store."""
