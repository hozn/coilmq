# -*- coding: utf-8 -*-
"""
Functional tests that use a SQLite storage backends and default
scheduler implementations.
"""
from sqlalchemy import engine_from_config

from coilmq.store.sa import SAQueue, init_model

from coilmq.tests.test_queue import QueueManagerTest

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

class SAQueueManagerTest(QueueManagerTest):
    """ Run all the tests from BasicTest using a SQLite database store. """
    
    def _queuestore(self):
        """
        Returns the configured L{QueueStore} instance to use.
        
        Can be overridden by subclasses that wish to change out any queue store parameters.
        
        @rtype: L{QueueStore}
        """
        configuration = {'qstore.sqlalchemy.url': 'sqlite:///data/coilmq.db'}
        engine = engine_from_config(configuration, 'qstore.sqlalchemy.')
        init_model(engine, drop=True)
        return SAQueue()