# -*- coding: utf-8 -*-
"""
Test of the QueueManager when using a DBM backend (store).
"""
import os
import os.path
import shutil

from coilmq.store.dbm import DbmQueue

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

class DBMQueueManagerTest(QueueManagerTest):
    """ Run all the tests from BasicTest using a SQLite database store. """
    
    def _queuestore(self):
        """
        Returns the configured L{QueueStore} instance to use.
        
        Can be overridden by subclasses that wish to change out any queue store parameters.
        
        @rtype: L{QueueStore}
        """
        data_dir = os.path.abspath(os.path.join(os.getcwd(), 'data'))
        if os.path.exists(data_dir):
            shutil.rmtree(data_dir)
        os.makedirs(data_dir)
            
        data_dir = './data'
        cp_ops = 100
        cp_timeout = 20
        store = DbmQueue(data_dir, checkpoint_operations=cp_ops, checkpoint_timeout=cp_timeout)
        return store