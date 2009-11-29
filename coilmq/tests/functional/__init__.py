#
# Copyright 2009 Hans Lellelid
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Functional tests to test full stack (but not actual socket layer).
"""
import unittest

from coilmq.tests import mock

class ConnectionTest(unittest.TestCase):
    """ Test basic connection and disconnection by clients. """
    
    def setUp(self):
        self.conn = mock.MockConnection()
         
    
    