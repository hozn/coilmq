"""
Tests for authenticators.
"""
import os
import unittest
try:
    from io import StringIO
    unicode = str
except ImportError:
    from StringIO import StringIO

from pkg_resources import resource_stream, resource_filename

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

class SimpleAuthenticatorTest(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_constructor(self):
        """
        Test the with passing auth store in constructor.
        """
        auth = SimpleAuthenticator({'user': 'pass'})
        assert auth.authenticate('user', 'pass') == True
        assert auth.authenticate('user1', 'pass') == False
        assert auth.authenticate('user', 'pass2') == False

    def test_from_configfile(self):
        """
        Test the loading store from config file path.
        """
        filename = resource_filename('coilmq.tests.resources', 'auth.ini')
        auth = SimpleAuthenticator()
        auth.from_configfile(filename)
        assert auth.authenticate('juniper', 'b3rr1es') == True
        assert auth.authenticate('oak', 'ac$rrubrum') == True
        assert auth.authenticate('pinetree', 'str0bus') == True
        assert auth.authenticate('foo', 'bar') == False

    def test_from_configfile_fp(self):
        """
        Test loading store from file-like object.
        """
        with open(resource_filename('coilmq.tests.resources', 'auth.ini'),'r') as fp:
            auth = SimpleAuthenticator()
            auth.from_configfile(fp)

        assert auth.authenticate('juniper', 'b3rr1es') == True
        assert auth.authenticate('oak', 'ac$rrubrum') == True
        assert auth.authenticate('pinetree', 'str0bus') == True
        assert auth.authenticate('foo', 'bar') == False

    def test_from_configfile_invalid(self):
        """
        Test loading store with invalid file path.
        """
        filename = resource_filename('coilmq.tests.resources', 'auth-invlaid.ini')
        auth = SimpleAuthenticator()
        try:
            auth.from_configfile(filename)
            self.fail("Expected error with invalid filename.")
        except ValueError as e:
            pass

    def test_from_configfile_fp_invalid(self):
        """
        Test loading store with missing section in config.
        """
        fp = StringIO(u"[invalid]\nusername=password")
        auth = SimpleAuthenticator()
        try:
            auth.from_configfile(fp)
            self.fail("Expected error with missing section.")
        except ValueError as e:
            pass