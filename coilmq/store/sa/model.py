"""
Definition of the datamodel required for SA storage backend.
"""

from sqlalchemy import Table, Column, Integer, String, PickleType, DateTime
from sqlalchemy.sql import func

from coilmq.store.sa import meta

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

frames_table = None #: The C{sqlalchemy.Table} set by L{setup_tables}

def setup_tables(create=True):
    """
    Binds the model classes to registered metadata and engine and (potentially) 
    creates the db tables.
    
    This function expects that you have bound the L{meta.metadata} and L{meta.engine}.
    
    @param create
    @type create: C{bool}
    """
    global frames_table
    frames_table = Table('frames', meta.metadata,
                         Column('message_id', String(255), primary_key=True),
                         Column('sequence', Integer, primary_key=False, autoincrement=True),
                         Column('destination', String(255), index=True),
                         Column('frame', PickleType),
                         Column('queued', DateTime, default=func.now()))
    
    if create:
        meta.metadata.create_all()