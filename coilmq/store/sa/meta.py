"""
Module to hold some "global" SA objects.

These objects are initialized by the SA store factory (i.e. L{coilmq.store.sa.make_sa})
"""
engine = None #: The SA engine
Session = None #: The SA Session (or Session-like callable) 
metadata = None #: The SA C{sqlalchemy.orm.MetaData} instance bound to the engine.
