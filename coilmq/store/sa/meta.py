"""
Module to hold shared SQLAlchemy state.

These objects are set by the L{coilmq.store.sa.init_model} function.
"""
#: The SA engine.
engine = None

#: The SA Session (or Session-like callable)
Session = None

#: The SA C{sqlalchemy.MetaData} instance bound to the engine.
metadata = None
