#!/usr/bin/python
import pymongo
import re
import time
from pprint import pprint # pretty printer
from pymongo.errors import AutoReconnect

class OplogWatcher(object):
    def __init__(self, db=None, collection=None, poll_time=1.0, connection=None, start_now=True):
        if collection is not None:
            if db is None:
                raise ValueError('must specify db if you specify a collection')
            self._ns_filter = db + '.' + collection
        elif db is not None:
            self._ns_filter = re.compile(r'^%s\.' % db)
        else:
            self._ns_filter = None

        self.poll_time = poll_time
        self.connection = connection or pymongo.Connection()

        if start_now:
            self.start()

    @staticmethod
    def __get_id(op):
        id = None
        o2 = op.get('o2')
        if o2 is not None:
            id = o2.get('_id')

        if id is None:
            id = op['o'].get('_id')

        return id

    def start(self):
        oplog = self.connection.local['oplog.$main']
        ts = oplog.find().sort('$natural', -1)[0]['ts']
        while True:
            if self._ns_filter is None: 
                filter = {}
            else:
                filter = {'ns': self._ns_filter}
            filter['ts'] = {'$gt': ts}
            try:
                cursor = oplog.find(filter, tailable=True)
                while True:
                    for op in cursor:
                        ts = op['ts']
                        id = self.__get_id(op)
                        self.all_with_noop(ns=op['ns'], ts=ts, op=op['op'], id=id, raw=op)
                    time.sleep(self.poll_time)
                    if not cursor.alive:
                        break
            except AutoReconnect:
                time.sleep(self.poll_time)

    def all_with_noop(self, ns, ts, op, id, raw):
        if op == 'n':
            self.noop(ts=ts)
        else:
            self.all(ns=ns, ts=ts, op=op, id=id, raw=raw)

    def all(self, ns, ts, op, id, raw):
        if op == 'i':
            self.insert(ns=ns, ts=ts, id=id, obj=raw['o'], raw=raw)
        elif op == 'u':
            self.update(ns=ns, ts=ts, id=id, mod=raw['o'], raw=raw)
        elif op == 'd':
            self.delete(ns=ns, ts=ts, id=id, raw=raw)
        elif op == 'c':
            self.command(ns=ns, ts=ts, cmd=raw['o'], raw=raw)
        elif op == 'db':
            self.db_declare(ns=ns, ts=ts, raw=raw)

    def noop(self, ts):
        pass

    def insert(self, ns, ts, id, obj, raw, **kw):
        pass

    def update(self, ns, ts, id, mod, raw, **kw):
        pass

    def delete(self, ns, ts, id, raw, **kw):
        pass

    def command(self, ns, ts, cmd, raw, **kw):
        pass

    def db_declare(self, ns, ts, **kw):
        pass

class OplogPrinter(OplogWatcher):
    def all(self, **kw):
        pprint (kw)
        print #newline

if __name__ == '__main__':
    OplogPrinter()
