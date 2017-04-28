#!/usr/bin/env python
#future comptability
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from builtins import (ascii, bytes, chr, dict, filter, hex, input,
                      int, map, next, oct, open, pow, range, round,
                      str, super, zip)

import os
from datetime import datetime
import time
from tzlocal import get_localzone
import sys
from queue import Queue
import threading

from plcclient import PlcClient
from database import Database

class TagReader(threading.Thread):
    def __init__(self, client, logCycle, tags, lock, queue):
        super().__init__()
        self._logCycle = logCycle
        self._client = client
        self._tags = tags
        self._queue = queue
        self._lock = lock
        logger.info('{} started. (cycle {})'.format(self.getName(), self._logCycle))
        self.start()
        
    def run(self):
        while True:
            with self._lock:
                logger.debug('{} - acquired lock'.format(self.getName()))
                if self._client.get_connected():
                    logger.debug('{} - client attempting to read tag/s'.format(self.getName()))
                    response = self._client.tag_read(self._tags)
                    if response is None:
                        logger.warning('{} - response from tag_read() was None'.format(self.getName()))
                        self._client.reset_connection()     #blocking call, returns when connection re-established
                        response = self._client.tag_read(self._tags)
                    self._queue.put(response)
            time.sleep(self._logCycle)

def main():
    logger.info('Start')

    #instantiate the db
    dbFile = 'logs.db'
    dbTable = 'data_log'
    dbColumns = (('Name', 'TEXT'), ('Value', 'REAL'), ('Time', 'TEXT'), ('Quality', 'INTEGER'), ('TZ', 'TEXT')) 
    db = Database(dbFile)
    
    #TODO need to implement the table creation script!!!!
#     if not db.table:
#         db.create(dbTable, dbColumns)
    
    #clear the table if desired
    clearTable = sys.argv[1] if len(sys.argv) == 2 else None
    if clearTable == 'clear': 
        db.cursor.execute('DELETE FROM {}'.format(dbTable))
        db.connection.commit()
    
    #instantiate plc client and connect
    plcClient = PlcClient('data_tags.cfg', 'plc.cfg')
    plcClient.connect()

    #fire off pool of threads that grab tags from plc at different logging cycles
    threadLock = threading.Lock()
    queue = Queue()
    for cycle in plcClient.cycles:
        TagReader(plcClient, cycle, plcClient.tagsByCycle[cycle], threadLock, queue)
 
    #do some work on collected tags
    onChangeLogsWere = {}
    tzid = get_localzone().zone
    while True:
        if queue.not_empty:
            dequeue = queue.get()
            
            cyclicLogs = {key.replace('{','[').replace('}',']'):dequeue[key] for key in dequeue.keys() if key in plcClient.tagsByAcqMode['cyclic']}
            onChangeLogs = {key.replace('{','[').replace('}',']'):dequeue[key] for key in dequeue.keys() if key in plcClient.tagsByAcqMode['on_change']}
            timestamp = datetime.utcnow().replace(microsecond=0).isoformat()
            
            #insert cyclic logs into db
            logger.info('Inserting cyclic logs into DB')
            for tag, val in cyclicLogs.items():
                print(tag, val, timestamp, tzid)
                db.insert(dbTable, (tag, val, timestamp, 1, tzid))
            
            #test on_change logs; if there was a change, insert into db    
            if onChangeLogs and cmp(onChangeLogs, onChangeLogsWere):
                logger.info('On-change log/s detected; Inserting into DB')
                for tag, val in onChangeLogs.items():
                    if (not onChangeLogsWere) or (onChangeLogs[tag] != onChangeLogsWere[tag]):
                        print(tag, val, timestamp, tzid)
                        db.insert(dbTable, (tag, val, timestamp, 1, tzid))
                onChangeLogsWere = onChangeLogs
         
    
if __name__ == "__main__": 
    import logging
    logger = logging.getLogger(__name__)
#     logging.basicConfig(stream=sys.stderr, level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)-5s - %(message)s')
    loggingWhitelist = ('root', 'plcclient', 'snap7', 'database', '__main__')
    loggingFilename = 'datalogger.log'
    loggingFormat = '%(asctime)s %(name) -15s %(levelname)-9s %(message)s'
    loggingDateFormat = '%Y-%m-%d %H:%M:%S'
    loggingLevel = logging.DEBUG
    
    class Whitelist(logging.Filter):
        def __init__(self, *whitelist):
            self.whitelist = [logging.Filter(name) for name in whitelist]
    
        def filter(self, record):
            return any(f.filter(record) for f in self.whitelist)

    logging.basicConfig(  
#         stream=sys.stderr, 
        filename=loggingFilename,
        level=loggingLevel,
        format=loggingFormat,
        datefmt=loggingDateFormat
        )
    for handler in logging.root.handlers:
        handler.addFilter(Whitelist(*loggingWhitelist))
    main()