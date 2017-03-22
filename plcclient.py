#!/usr/bin/env python
#future comptability
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from builtins import dict
from builtins import int
#from builtins import str
from builtins import super

import struct
import time
from configparser import ConfigParser
import logging
import sys

import snap7.client as S7
# import snap7.common

logger = logging.getLogger(__name__)

dataDict = {
    'bool':   {'format_char':'B',     'size':1},
    'byte':   {'format_char':'B',     'size':1},
    'word':   {'format_char':'H',     'size':2},
    'dword':  {'format_char':'I',     'size':4},
    'usint':  {'format_char':'B',     'size':1},
    'sint':   {'format_char':'b',     'size':1},
    'uint':   {'format_char':'H',     'size':2},
    'int':    {'format_char':'h',     'size':2},
    'udint':  {'format_char':'I',     'size':4},
    'dint':   {'format_char':'i',     'size':4},
    'real':   {'format_char':'f',     'size':4},
    'lreal':  {'format_char':'d',     'size':8},
    'ulint':  {'format_char':'q',     'size':8},
    'string': {'format_char':'str',   'size':256},
    'time':   {'format_char':'I',     'size':4}
    }

class PlcClient(S7.Client):
    """
    S7Client extends the snap7.client.Client to play more friendly with reading tag values out of the PLC.
    """
    def __init__(self, *cfg_files):
        """
        cfg_files can be a single file or a comma separated list of config files.  Config file/s need to 
        contain appropriate tag and PLC connection information 
        """
        super().__init__()
        self._create(*cfg_files)
          
    def _create(self, *cfg_files):
        #parse config files for plc connection info and tag details
        self._config = ConfigParser()
        self._config.read(list(cfg_files))
        
        #build list of tags defined in the config file
        self._tagList = self._config.sections()
        self._tagList.remove('plc_connection')
        self._tagList.remove('connection_persistance')
             
        #build a sorted list of logging cycles that have been defined in config    
        self._cycles = []
        for cycle in [self._config[tag].getint('logging_cycle') for tag in self._tagList]:
            if cycle not in self._cycles:
                self._cycles.append(cycle)
        self._cycles.sort()
  
        #build a dictionary where the keys are logging frequencies (integer seconds) and the values are lists of tags    
        tempList=[]
        tempTagList=[]
        for cycle in self._cycles:
            for tag in self._tagList:
                if cycle == self._config[tag].getint('logging_cycle'):
                    tempTagList.append(tag)
            tempList.append([cycle,tempTagList[:]]) 
            del tempTagList[:]
        self._tagsByCycle = dict(tempList)
  
        #build a list of acquistion modes
        del tempList[:]
        del tempTagList[:]
        self._acqModes = []
        for mode in [self._config[tag].get('acquisition_mode') for tag in self._tagList]:
            if mode not in self._acqModes:
                self._acqModes.append(mode)
                
        #build a dictionary where keys are acquisition modes and the values are lists of tags        
        for mode in self._acqModes:
            for tag in self._tagList:
                if mode == self._config[tag].get('acquisition_mode'):
                    tempTagList.append(tag)
            tempList.append([mode, tempTagList[:]])
            del tempTagList[:]
        self._tagsByAcqMode = dict(tempList)
        del tempTagList
        del tempList  
        
    def connect(self):
        """
        Overloaded connect method.  Uses PLC connection settings defined in config file.
        """
        _reconnectCount = 0
        while _reconnectCount < self._config['connection_persistance'].getint('reconnect_attempts') and not super().get_connected():
            try:
                super().connect(self._config['plc_connection']['address'], self._config['plc_connection'].getint('rack'),\
                                    self._config['plc_connection'].getint('slot'), self._config['plc_connection'].getint('tcp_port'))  
                logger.info('Connect successful')
            except:
                _reconnectCount += 1
                logger.warning('Connect failed, attempting to retry ({}) in {} seconds'.format\
                               (_reconnectCount, self._config['connection_persistance'].getint('reconnect_timeout')))
                time.sleep(self._config['connection_persistance'].getint('reconnect_timeout'))
        if not super().get_connected():
            logger.error('Could not establish a connection.  An Exception will be raised')
            #TODO: do a better job of raising exception
            raise Exception('Could not establish a connection')      
            
    def reset_connection(self):
        logger.info('Resetting connection')
        super().disconnect()
        self.connect()
        
    def tag_read(self, tags):
        """
        Returns a dictionary object of tag:value pairs.  The method can be called with tags argument as a single string or list or strings
        """
        logger.debug('tag_read {}'.format(tags))
        response = []
        if type(tags) is not type([]):
            tags = [tags]
        try:
            for tag in tags: 
                db = self._config[tag].getint('database', None)
                offset = self._config[tag].getint('offset')
                size = dataDict[self._config[tag].get('data_type')].get('size')
                formatChar = dataDict[self._config[tag].get('data_type')].get('format_char')
                bit = self._config[tag].getint('bit', None)
                      
                if db is None:
                    byteArray = super().ab_read(offset, size)
                else:
                    byteArray = super().db_read(db, offset, size)
                    
                if formatChar is 'str':
                    value = byteArray[2:].decode('utf-8')
                else:
                    byteString = struct.pack(str(size)+'B', *byteArray)
                    value = struct.unpack('>'+formatChar, byteString)[0]
                    if bit is not None:
                        value = int(bool(value & 1 << bit))
                
                response.append((tag,value))
        except Exception as e:
            #exc_type, exc_obj, exc_tb = sys.exc_info()
            exc_tb = sys.exc_info()[2]
            logger.error('tag_read failed - {} - line {}'.format(e, exc_tb.tb_lineno))
            response = None
        finally:
            if response is not None:
                response = dict(response)
            return response
        
    @property
    def tagList(self):
        return self._tagList 
    
    @property
    def tagsByCycle(self):
        return self._tagsByCycle
    
    @property
    def tagsByAcqMode(self):
        return self._tagsByAcqMode
    
    @property
    def acqModes(self):
        return self._acqModes
    
    @property
    def cycles(self):
        return self._cycles
  
def main():
    plc = PlcClient('tags.cfg', 'plc.cfg')
    plc.connect()
    while True:
        readResult = plc.tag_read(plc.tagList)
        if readResult is None: 
            plc.reset_connection()
        else:
            for tag, value in readResult.items():
                print(tag, value)
            time.sleep(2)
        
if __name__ == "__main__":   
    import time
    import sys
    import logging
    
    loggingWhitelist = ('root', 'plcclient', 'snap7', 'database', '__main__')
    loggingFilename = 'debug.log'
    loggingFormat = '%(asctime)s %(name) -15s %(levelname)-9s %(message)s'
    loggingDateFormat = '%Y-%m-%d %H:%M:%S'
    loggingLevel = logging.DEBUG
    
    class Whitelist(logging.Filter):
        def __init__(self, *whitelist):
            self.whitelist = [logging.Filter(name) for name in whitelist]
    
        def filter(self, record):
            return any(f.filter(record) for f in self.whitelist)

    logging.basicConfig(  
        stream=sys.stderr, 
#         filename=loggingFilename,
        level=loggingLevel,
        format=loggingFormat,
        datefmt=loggingDateFormat
        )
    for handler in logging.root.handlers:
        handler.addFilter(Whitelist(*loggingWhitelist))
    main()
