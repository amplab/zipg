#!/usr/bin/env python

import os
import sys

lib_path = os.path.dirname(os.path.realpath(sys.argv[0])) + "/lib"
print "Importing ZipG python client libraries from %s..." % lib_path

sys.path.append(lib_path)

from zipg import GraphQueryAggregatorService
from zipg.ttypes import *

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

zipgClient = None
zipgTransport = None
zipgProtocol = None

def zipgConnect(host = 'localhost', port = 11001):
  global zipgClient
  global zipgTransport
  global zipgProtocol
  
  # Make socket
  zipgTransport = TSocket.TSocket(host, port)

  # Buffering is critical. Raw sockets are very slow
  zipgTransport = TTransport.TBufferedTransport(zipgTransport)

  # Wrap in a protocol
  zipgProtocol = TBinaryProtocol.TBinaryProtocol(zipgTransport)

  # Create a client to use the protocol encoder
  zipgClient = GraphQueryAggregatorService.Client(zipgProtocol)

  # Connect!
  zipgTransport.open()
  
  print "ZipG Client is now available as zipgClient."
  
try:
  zipgConnect()
  
except Thrift.TException, tx:
  print '%s' % (tx.message)
  print 'Check your server status and retry connecting with zipgConnect(host, port)'


