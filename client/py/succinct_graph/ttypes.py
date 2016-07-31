#
# Autogenerated by Thrift Compiler (0.9.3)
#
# DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
#
#  options string: py
#

from thrift.Thrift import TType, TMessageType, TException, TApplicationException

from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol, TProtocol
try:
  from thrift.protocol import fastbinary
except:
  fastbinary = None



class ThriftAssoc:
  """
  Attributes:
   - srcId
   - dstId
   - atype
   - timestamp
   - attr
  """

  thrift_spec = (
    None, # 0
    (1, TType.I64, 'srcId', None, None, ), # 1
    (2, TType.I64, 'dstId', None, None, ), # 2
    (3, TType.I64, 'atype', None, None, ), # 3
    (4, TType.I64, 'timestamp', None, None, ), # 4
    (5, TType.STRING, 'attr', None, None, ), # 5
  )

  def __init__(self, srcId=None, dstId=None, atype=None, timestamp=None, attr=None,):
    self.srcId = srcId
    self.dstId = dstId
    self.atype = atype
    self.timestamp = timestamp
    self.attr = attr

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.I64:
          self.srcId = iprot.readI64()
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.I64:
          self.dstId = iprot.readI64()
        else:
          iprot.skip(ftype)
      elif fid == 3:
        if ftype == TType.I64:
          self.atype = iprot.readI64()
        else:
          iprot.skip(ftype)
      elif fid == 4:
        if ftype == TType.I64:
          self.timestamp = iprot.readI64()
        else:
          iprot.skip(ftype)
      elif fid == 5:
        if ftype == TType.STRING:
          self.attr = iprot.readString()
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('ThriftAssoc')
    if self.srcId is not None:
      oprot.writeFieldBegin('srcId', TType.I64, 1)
      oprot.writeI64(self.srcId)
      oprot.writeFieldEnd()
    if self.dstId is not None:
      oprot.writeFieldBegin('dstId', TType.I64, 2)
      oprot.writeI64(self.dstId)
      oprot.writeFieldEnd()
    if self.atype is not None:
      oprot.writeFieldBegin('atype', TType.I64, 3)
      oprot.writeI64(self.atype)
      oprot.writeFieldEnd()
    if self.timestamp is not None:
      oprot.writeFieldBegin('timestamp', TType.I64, 4)
      oprot.writeI64(self.timestamp)
      oprot.writeFieldEnd()
    if self.attr is not None:
      oprot.writeFieldBegin('attr', TType.STRING, 5)
      oprot.writeString(self.attr)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __hash__(self):
    value = 17
    value = (value * 31) ^ hash(self.srcId)
    value = (value * 31) ^ hash(self.dstId)
    value = (value * 31) ^ hash(self.atype)
    value = (value * 31) ^ hash(self.timestamp)
    value = (value * 31) ^ hash(self.attr)
    return value

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class ThriftEdgeUpdatePtr:
  """
  Attributes:
   - shardId
   - offset
  """

  thrift_spec = (
    None, # 0
    (1, TType.I64, 'shardId', None, None, ), # 1
    (2, TType.I64, 'offset', None, None, ), # 2
  )

  def __init__(self, shardId=None, offset=None,):
    self.shardId = shardId
    self.offset = offset

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.I64:
          self.shardId = iprot.readI64()
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.I64:
          self.offset = iprot.readI64()
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('ThriftEdgeUpdatePtr')
    if self.shardId is not None:
      oprot.writeFieldBegin('shardId', TType.I64, 1)
      oprot.writeI64(self.shardId)
      oprot.writeFieldEnd()
    if self.offset is not None:
      oprot.writeFieldBegin('offset', TType.I64, 2)
      oprot.writeI64(self.offset)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __hash__(self):
    value = 17
    value = (value * 31) ^ hash(self.shardId)
    value = (value * 31) ^ hash(self.offset)
    return value

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class ThriftSrcAtype:
  """
  Attributes:
   - src
   - atype
  """

  thrift_spec = (
    None, # 0
    (1, TType.I64, 'src', None, None, ), # 1
    (2, TType.I64, 'atype', None, None, ), # 2
  )

  def __init__(self, src=None, atype=None,):
    self.src = src
    self.atype = atype

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.I64:
          self.src = iprot.readI64()
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.I64:
          self.atype = iprot.readI64()
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('ThriftSrcAtype')
    if self.src is not None:
      oprot.writeFieldBegin('src', TType.I64, 1)
      oprot.writeI64(self.src)
      oprot.writeFieldEnd()
    if self.atype is not None:
      oprot.writeFieldBegin('atype', TType.I64, 2)
      oprot.writeI64(self.atype)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __hash__(self):
    value = 17
    value = (value * 31) ^ hash(self.src)
    value = (value * 31) ^ hash(self.atype)
    return value

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)
