import grpc
from grpc.framework.common import cardinality
from grpc.framework.interfaces.face import utilities as face_utilities

import message_pb2 as message__pb2
import message_pb2 as message__pb2
import message_pb2 as message__pb2
import message_pb2 as message__pb2
import message_pb2 as message__pb2
import message_pb2 as message__pb2
import message_pb2 as message__pb2
import message_pb2 as message__pb2


class SpiderServerStub(object):

  def __init__(self, channel):
    """Constructor.

    Args:
      channel: A grpc.Channel.
    """
    self.req = channel.unary_unary(
        '/DistributeSpider.SpiderServer/req',
        request_serializer=message__pb2.Request.SerializeToString,
        response_deserializer=message__pb2.Response.FromString,
        )
    self.wait = channel.unary_unary(
        '/DistributeSpider.SpiderServer/wait',
        request_serializer=message__pb2.Wait.SerializeToString,
        response_deserializer=message__pb2.Ack.FromString,
        )
    self.keepalive = channel.unary_unary(
        '/DistributeSpider.SpiderServer/keepalive',
        request_serializer=message__pb2.Register.SerializeToString,
        response_deserializer=message__pb2.UrlsAck.FromString,
        )
    self.save = channel.unary_unary(
        '/DistributeSpider.SpiderServer/save',
        request_serializer=message__pb2.House.SerializeToString,
        response_deserializer=message__pb2.Ack.FromString,
        )


class SpiderServerServicer(object):

  def req(self, request, context):
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def wait(self, request, context):
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def keepalive(self, request, context):
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def save(self, request, context):
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')


def add_SpiderServerServicer_to_server(servicer, server):
  rpc_method_handlers = {
      'req': grpc.unary_unary_rpc_method_handler(
          servicer.req,
          request_deserializer=message__pb2.Request.FromString,
          response_serializer=message__pb2.Response.SerializeToString,
      ),
      'wait': grpc.unary_unary_rpc_method_handler(
          servicer.wait,
          request_deserializer=message__pb2.Wait.FromString,
          response_serializer=message__pb2.Ack.SerializeToString,
      ),
      'keepalive': grpc.unary_unary_rpc_method_handler(
          servicer.keepalive,
          request_deserializer=message__pb2.Register.FromString,
          response_serializer=message__pb2.UrlsAck.SerializeToString,
      ),
      'save': grpc.unary_unary_rpc_method_handler(
          servicer.save,
          request_deserializer=message__pb2.House.FromString,
          response_serializer=message__pb2.Ack.SerializeToString,
      ),
  }
  generic_handler = grpc.method_handlers_generic_handler(
      'DistributeSpider.SpiderServer', rpc_method_handlers)
  server.add_generic_rpc_handlers((generic_handler,))
