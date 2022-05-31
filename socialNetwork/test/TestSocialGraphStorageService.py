import sys
sys.path.append('/home/jrpear/repos/DeathStarBench/socialNetwork/gen-py')

import uuid
from social_network import SocialGraphStorageService
from social_network.ttypes import ServiceException

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

def add_rem_followers():
  socket = TSocket.TSocket("localhost", 10012)
  transport = TTransport.TFramedTransport(socket)
  protocol = TBinaryProtocol.TBinaryProtocol(transport)
  client = SocialGraphStorageService.Client(protocol)
  transport.open()
  client.AddFollower(1, 2)
  client.AddFollowee(2, 1)
  print(f"followers of 1 = {client.ReadFollowers(1)}; should be [2]")
  print(f"followers of 2 = {client.ReadFollowers(2)}; should be []")
  print(f"followees of 1 = {client.ReadFollowees(1)}; should be []")
  print(f"followees of 2 = {client.ReadFollowees(2)}; should be [1]")
  client.RemoveFollower(1, 2)
  client.RemoveFollowee(2, 1)
  print(f"followers of 1 = {client.ReadFollowers(1)}; should be []")
  print(f"followers of 2 = {client.ReadFollowers(2)}; should be []")
  print(f"followees of 1 = {client.ReadFollowees(1)}; should be []")
  print(f"followees of 2 = {client.ReadFollowees(2)}; should be []")
  transport.close()

if __name__ == '__main__':
  try:
    add_rem_followers()
  except ServiceException as se:
    print('%s' % se.message)
  except Thrift.TException as tx:
    print('%s' % tx.message)
