import hashlib
import socket
import uuid

import datetime


def getTime():
    import datetime
    return int(datetime.datetime.now().timestamp() * 1000000)


def getMacAddress():
    mac = uuid.UUID(int=uuid.getnode()).hex[-12:]
    return ":".join([mac[e:e + 2] for e in range(0, 11, 2)])


def getHostName():
    return socket.getfqdn(socket.gethostname())


def getIp():
    return socket.gethostbyname(getHostName())

#产生MD5序列
def generateMD5(str):
    generator = hashlib.md5()
    generator.update(str.encode('utf-8'))
    return generator.hexdigest()


def getCurrentTime():
    now = datetime.datetime.now()
    return datetime.datetime.strftime(now, '%Y-%m-%d %H:%M:%S')


if __name__ == '__main__':
    print(getTime())
    print(getMacAddress())
    print(getHostName())
    print(getIp())
