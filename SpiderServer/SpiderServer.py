import threading
import time
from concurrent import futures

import grpc

from Utils.RedisUtils import RedisUtils
from Utils.Utils import *
from proto import message_pb2

_ONE_DAY_IN_SECONDS = 24 * 60 * 60
_HOST = 'localhost'
_PORT = '16305'
urlsSpiderAddress = ""
timestamp = getTime()

redis = RedisUtils()

# 列表爬虫
urlsSpiders = dict()


# 从Redis数据库中获取到一个房源链接
# 输入爬虫名称，返回链接
def getUrlFromRedis(spider_name):
    return redis.get_url(spider_name)


def ack():
    return message_pb2.Ack(host=getIp(), timestamp=getTime())


spiders = ["Spider58", "SpiderAnjuke", "SpiderGanji", "SpiderFang"]


class SpiderServer(message_pb2.SpiderServerServicer):
    spider = ""
    work = False

    def req(self, request, context):
        print("%s>Rec req# %s:%s" % (getCurrentTime(), request.host, request.spider))
        isEmpty, url = getUrlFromRedis(request.spider)
        return message_pb2.Response(
            empty=isEmpty,
            url=url,
            timestamp=getTime(),
        )

    def urlsAck(self):
        crawls = list()
        for spider in spiders:
            if redis.isEmpty(spider):
                crawls.append(spider)
        return message_pb2.UrlsAck(work=len(crawls) > 0, spider=crawls, timestamp=getTime())

    def keepalive(self, request, context):
        host = request.host
        time = request.timestamp
        status = request.status
        urlsSpiders[host] = {'time': time, 'status': status}
        print("%s>Rec hello# %s" % (getCurrentTime(), host))
        '''
        for item in status:
            print(item.timestamp, item.spider, item.isalive)
        '''
        return self.urlsAck()

    def wait(self, request, context):
        pass

    def testSpider58(self):
        while True:
            print("[Work]", self.work, "[Spider]", self.spider if self != '' else None)
            time.sleep(10)
            self.spider = "SpiderAnjuke"
            self.work = True
            print("[Work]", self.work, "[Spider]", self.spider if self != '' else None)
            time.sleep(6)
            self.work = False

    def serve(self):
        # 定义一个服务器，同时定义允许的并发连接
        grpcServer = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
        # 添加服务
        message_pb2.add_SpiderServerServicer_to_server(self, grpcServer)
        # 定义端口号
        grpcServer.add_insecure_port('[::]:%s' % _PORT)
        # 启动服务
        grpcServer.start()

        count = 0
        try:
            while True:
                print("%s>Server is running..." % getCurrentTime())
                time.sleep(60)
        except KeyboardInterrupt:
            grpcServer.stop(0)

    # 检查当前的工作状态
    def checkUrlsSpider(self):
        if (len(urlsSpiders.keys()) == 0):
            print("当前没有列表爬虫运行中")
            return
        for key in urlsSpiders.keys():
            delta = getTime() - int(urlsSpiders[key])
            if delta < 6 * 1000000:
                print("%s正在等待状态" % key)


if __name__ == '__main__':
    server = SpiderServer()
    threading.Thread(target=server.serve).start()
