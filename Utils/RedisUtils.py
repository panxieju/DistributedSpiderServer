import redis

HOST = '39.108.51.140'
PASSWORD = "790623"
PORT = 6379


class RedisUtils(object):
    def __init__(self, spider=None):
        if spider is not None:
            self.spider = spider
        self.conn = redis.Redis(host=HOST, port=PORT, password=PASSWORD)

    def get_url(self, spider_name=None):
        if spider_name is None:
            spider_name = self.__class__.__name__
        if self.isEmpty(spider_name):
            return True, ''
        try:
            return False, self.conn.rpop(self.get_redis_key_by_spider(spider_name)).decode('utf-8')
        except:
            raise AttributeError("错误！")

    def get_redis_key_by_spider(self, spider_name=None):
        if spider_name is not None:
            return 'HouseMasterSpider:%s_urls' % spider_name
        else:
            return 'HouseMasterSpider:%s_urls' % self.spider.__class__.__name__

    def sadd(self, name, url):
        key = 'HouseMasterSpider:%s_filter' % name
        from Utils.Utils import generateMD5
        value = generateMD5(url)
        return self.conn.sadd(key, value)

    def lpush(self, name, value):
        key = 'HouseMasterSpider:%s_urls' % name
        self.conn.lpush(key, value)

    def add_to_redis(self, key, url):
        result = self.sadd(key, url)
        if result == 1:
            self.lpush(key, url)
            return True
        else:
            return False

    def isEmpty(self, name):
        key = 'HouseMasterSpider:%s_urls' % name
        return False if key in self.getKeys() else True

    def getKeys(self):
        keys = self.conn.keys()
        return list(map(lambda x: x.decode('utf-8'), keys))


if __name__ == '__main__':
    redis = RedisUtils()
    print(redis.get_url("Spider58"))
