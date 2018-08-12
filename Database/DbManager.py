import re
import time

import pymysql

from Bean.House import House
from Utils.Utils import getCurrentTime

class DB_Manager(object):
    def __init__(self, house):
        '''
        Constructor
        '''
        host = '39.108.51.140'
        port = 3306
        user = 'spider'
        password = 'spider'
        charset = 'utf8mb4'
        db = 'house'
        self.city_list = ('guangzhou', 'shenzhen', 'shanghai', 'beijing')
        cursor_class = pymysql.cursors.DictCursor
        self.conn = pymysql.connect(host=host,
                                    user=user,
                                    password=password,
                                    port=port,
                                    db=db,
                                    charset=charset,
                                    cursorclass=cursor_class)
        self.cursor = self.conn.cursor()
        try:
            self.writeHouse(house)
        except:
            try:
                time.sleep(1)
                self.writeHouse()
            except:
                pass

        self.cursor.close()
        self.conn.close()

    def writeHouse(self, house):
        if house.lat == 0:
            print('%s 写入失败，没有定位信息 %s' % (getCurrentTime(), house.title))
            return
        table = self.getTableByCity(house.city)
        sql = "INSERT IGNORE INTO " + table + "(rental, title, campus, house_type, date, rent_type, area, floor, district, address, url,image_url, source, city, lat, lon, time, rooms, md5, contact, phone) VALUES( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        self.cursor.execute(sql, (
            house.rental, house.title, house.campus, house.house_type, house.date, house.rent_type, house.area,
            house.floor,
            house.district, house.address, house.url, house.image_url, house.source, house.city, house.lat, house.lon,
            house.time,
            house.rooms, house.md5, house.contact, house.phone))
        self.conn.commit()
        print("%s 写入%s房源: %s, %s" % (getCurrentTime(), house.source, house.title, house.url))

    def getTableByCity(self, city):
        if city is None:
            return 'history'
        if re.search(r'广州', city):
            table = 'guangzhou'
        if re.search(r'深圳', city):
            table = 'shenzhen'
        if re.search(r'北京', city):
            table = 'beijing'
        if re.search(r'上海', city):
            table = 'shanghai'
        if re.search(r'TEST', city):
            table = 'test'
        return table


if __name__ == '__main__':
    house = House()
    house.rental = 100.0
    house.lat = 23.111
    house.lon = 133.4313
    house.city = 'TEST'
    db = DB_Manager()
    sql = "INSERT INTO test (rental, lat) VALUE (%s, %s)"
    db.cursor.execute(sql, (house.rental, house.lat))
    db.conn.commit()
