#!usr/bin/python
# -*- coding:utf-8 -*-

'''
Created on 2017年3月20日

@author: Administrator
'''
import pymysql

from Util.Utils import Utils


class CloudHouseDb(object):
    '''
    classdocs
    '''

    def __init__(self):
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
        self.expire_days = 7

    def set_expire_days(self, days):
        self.expire_days = days

    def get_conn_and_cursor(self):
        return self.conn, self.cursor

    def read_table_houses(self, table):
        sql = 'SELECT * from ' + table
        self.cursor.execute(sql)
        self.conn.commit()
        result = self.cursor.fetchall()
        return result

    def read_all_tables(self):
        city_list = ('guangzhou', 'shenzhen', 'shanghai', 'beijing')
        result = []
        for city in city_list:
            for house in self.read_table_houses(city):
                result.append(house)
        return result

    def is_exist(self, house):
        if house is None:
            return True
        url = house['url']
        city = house['city']
        table = self.get_city_pinyin_table(city)

        sql = 'SELECT * FROM ' + table + ' WHERE url = %s'
        self.cursor.execute(sql, (url))
        self.conn.commit()
        result = self.cursor.fetchall()
        if len(result) > 0:
            return True
        else:
            return False

    def write_house(self, house):
        title = house.title
        campus = house['campus']
        url = house['url']
        image_url = house['image_url']
        rental = house['rental']
        if type(rental) == type(0.02):
            rental = int(rental)
        area = house['area']
        house_type = house['house_type']
        source = house['source']
        date = house['date']
        rent_type = house['rent_type']
        floor = house['floor']
        address = house['address']
        district = house['district']
        district = self.check_guangzhou_district(district)
        city = house['city']
        lat = house['lat']
        lon = house['lon']
        md5 = Utils.generateMD5(url)
        try:
            rooms = house['rooms']
        except:
            rooms = Utils.get_rooms(house_type)
        time = Utils.getCurrentTime()
        import datetime
        timedelta = datetime.datetime.now() - datetime.datetime.strptime(date, '%Y-%m-%d')
        days = timedelta.days
        if days > self.expire_days:
            return
        table = self.get_city_pinyin_table(city)
        sql = "insert into " + table + "(rental, title, campus, house_type, date, rent_type, area, floor, district, address, url,image_url, source, city, lat, lon, time, rooms, md5) values( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) on duplicate key update time = %s, date = %s, rental = %s"
        self.cursor.execute(sql, (
            rental, title, campus, house_type, date, rent_type, area, floor, district, address, url, image_url, source, city, lat, lon, time, rooms, md5, time, date, rental))
        self.conn.commit()
        print(Utils.getCurrentTime(), '在云端数据库中写入新房源：' + city, campus, url)

    def update_house(self, house):
        title = house['title']
        url = house['url']
        rental = house['rental']
        date = house['date']
        city = house['city']
        time = Utils.getCurrentTime()
        table = self.get_city_pinyin_table(city)
        sql = "update " + table + " set date= %s， rental = %s, time = %s where url = %s"
        self.cursor.execute(sql, (date, rental, time, url))
        self.conn.commit()
        print(Utils.getCurrentTime(), '在云端数据库中更新房源：' + city, url)

    def write_houses(self, list):
        if list is not None:
            for house in list:
                self.write_house(house)

    def update_houses(self, list):
        if list is not None:
            for house in list:
                self.update_house(house)

    def check_guangzhou_district(self, district):
        if district == '萝岗':
            return '黄埔'
        else:
            return district

    def delete_unformatedRentalHouse(self):
        sql = 'delete from houses where rental = %s'
        self.cursor.execute(sql, ('面议'))
        self.conn.commit()

    def get_city_pinyin_table(self, city):
        if re.search(r'广州', city):
            table = 'guangzhou'
        if re.search(r'深圳', city):
            table = 'shenzhen'
        if re.search(r'北京', city):
            table = 'beijing'
        if re.search(r'上海', city):
            table = 'shanghai'
        return table

    def delete_incorrect_houses(self, city):
        if re.search(r'广州', city):
            table = 'guangzhou'
        if re.search(r'深圳', city):
            table = 'shenzhen'
        if re.search(r'北京', city):
            table = 'beijing'
        if re.search(r'上海', city):
            table = 'shanghai'

        sql = 'DELETE FROM ' + table + ' WHERE city <> %s'
        print(sql)
        self.cursor.execute(sql, city)
        self.conn.commit()

    def close(self):
        if self.cursor is not None:
            self.cursor.close()
        if self.conn is not None:
            self.conn.close()
        pass

    def write_history_house(self, house):
        title = house['title']
        campus = house['campus']
        url = house['url']
        image_url = house['image_url']
        rental = house['rental']
        if type(rental) == type(0.02):
            rental = int(rental)
        rooms = house['rooms']
        area = house['area']
        house_type = house['house_type']
        source = house['source']
        date = house['date']
        rent_type = house['rent_type']
        floor = house['floor']
        address = house['address']
        district = house['district']
        district = self.check_guangzhou_district(district)
        city = house['city']
        lat = house['lat']
        lon = house['lon']
        time = house['time']
        contact = house['contact']
        phone = house['phone']

        import datetime
        timedelta = datetime.datetime.now() - datetime.datetime.strptime(date)
        days = timedelta.days
        print('days', days)
        if days >7:
            return

        sql = "insert into history(rental, title, campus, house_type, date, rent_type, area, floor, district, address, url,image_url, source, city, lat, lon, time,rooms, contact, phone) values( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s)"
        self.cursor.execute(sql, (
            rental, title, campus, house_type, date, rent_type, area, floor, district, address, url, image_url, source, city, lat, lon, time, rooms, contact, phone))
        self.conn.commit()

    def write_history_houses(self, houses):
        for house in houses:
            self.write_history_house(house)

    def backup_history_record(self):
        date = Utils.getDeltaDate(days=7)
        for city in self.city_list:
            sql = 'SELECT * FROM '+city + ' WHERE date < "%s"'%(date)
            self.cursor.execute(sql)
            self.conn.commit()
            result = self.cursor.fetchall()
            self.write_history_houses(result)
            sql = 'DELETE FROM '+city+' WHERE date< "%s"'%(date)
            self.cursor.execute(sql)
            self.conn.commit()

    def get_expired_house_count(self):
        sum = 0
        date = Utils.getDeltaDate(days=6)
        for city in self.city_list:
            sql = 'SELECT count(*) FROM %s WHERE date < "%s"'%(city, date)
            self.cursor.execute(sql)
            self.conn.commit()
            result = self.cursor.fetchone()['count(*)']
            sum += result
        return sum

    def get_house_count(self):
        result = list()
        for city in self.city_list:
            sql = 'SELECT COUNT(*) FROM %s'%city
            self.cursor.execute(sql)
            self.conn.commit()
            query_result = self.cursor.fetchone()
            print(query_result)
            info = dict()
            info['city'] = self.get_city_name(city)
            info['count'] = query_result['COUNT(*)']
            result.append(info)
        return result

    def get_city_name(self, name):
        if name=='guangzhou':
            return '广州'
        if name=='shenzhen':
            return '深圳'
        if name=='beijing':
            return '北京'
        if name=='shanghai':
            return '上海'

    def get_table_name_by_city(self, city = None):
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
        return table

import re

if __name__ == "__main__":
    house_db = CloudHouseDb()
    item = {'campus': '协和新世界', 'rooms': '2', 'city': '广州', 'floor': '高楼层 (共33层)', 'title': '天河北 协和新世界 温馨两房 户型方正修', 'house_type': '2室2厅1卫', 'area': '92平米', 'lon': '113.337785', 'date': '2017-10-26', 'district': '天河', 'rental': '6300', 'url': 'https://m.lianjia.com/gz/zufang/GZ0003183181.html', 'image_url': 'https://image1.ljcdn.com/mytophomeimg/15090835284377_2265253062_2.jpg.600x450.jpg', 'address': '广东省广州市天河区石牌街道聚点桌球会所芳草园', 'rent_type': '整租', 'lat': '23.141829', 'source': '链家'}

    house_db.write_house(item)
