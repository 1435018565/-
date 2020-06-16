# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

from scrapy.exporters import JsonLinesItemExporter
from fang.items import NewHouseItem, ESFHouseItem

class FangePipeline(object):
    def __init__(self):
        self.newhouse_fp = open("newhouse.json", "wb")
        self.esfhouse_fp = open("esfhouse.json", "wb")
        self.newhouse_exporter = JsonLinesItemExporter(self.newhouse_fp, ensure_ascii = False)
        self.esfhouse_exporter = JsonLinesItemExporter(self.esfhouse_fp, ensure_ascii=False)


    def process_item(self, item, spider):
        # 判断返回的item和items.py文件中定义的item类型是否一致
        if isinstance(item, NewHouseItem):
            self.newhouse_exporter.export_item(item)
        else:
            self.esfhouse_exporter.export_item(item)
        return item

    def close_spider(self, spider):
        self.newhouse_fp.close()
        self.esfhouse_fp.close()



# 存入MySQL数据库(同步操作)
import pymysql
class MysqlSavePipline_1(object):
    def __init__(self):
        # 建立连接   后面参数分别为：主机， MySQL用户名， MySQL密码， 哪一个数据库
        self.conn = pymysql.connect("localhost", "root", "123456", "fang")
        # 创建游标
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):
        # 判断返回的item和items.py文件中定义的item类型是否一致
        if isinstance(item, NewHouseItem):
            # 新房sql语句
            insert_sql = """
                    insert into newhouse(province, city, name, price, rooms, area, address, district, sale, origin_url) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            # 执行插入数据库的操作
            self.cursor.execute(insert_sql, (item['province'], item['city'], item['name'], item['price'], item['rooms'], item['area'], item['address'], item['district'], item['sale'], item['origin_url']))

        if isinstance(item, ESFHouseItem):
            # 二手房sql语句
            insert_sql = """
                insert into esfhouse(province, city, name, address, price, unit, origin_url, infos) values (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            # 执行s插入数据库操作
            self.cursor.execute(insert_sql, (item['province'], item['city'], item['name'], item['address'], item['price'], item['unit'], item['origin_url'], item['infos']))

        # 提交，不进行提交保存不到数据库
        self.conn.commit()

    def close_spider(self, spider):
        # 关闭游标和连接
        self.cursor.close()
        self.conn.close()



# 存入MySQL数据库(异步操作)
import pymysql
from twisted.enterprise import adbapi
class MysqlSavePipline_2(object):
    def __init__(self, dbpool):
        self.dbpool = dbpool

    @classmethod
    # 函数名固定，会被scrapy调用，直接可用settings的值
    def from_settings(cls, settings):
        """
        数据库建立连接
        :param settings:配置参数
        :return:实例化参数
        """
        adbparams = dict(
            host = settings['MYSQL_HOST'],
            db = settings['MYSQL_DBNAME'],
            user = settings['MYSQL_USER'],
            password = settings['MYSQL_PASSWORD'],
            # 指定cursor类型
            cursorclass = pymysql.cursors.DictCursor
        )
        # 连接数据池ConnectionPool，使用pymysql连接
        dbpool = adbapi.ConnectionPool('pymysql', **adbparams)
        # 返回实例化参数
        return cls(dbpool)

    def process_item(self, item, spider):
        """
        使用twisted将MySQL插入变成异步执行。通过连接池执行具体的sql操作，返回一个对象
        :param item:
        :param spider:
        :return:
        """
        # 指定操作方法和操作数据
        query = self.dbpool.runInteraction(self.do_insert, item)
        # 添加异常处理
        query.addCallback(self.handle_error)

    def do_insert(self, cursor, item):
        # 对数据库执行插入操作，并不需要commit，twisted会自动commit
        # 首先判断应该插入哪一张表
        if isinstance(item, NewHouseItem):
            insert_sql = """
                insert into newhouse(province, city, name, price, rooms, area, address, district, sale, origin_url) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_sql, (item['province'], item['city'], item['name'], item['price'], item['rooms'], item['area'], item['address'], item['district'], item['sale'], item['origin_url']))
        if isinstance(item, ESFHouseItem):
            insert_sql = """
                            insert into esfhouse(province, city, name, address, price, unit, origin_url, infos) values (%s, %s, %s, %s, %s, %s, %s, %s)
                        """
            # 执行s插入数据库操作
            cursor.execute(insert_sql, (item['province'], item['city'], item['name'], item['address'], item['price'], item['unit'], item['origin_url'], item['infos']))

    def handle_error(self,failure):
        if failure:
            # 打印错误信息
            print(failure)



# 存入MongoDB数据库
import pymongo
class MongodbPipline(object):
    def __init__(self):
        # 建立数据库连接
        client = pymongo.MongoClient('127.0.0.1', 27017)
        # 连接所需数据库， fang为数据库名字
        db = client['fang']
        # 连接所用集合，也就是通常所说的表，newhouse为表名
        self.post_newhouse = db['newhouse']    # 新房
        self.post_esfhouse = db['esfhouse']    # 二手房

    def process_item(self, item, spider):
        if isinstance(item, NewHouseItem):
            # 把item转化为字典形式
            postItem = dict(item)
            # 向数据库插入一条记录
            self.post_newhouse.insert(postItem)
        if isinstance(item, ESFHouseItem):
            # 把item转化为字典形式
            postItem = dict(item)
            # 向数据库插入一条记录
            self.post_esfhouse.insert(postItem)