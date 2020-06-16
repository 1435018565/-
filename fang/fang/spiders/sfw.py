# -*- coding: utf-8 -*-
import scrapy
import re
from fang.items import NewHouseItem, ESFHouseItem


class SfwSpider(scrapy.Spider):
    name = 'sfw'
    allowed_domains = ['fang.com']
    start_urls = ['https://www.fang.com/SoufunFamily.htm']

    def parse(self, response):
        # 所有城市标签
        trs = response.xpath("//div[@class = 'outCont']//tr")
        province = None
        # 遍历得到每一行的数据
        for tr in trs:
            # 获取省份和对应城市的两个td标签
            tds = tr.xpath(".//td[not(@class)]")
            # 省份名称
            province_text = tds[0]
            # 省份对应的城市名称及链接
            city_info = tds[1]
            # 提取省份名称
            province_text = province_text.xpath(".//text()").get()
            province_text = re.sub(r"\s", "", province_text)
            if province_text:
                province = province_text
            # 不爬取海外房产
            if province == "其它":
                continue
            # 提取城市名称及链接
            city_links = city_info.xpath(".//a")
            for city_link in city_links:
                # 获取城市
                city = city_link.xpath(".//text()").get()
                # 获取城市链接
                city_url = city_link.xpath(".//@href").get()

                # 构建新房链接
                url_split = city_url.split("fang")
                url_former = url_split[0]
                url_backer = url_split[1]
                newhouse_url = url_former + "newhouse.fang.com/house/s/"
                # 构建二手房链接
                esf_url = url_former + "esf.fang.com/"

                # print("++" * 20)
                # print("省份：", province)
                # print("城市：", city)
                # print("新房链接：", newhouse_url)
                # print("二手房链接:", esf_url)
                # print("++" * 20)

                # 返回新房信息再解析
                yield scrapy.Request(url=newhouse_url, callback=self.parse_newhouse, meta={"info": (province, city)})

                # 返回二手房信息再解析
                yield scrapy.Request(url=esf_url, callback=self.parse_esf, meta = {"info": (province, city)})



    # 新房页面解析
    def parse_newhouse(self, response):
        province, city = response.meta.get("info")
        lis = response.xpath("//div[contains(@class, 'nl_con')]/ul/li[not(@style)]")
        for li in lis:
            # 获取房产名字
            name = li.xpath(".//div[@class='nlcd_name']/a/text()").get().strip()
            # 获取几居室
            rooms = li.xpath(".//div[contains(@class, 'house_type')]/a//text()").getall()
            # 获取面积
            area = li.xpath(".//div[contains(@class, 'house_type')]/text()").getall()
            area = "".join(area).strip()
            area = re.sub(r"/|－|/s|	|\n", "", area)
            # 获取地址
            address = li.xpath(".//div[@class = 'address']/a/@title").get()
            # 获取是哪个区的房子
            district = li.xpath(".//div[@class = 'address']/a//text()").getall()
            district = "".join(district)
            district = re.search(r".*\[(.+)\].*", district).group(1)
            # 获取是否在售
            sale = li.xpath(".//div[contains(@class, 'fangyuan')]/span/text()").get()
            # 获取价格
            price = li.xpath(".//div[@class = 'nhouse_price']//text()").getall()
            price = "".join(price).strip()
            # 获取详情页url
            origin_url = li.xpath(".//div[@class = 'nlcd_name']/a/@href").get()

            # 构建item返回
            item = NewHouseItem(province = province, city = city, name = name, rooms = rooms, area = area, address = address, district = district, sale = sale, price = price, origin_url = origin_url)
            yield item

        # 爬取下一页数据
        next_url = response.xpath("//div[@class = 'page']//a[@class = 'next']/@href").get()
        if next_url:
            yield scrapy.Request(url=response.urljoin(next_url), callback=self.parse_newhouse, meta={"info": (province, city)})




    # 二手房页面解析
    def parse_esf(self, response):
        province, city = response.meta.get("info")
        dls = response.xpath("//div[contains(@class, 'shop_list')]/dl[@dataflag = 'bg']")
        for dl in dls:
            item = ESFHouseItem(province = province, city = city)
            # 房子名字
            name = dl.xpath(".//p[@class = 'add_shop']/a/@title").get()
            item["name"] = name
            # 信息(几室几厅（rooms），面积（area）， 层（floor）， 朝向（toward）， 年代（year）)
            infos = dl.xpath(".//p[@class = 'tel_shop']/text()").getall()
            infos = "".join(infos).strip()
            infos = re.sub(r"'|\|\r|\n|/s| ", "", infos)
            item['infos'] = infos
            # 地址
            address = dl.xpath(".//p[@class = 'add_shop']/span/text()").get()
            item['address'] = address
            # 价格
            price = dl.xpath(".//dd[@class = 'price_right']/span[1]//text()").getall()
            price = "".join(price)
            item['price'] = price
            # 均价
            unit = dl.xpath(".//dd[@class = 'price_right']/span[2]/text()").get()
            item['unit'] = unit
            # 原始url
            origin_url = dl.xpath(".//h4[@class = 'clearfix']/a/@href").getall()
            origin_url = "".join(origin_url)
            origin_url = response.urljoin(origin_url)
            item['origin_url'] = origin_url
            yield item

        # 下一页url
        next_url = response.xpath("//div[@class = 'page_al']/p[last()-1]/a/@href").get()
        if next_url:
            yield scrapy.Request(url=response.urljoin(next_url), callback=self.parse_esf, meta={"info": (province, city)})



if __name__ == '__main__':
    from scrapy import cmdline
    args = "scrapy crawl sfw".split()
    cmdline.execute(args)
