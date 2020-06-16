# Scrapy爬虫项目，可改造成Scrapy-Redis分布式爬虫
## 数据存储方式有以下几种：
##### 1、存入Json文件
##### 2、存入MySQL数据库
##### 3、存入MongoDB数据库

##### 1、所有城市的url链接：
#####    https://www.fang.com/SoufunFamily.htm
##### 2、每一个城市的主页面，比如说南阳的：
#####    https://nanyang.fang.com/
##### 3、每一个城市的新房链接：
#####    https://nanyang.newhouse.fang.com/house/s/
##### 4、每一个城市的二手房链接：
#####    https://nanyang.esf.fang.com/

##### 注意：
##### 北京是个例外：
##### 主页：https://bj.fang.com/
##### 新房：https://newhouse.fang.com/house/s/
##### 二手房：https://esf.fang.com/



### 3.1.1	`Json`支持的数据格式

1. 对象（字典）。使用花括号
2. 数组（列表）。使用方括号
3. 整型，浮点型，布尔类型，null类型。
4. 字符串类型（字符串必须使用双引号，不能使用单引号）。



​     多个数据之间使用逗号分开。

​     **注意：`Json`本质上就是一个字符串**

### 3.1.2	字典和列表转化为`Json`（`dumps`和`dump`使用）

`dumps`：将列表或者字典转化为`Json`字符串。

`dump`：将列表或者字典转化为`Json`文件。

**dumps**

```python
import json

books = [
    {
        'title': "钢铁是怎样炼成的",
        'peice': "9.8"
    },
    {
        'title': "红楼梦",
        'price': 9.9
    }
]

json_str = json.dumps(books)
print(type(json_str))
print(json_str)
```

输出结果为：

```python
<class 'str'>
[{"title": "\u94a2\u94c1\u662f\u600e\u6837\u70bc\u6210\u7684", "peice": "9.8"}, {"title": "\u7ea2\u697c\u68a6", "price": 9.9}]
```

可以看出来，`Json`实际上是一个字符串，然后`dumps`将列表或者字典转化为了字符串。
