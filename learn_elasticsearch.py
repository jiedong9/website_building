# -*- encoding: utf-8 -*-
'''
@File    :   learn_elasticsearch.py
@Time    :   2019/09/29 16:28:26
@Author  :   Axi
@Version :   1.0
@Contact :   785934566@qq.com
@Desc    :   None
'''

# here put the import lib
from elasticsearch import Elasticsearch

es = Elasticsearch()
es.indices.create(index='news', ignore=400)
data = {
    'title': '美国留给伊拉克的是个烂摊子吗',
    'url': 'http://view.news.qq.com/zt2011/usa_iraq/index.htm'
}
result = es.create(index='news', doc_type='politics', id=1, body=data)
print(result)