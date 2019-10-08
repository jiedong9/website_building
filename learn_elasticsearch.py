# -*- encoding: utf-8 -*-
'''
@File    :   learn_elasticsearch.py
@Time    :   2019/09/29 16:28:26
@Author  :   Axi
@Version :   1.0
@Contact :   785934566@qq.com
@Desc    :   None
'''

import os, json, time
from whoosh.index import create_in
from whoosh.fields import *
from jieba.analyse import ChineseAnalyzer
from whoosh.qparser import QueryParser
from whoosh import qparser, scoring
from whoosh import index
import pymysql

start = time.time()
con = pymysql.connect('127.0.0.1',
                      'root',
                      '123456',
                      'fencitest',
                      charset='utf8')


# 建立whoosh索引的函数
def new_index_sql():
    # 按照schema定义信息，增加需要建立索引的文档

    # 初始值，不用动
    n = 0
    # 建立索引，limitmb代表调用多大内存容量（以M为单位）来创建索引，procs可有理解为线程数
    writer = ix.writer(limitmb=256, procs=4)
    # 获取数据表有多少行
    cur = con.cursor()
    cur.execute(' select count(1) from luke_yinpin  ')
    number = cur.fetchone()[0]
    with con:
        cur.execute(" select id,duanluo from luke_yinpin  ")
        data = cur.fetchall()
        for line in data:
            id = line[0]
            duanluo = line[1]
            writer.add_document(duanluo=duanluo, id=id)
            # 输出百分比进度条
            n += 1
            percent = float(n) * 100 / float(number)
            sys.stdout.write("-----------> 完成百分比：%.2f" % percent)
            sys.stdout.write("%\r")
            sys.stdout.flush()

    writer.commit()
    sys.stdout.flush()
    print('>>> found whoosh index done...')


# 实现搜索功能的函数。ps：需要索引创建完成，才能实现搜索功能
def search_index(words):
    xg_duanluo = []
    with ix.searcher() as s:

        # group=qparser.OrGroup 表示可匹配任意查询词，而不是所有查询词都匹配才能出结果
        qp = QueryParser('duanluo', schema=ix.schema, group=qparser.OrGroup)

        # 下面两行表示可以使用通配符搜索，如"窗前*月光"
        qp.remove_plugin_class(qparser.WildcardPlugin)
        qp.add_plugin(qparser.PrefixPlugin())

        for word in words:
            q = qp.parse(u'%s' % word)
            # limit：代表返回多少条搜索结果
            results = s.search(q, limit=100)
            for i in results:
                xg_duanluo.append((i['id'], i['duanluo']))
                # print (word,i['section'])
    return xg_duanluo


# 使用结巴中文分词
analyzer = ChineseAnalyzer()

# 创建schema, stored为True表示能够被检索
'''
section：代表索引字段的名称，自定义
TEXT：代表索引字段的数据类型，常用的有TEXT、NUMERIC
stored=True：代表改字段能够被搜索
analyzer：代表改字段需要使用结巴分词，都添加
'''
schema = Schema(
    duanluo=TEXT(stored=True, analyzer=analyzer),
    id=NUMERIC(stored=True),
)


def main(query):
    global ix
    # indexdir代表存储索引数据的目录，目录自定义
    indexdir = 'yinpin_whooshindex/'
    # 检查当前脚本目录下，是否存在indexdir对应的目录
    if not os.path.exists(indexdir):
        os.mkdir(indexdir)  # 创建indexdir对应的目录
    try:
        # 获取索引内容
        ix = index.open_dir(indexdir)
        # print ('>>>>>>>> 已创建索引 <<<<<<<<<<')
    except:
        print('>>>>>>>> 未创建索引 <<<<<<<<<<')
        ix = create_in(indexdir, schema)
        new_index_sql()

    words = ["%s" % query]
    xgwords = search_index(words)
    return xgwords


if __name__ == "__main__":
    c = main('口子')
    print(c)