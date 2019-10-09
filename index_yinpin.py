# -*- encoding: utf-8 -*-
'''
@File    :   index_duanluo.py
@Time    :   2019/10/08 16:01:23
@Author  :   Axi
@Version :   1.0
@Contact :   785934566@qq.com
@Desc    :   None
'''

# here put the import lib
import os
import json
import time
from whoosh.index import create_in
from whoosh.fields import *
from jieba.analyse import ChineseAnalyzer
from whoosh.qparser import QueryParser
from whoosh import qparser, scoring
from whoosh import index
import pymysql
from tqdm import tqdm

con = pymysql.connect('127.0.0.1',
                      'root',
                      '123456',
                      'fencitest',
                      charset='utf8')
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


def indexSql():
    writer = ix.writer(limitmb=1024, procs=4)
    # 获取数据表有多少行
    cur = con.cursor()
    cur.execute(' select count(1) from luke_yinpin  ')
    number = cur.fetchone()[0]
    with con:
        cur.execute('select id,duanluo from luke_yinpin')
        data = cur.fetchall()
        for line in tqdm(data):
            id = line[0]
            duanluo = line[1]
            writer.add_document(duanluo=duanluo, id=id)
    writer.commit()
    print('>>> found whoosh index done...')


def search_index(words):
    xg_duanluo = []
    with ix.searcher() as s:
        qp = QueryParser('duanluo', schema=ix.schema, group=qparser.OrGroup)
        qp.remove_plugin_class(qparser.WildcardPlugin)
        qp.add_plugin(qparser.PrefixPlugin())

        for word in words:
            q = qp.parse(u'{}'.format(word))
            results = s.search(q, limit=10)
            for i in results:
                xg_duanluo.append((i['id'], i['duanluo']))
    return xg_duanluo


def main(query):
    global ix
    indexdir = 'yinpin_whooshindex/'
    if not os.path.exists(indexdir):
        os.mkdir(indexdir)  # 创建indexdir对应的目录
    try:
        # 获取索引内容
        ix = index.open_dir(indexdir)
        # print ('>>>>>>>> 已创建索引 <<<<<<<<<<')
    except:
        print('>>>>>>>> 未创建索引 <<<<<<<<<<')
        ix = create_in(indexdir, schema)
        indexSql()

    words = list('{}'.format(query))
    xgwords = search_index(words)
    return xgwords
