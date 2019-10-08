import pymysql
import os, json, time
from whoosh.index import create_in
from whoosh.fields import *
from jieba.analyse import ChineseAnalyzer
from whoosh.qparser import QueryParser
from whoosh import qparser, scoring
from whoosh import index
from tqdm import tqdm

con = pymysql.connect('127.0.0.1',
                      'root',
                      '123456',
                      'fencitest',
                      charset='utf8')

analyzer = ChineseAnalyzer()
'''
section：代表索引字段的名称，自定义
TEXT：代表索引字段的数据类型，常用的有TEXT、NUMBER
stored=True：代表改字段能够被搜索
analyzer：代表改字段需要使用结巴分词，都添加
'''
schema = Schema(section=TEXT(stored=True, analyzer=analyzer))


# 创建索引
def indexSql():
    writer = ix.writer(limitmb=256, procs=4)
    cur = con.cursor()
    cur.execute('select count(1) from baidu_xgss')
    number = cur.fetchone()[0]

    with con:
        cur.execute('select distinct keyword from baidu_xgss')
        data = cur.fetchall()
        for line in tqdm(data):
            keyword = line[0]
            writer.add_document(section=keyword)
    writer.commit()
    print('>>> found whoosh index done...')


# 实现搜索功能
def search_index(words):
    xg_words = []
    with ix.searcher() as s:
        qp = QueryParser('section', schema=ix.schema, group=qparser.OrGroup)

        # 可以使用通配符搜索
        qp.remove_plugin_class(qparser.WildcardPlugin)
        qp.add_plugin(qparser.PrefixPlugin())

        for word in words:
            q = qp.parse(u'{}'.format(word))
            results = s.search(q, limit=10)
            for i in results:
                xg_words.append(i['section'])
    return xg_words


def mian(query):
    global ix
    indexdir = 'keyword_whooshindex/'
    if not os.path.exists(indexdir):
        os.mkdir(indexdir)
    try:
        ix = index.open.exists(indexdir)
    except:
        print('>>>>>>>> 未创建索引 <<<<<<<<<<')
        ix = create_in(indexdir, schema)
        indexSql()

    words = list('{}'.format(query))
    xgwords = search_index(words)
    return xgwords


if __name__ == "__main__":
    a = mian('风控口子')
    print(a)