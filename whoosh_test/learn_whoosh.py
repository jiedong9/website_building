'''
根据关键词表，匹配出相关段
Whoosh文档：https://www.osgeo.cn/whoosh/index.html
'''
# coding=utf-8
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
    cur.execute('select count(1) from baidu_xgss ')
    number = cur.fetchone()[0]
    with con:
        cur.execute("select distinct keyword from baidu_xgss  ")
        data = cur.fetchall()
        for line in data:
            keyword = line[0]
            # 将每行记录写入到索引中，将keyword复制给section索引变量
            writer.add_document(section=keyword)
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
    xg_words = []
    with ix.searcher() as s:

        # group=qparser.OrGroup 表示可匹配任意查询词，而不是所有查询词都匹配才能出结果
        qp = QueryParser('section', schema=ix.schema, group=qparser.OrGroup)

        # 下面两行表示可以使用通配符搜索，如"窗前*月光"
        qp.remove_plugin_class(qparser.WildcardPlugin)
        qp.add_plugin(qparser.PrefixPlugin())

        for word in words:
            q = qp.parse(u'%s' % word)
            # limit：代表返回多少条搜索结果
            results = s.search(q, limit=10)
            for i in results:
                xg_words.append(i['section'])
                # print (word,i['section'])
    return xg_words


# 使用结巴中文分词
analyzer = ChineseAnalyzer()

# 创建schema, stored为True表示能够被检索
'''
section：代表索引字段的名称，自定义
TEXT：代表索引字段的数据类型，常用的有TEXT、NUMBER
stored=True：代表改字段能够被搜索
analyzer：代表改字段需要使用结巴分词，都添加
'''
schema = Schema(section=TEXT(stored=True, analyzer=analyzer), )


def main(query):
    global ix
    # indexdir代表存储索引数据的目录，目录自定义
    indexdir = 'keyword_whooshindex/'
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


a = main('风控口子')
print(a)
