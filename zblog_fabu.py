# -*- encoding: utf-8 -*-
'''
@File    :   zblog_fabu.py
@Time    :   2019/10/09 09:08:31
@Author  :   Axi
@Version :   1.0
@Contact :   785934566@qq.com
@Desc    :   None
'''

# here put the import lib
import whoosh_keyword, index_duanluo, index_yinpin
import pymysql
import random
import time
import re
import os

# 本地数据库
con = pymysql.connect('127.0.0.1',
                      'root',
                      '123456',
                      'fencitest',
                      charset='utf8')
cursor = con.cursor()
cursor.execute(" select distinct keyword from baidu_xgss")
results = cursor.fetchall()


def get_include_file(filename):
    a = os.path.exists(filename)
    return a


# 提取两个关键词组成标牌提，第一个关键词为为根词，第二个词为索引出的相关词
if get_include_file('bd_xgss'):
    pass
else:
    xgss_file = open('bd_xgss', 'w', encoding='gbk')
    for row in results:
        try:
            xgss_file.write('{}\n'.format(row[0]))
        except:
            print(row[0])
    xgss_file.close()

bd_xgss = open('bd_xgss').readlines()
n = 1
while n <= 10:
    keyword_1 = random.sample(bd_xgss, 1)[0].strip()
    keyword2_list = whoosh_keyword.main(keyword_1)
    if len(keyword2_list) == 1:
        title = keyword_1
        keyword_2 = ''  # 为空时
    else:
        keyword_2 = random.sample(whoosh_keyword.main(keyword_1)[1:],
                                  1)[0].strip()
        title = '{},{}'.format(keyword_1, keyword_2)
    print('{},{}'.format(n, title))

    # 提取关键词1的5个相关音频段落
    content = ''
    yinpin_list = index_yinpin.main(keyword_1)
    if len(yinpin_list) <= 5:
        for line in yinpin_list:
            duanluo = '<p>{}</p>'.format(line[1])
            content = content + duanluo
    else:
        for line in random.sample(yinpin_list, 5):
            duanluo = duanluo = '<p>{}</p>'.format(line[1])
            content = content + duanluo
            
    content = content + '--------------'
    print(content)
    # 提取关键词1的相关采集段落
    caiji_list = index_duanluo.main(keyword_1)
    if len(caiji_list) <= 5:
        for line in caiji_list:
            duanluo = '<p>{}</p>'.format(line[1])
            content = content + duanluo
    else:
        for line in random.sample(caiji_list, 5):
            duanluo = duanluo = '<p>{}</p>'.format(line[1])
            content = content + duanluo
    content = content + '==========='

    content = content + "<p>相关标题：%s</p>" % (','.join(
        random.sample(keyword2_list, 5)))

    print(content)
    n += 1