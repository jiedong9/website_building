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
import whoosh_keyword, whoosh_caiji, whoosh_yinpin
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


if get_include_file('bd_xgss'):
    pass
else:
    xgss_file = open('bd_xgss', 'w')
    for row in results:
        try:
            xgss_file.write('{}\n'.format(row[0]))
        except:
            print(row[0])
    xgss_file.close()
