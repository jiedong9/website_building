# -*- encoding: utf-8 -*-
'''
@File    :   whoosh_yinpin.py
@Time    :   2019/10/08 15:29:16
@Author  :   Axi
@Version :   1.0
@Contact :   785934566@qq.com
@Desc    :   None
'''

import hashlib
# here put the import lib
import os
import re
import time

import pymysql

con = pymysql.connect('127.0.0.1',
                      'root',
                      '123456',
                      'fencitest',
                      charset='utf8')
cursor = con.cursor()


def get_content_wordnumber(html):
    text = re.sub("[+/_,$%^*(+\"]+|[+——！，:：。？、~@#￥%……&*（）“”《》]+", "", html)
    text2 = re.sub('<[^>]*?>', '', text)
    words_number = len(text2)
    return words_number


def get_md5(str):
    m2 = hashlib.md5()
    m2.update(str.encode('utf8'))
    return m2.hexdigest()


def get_now_time():
    a = time.strftime('%Y-%m-%d %H:%M:%S ', time.localtime(time.time()))
    return a


def eachFile(filepath):
    file_pash_list = []
    pathDir = os.listdir(filepath)
    for allDir in pathDir:
        child = os.path.join('{}\{}'.format(filepath, allDir))
        file_pash_list.append(child)
    return file_pash_list


if __name__ == "__main__":
    n = 1
    for file in eachFile(r'E:\testdata\录音转文字'):
        print('>>>>>> 正在处理第{}篇'.format(n))
        content = open(file).readlines()

        for line in content:
            line = line.strip()
            number = get_content_wordnumber(line)
            if number >= 130:
                sql = "INSERT INTO luke_yinpin (duanluo, md5, input_date) VALUES ('{duanluo}', '{md5}', '{input_date}')".format(
                    duanluo=line, md5=get_md5(line), input_date=get_now_time())
            try:
                cursor.execute(sql)
                con.commit()
            except:
                con.rollback()
        n += 1
