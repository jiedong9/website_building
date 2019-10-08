# -*- encoding: utf-8 -*-
'''
@File    :   whoosh_caiji.py
@Time    :   2019/10/08 13:43:51
@Author  :   Axi
@Version :   1.0
@Contact :   785934566@qq.com
@Desc    :   None
'''

# here put the import lib
import pymysql
import re
import time
import hashlib

con = pymysql.connect('127.0.0.1',
                      'root',
                      '123456',
                      'fencitest',
                      charset='utf8')


def get_now_time():
    time_now = time.strftime('%Y-%m-%d %H:%M:%S ', time.localtime(time.time()))
    return time_now


def get_md5(str):
    m2 = hashlib.md5()
    m2.update(str.encode('utf-8'))
    return m2.hexdigest()


def get_content_wordnumber(html):
    text = re.sub('[+/_,$%^*(+\"]+|[+——！，:：。？、~@#￥%……&*（）“”《》]+', '', html)
    text2 = re.sub('<[^>]*?>', '', text)
    words_number = len(text2)
    return words_number


if __name__ == "__main__":
    cursor = con.cursor()
    cursor.execute(" select content from news")
    results = cursor.fetchall()
    n = 1
    for row in results:
        print('>>>>>>>>正在处理第{}篇'.format(n))
        content = row[0]
        for line in re.findall(r'<[p|P][^>]*?>(.*?)</[p|P]>', content):
            line = re.sub(r'<[^>]*?>|&[^;]*?;', '', line).strip()
            number = get_content_wordnumber(line)
            if number >= 130:
                sql = "INSERT INTO luke_caiji (duanluo, md5, input_date) VALUES ('{duanluo}', '{md5}', '{input_date}')".format(
                    duanluo=line, md5=get_md5(line), input_date=get_now_time())
                try:
                    cursor.execute(sql)
                    con.commit()
                except Exception as e:
                    con.rollback()
                    print(e)
        n += 1