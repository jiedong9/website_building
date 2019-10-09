# -*- encoding: utf-8 -*-
'''
@File    :   learn_elasticsearch.py
@Time    :   2019/09/29 16:28:26
@Author  :   Axi
@Version :   1.0
@Contact :   785934566@qq.com
@Desc    :   None
'''
'''
1）从baidu_xgss表中keywords字段中，随机抽取一个词，最为标题关键词1
2）从关键词1的10个相关词中，随机抽取一个作为关键词2
3）文章title：{关键词1},{关键词2}
4）文章正文：
    1-5个段落：关键词1的前100个相关音频段落中，随机抽取5个
    5-15个段落：关键词1的前100个采集相关段落中，随机抽取10个
    16段落：关键词1的5个相关词，和关键词2的5个相关词
'''
import whoosh_keyword, whoosh_caiji, whoosh_yinpin
import pymysql, random, time, re
import os


def get_include_file(filename):
    a = os.path.exists(filename)
    return a


'''七牛图片文件枚举'''
if get_include_file('tupian'):
    pass
else:
    from qiniu import Auth
    from qiniu import BucketManager
    access_key = '{换成你的}'
    secret_key = '{换成你的}'
    q = Auth(access_key, secret_key)
    bucket = BucketManager(q)
    tupian_file = open('tupian', 'w')

    # 存储空间名称
    bucket_name = '{换成你的}'
    # 前缀
    prefix = None
    # 列举数量
    limit = 1000
    # 列举出除'/'的所有文件以及以'/'为分隔的所有前缀
    delimiter = None
    # 起始编号
    marker = None
    while True:
        ret, eof, info = bucket.list(bucket_name, prefix, marker, limit,
                                     delimiter)
        if ret.__contains__('marker'):
            marker = ret['marker']
            for line in ret['items']:
                imgurl = "http://pv1oipssv.bkt.clouddn.com/%s" % line['key']
                tupian_file.write('%s\n' % imgurl)
        else:
            for line in ret['items']:
                imgurl = "http://pv1oipssv.bkt.clouddn.com/%s" % line['key']
                tupian_file.write('%s\n' % imgurl)
            break
    tupian_file.close()

imgs_list = open('tupian').readlines()

con = pymysql.connect('127.0.0.1',
                      'root',
                      'r1#-fj.qbDlo',
                      'fintech',
                      charset='utf8')
cursor = con.cursor()
cursor.execute(" select distinct keyword from baidu_xgss")
results = cursor.fetchall()

# 远程连接zblog服务器的MySQL
con_zblog = pymysql.connect('47.244.252.39',
                            'gogo',
                            '123456',
                            'www_indoiphone_',
                            charset='utf8')
cursor_zblog = con_zblog.cursor()

import os


def get_include_file(filename):
    a = os.path.exists(filename)
    return a


# 将所有关键词放入bd_xgss这个文件中，如果当前目录包含bd_xgss这个文件，则忽略
if get_include_file('bd_xgss'):
    pass
else:
    xgss_file = open('bd_xgss', 'w')
    for row in results:
        try:
            xgss_file.write('%s\n' % row[0])
        except:
            print(row[0])
    xgss_file.close()

# 提取关键词1
bd_xgss = open('bd_xgss').readlines()
n = 1
while n <= 10:
    keyword_1 = random.sample(bd_xgss, 1)[0].strip()

    # 提取关键词2
    keyword2_list = whoosh_keyword.main(keyword_1)
    if len(keyword2_list) == 1:  # 如果关键词1没有相关搜索词，则title只有关键词1
        title = keyword_1
        keyword_2 = ""
    else:
        keyword_2 = random.sample(whoosh_keyword.main(keyword_1)[1:],
                                  1)[0].strip()
        title = "%s,%s" % (keyword_1, keyword_2)
    print("%s, %s" % (n, title))

    # 提取关键词1的5个相关音频段落
    content = ""  # 文章正文
    yinpin_lists = whoosh_yinpin.main(keyword_1)
    if len(yinpin_lists) <= 5:  # 若音频段落搜索结果少于5个，则所有搜索结果拼成正文
        for line in yinpin_lists:
            duanluo = "<p>{duanluo}</p>".format(duanluo=line[1])
            content = content + duanluo
    else:
        for line in random.sample(yinpin_lists, 5):
            duanluo = "<p>{duanluo}</p>".format(duanluo=line[1])
            content = content + duanluo
    content = content + '<img src="{src}" alt="{keyword}">'.format(
        src=random.sample(imgs_list, 1)[0].strip(), keyword=keyword_1)

    # 提取关键词1的10个相关采集段落
    caiji_lists = whoosh_caiji.main(keyword_1)
    if len(caiji_lists) <= 5:  # 若音频段落搜索结果少于5个，则所有搜索结果拼成正文
        for line in caiji_lists:
            duanluo = "<p>{duanluo}</p>".format(duanluo=line[1])
            content = content + duanluo
    else:
        for line in random.sample(caiji_lists, 5):
            duanluo = "<p>{duanluo}</p>".format(duanluo=line[1])
            content = content + duanluo
    content = content + '<img src="{src}" alt="{keyword}">'.format(
        src=random.sample(imgs_list, 1)[0].strip(), keyword=keyword_1)

    # 16个段落
    content = content + "<p>相关标题：%s</p>" % (','.join(
        random.sample(keyword2_list, 5)))

    # 发布zblog
    sql = '''INSERT INTO zbp_post (log_CateID, log_AuthorID, log_Tag, log_Status, log_Type, log_Alias, log_IsTop, log_IsLock, log_Title, log_Intro, log_Content, log_PostTime, log_CommNums, log_ViewNums, log_Template, log_Meta) VALUES ( {log_CateID}, {log_AuthorID}, '{log_Tag}', {log_Status}, {log_Type}, '{log_Alias}', {log_IsTop}, {log_IsLock}, '{log_Title}', '{log_Intro}', '{log_Content}', {log_PostTime}, {log_CommNums}, {log_ViewNums}, '{log_Template}', '{log_Meta}' )'''.format(
        log_CateID=random.randint(1, 5),
        log_AuthorID=1,
        log_Tag=random.randint(1, 9),
        log_Status=0,
        log_Type=0,
        log_Alias="",
        log_IsTop=0,
        log_IsLock=0,
        log_Title=title,
        log_Intro=re.sub('<[^>]*?>', '', content[:200]),
        log_Content=content,
        log_PostTime=time.time(),
        log_CommNums=0,
        log_ViewNums=random.randint(100, 500),
        log_Template="",
        log_Meta="")

    try:
        cursor_zblog.execute(sql)
        con_zblog.commit()
        n += 1
    except:
        con_zblog.rollback()
        print("Error")
