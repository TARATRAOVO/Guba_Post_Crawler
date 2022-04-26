import requests
import sqlite3
from bs4 import BeautifulSoup
import unicodedata
import time
import re

import threading
import time
import os
import pandas as pd

import random

import os
import shutil

def all_path(dirname):
    result = []
    for maindir, subdir, file_name_list in os.walk(dirname):
        for filename in file_name_list:
            apath = os.path.join(maindir, filename)
            result.append(apath)
    return result

def extract_uid(relative_url):
    relative_url = re.search(',(.*)html', relative_url).group(1)
    relative_url = re.search(',(.*).', relative_url).group(1)
    return relative_url

#将一个地址改为处理中
def processing(infodb_location, db_location):
    conn_database_information = sqlite3.connect(infodb_location)
    c_database_infromation = conn_database_information.cursor()
    command = '''UPDATE DB_INFO SET ISPROCESSING = 1 WHERE DB_LOCATION = ?'''
    para = (db_location,)
    c_database_infromation.execute(command, para)
    conn_database_information.commit()
    conn_database_information.close()

#将一个地址改为处理完成
def done(infodb_location, db_location):
    conn = sqlite3.connect(infodb_location)
    c = conn.cursor()
    command = '''UPDATE DB_INFO SET ISDONE = 1 WHERE DB_LOCATION = ?'''
    para = (db_location,)
    c.execute(command, para)
    conn.commit()
    conn.close()

#选取第一个没处理完成的
def select_unprocessing(infodb_location):
    conn = sqlite3.connect(infodb_location)
    c = conn.cursor()
    cursor = c.execute('''SELECT DB_LOCATION FROM DB_INFO WHERE ISPROCESSING = 0''')
    for row in cursor:
        break
    conn.close()
    return row[0]
    

def get_ip():    
    #min_index = ip[ip['COUNT'] == ip['COUNT'].min()].index[-1]
    #count_list = list(ip['COUNT'])
    #count_list[min_index] += 1
    #ip['COUNT'] = count_list
    #ip.to_csv(ip_pool_location,index=False)
    ip_pool_location = '/Users/administrator/work-collection/crawlers/ip_pool.csv'
    ip = pd.read_csv(ip_pool_location)
    min_index = random.sample(range(len(ip['IP'])),1)
    ip_list = list(ip['IP'])
    return ip_list[min_index[0]]


def get_urls(infodb_location):
    db_location = select_unprocessing(infodb_location)
    processing(infodb_location=infodb_location, db_location=db_location)
    relative_url = []
    TIME = []
    title = []
    conn = sqlite3.connect(db_location)
    c = conn.cursor()

    cursor = c.execute("SELECT RELATIVE_URL from POST")
    for row in cursor:
        relative_url.append(row[0])

    cursor = c.execute("SELECT POST_TIME from POST")
    for row in cursor:
        TIME.append(row[0])    

    cursor = c.execute("SELECT TITLE from POST")
    for row in cursor:
       title.append(row[0])
    
    conn.close()
    return relative_url, TIME, title, db_location

def process_respon(respon):
    post_content = respon.text
    soup = BeautifulSoup(post_content, 'lxml')
    post_content = soup.find("div", {"class":"stockcodec .xeditor"}).text
    post_content = unicodedata.normalize('NFKC', post_content)
    return post_content

def process_one(url):
    global content
    headers = {
    'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.9 Safari/536.5',
    'user-agent1':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.83 Safari/537.36'
    }
    base_url = 'http://guba.eastmoney.com'
    post_url = base_url + url
    count_fail_to_handle = 0
    while True:
        count_time_out = 0
        try:
            dynamic_ip = get_ip()
        except:
            continue
        proxies = {
            'http': dynamic_ip
        }
        try:
            respon = requests.post(post_url, headers=headers, proxies=proxies, verify=False, timeout=10)
        except:
            if count_time_out > 2:
                print("ERROR: TIME OUT")
                return content
            count_time_out += 1
            continue
        try:
            content[url] = process_respon(respon)
            return content
        except:
            if count_fail_to_handle > 2:
                print("ERROR: POST DELETED")
                return content
            count_fail_to_handle += 1

def dict_to_db(relative_url, content, time_dict, title_dict, db_location):
    conn = sqlite3.connect(db_location)
    c = conn.cursor()
    for i in range(len(relative_url)):
        url = relative_url[i]
        TIME = time_dict[url]     
        try:
            post_content = content[url]
        except:
            post_content = title_dict[url]
        try:
            PID = extract_uid(url)
        except:
            PID = url
        para = (PID,TIME,post_content)
        sql_command = '''INSERT INTO POST_TEXT (PID, POST_DETAIL_TIME, POST_CONTENT) VALUES (?,?,?)'''
        c.execute(sql_command, para)
        conn.commit()
    conn.close()
    shutil.move(db_location, '/Users/administrator/work-collection/crawlers/done')
    
    
#被标注处理完成的url
def get_isdone_location():
    done_list = []
    connn = sqlite3.connect('/Users/administrator/work-collection/crawlers/database_information.db')
    cc = connn.cursor()
    rows = cc.execute('SELECT DB_LOCATION FROM DB_INFO WHERE ISDONE = 1')
    for row in rows:
        print(row)
        done_list.append(row[0])
    connn.close()
    return done_list
    
def get_isprocessing_location():
    processing_list = []
    connn = sqlite3.connect('/Users/administrator/work-collection/crawlers/database_information.db')
    cc = connn.cursor()
    rows = cc.execute('SELECT DB_LOCATION FROM DB_INFO WHERE ISPROCESSING = 1')
    for row in rows:
        processing_list.append(row[0])
        print(row)
    connn.close()
    return processing_list

#重设ISDONE DB
def undone_all(infodb_location):
    conn = sqlite3.connect(infodb_location)
    c = conn.cursor()
    c.execute('''UPDATE DB_INFO SET ISDONE = 0 WHERE ISDONE = 1''')
    conn.commit()
    conn.close()

def unprocessing_all(infodb_location):
    conn = sqlite3.connect(infodb_location)
    c = conn.cursor()
    c.execute('''UPDATE DB_INFO SET ISPROCESSING = 0 WHERE ISPROCESSING = 1''')
    conn.commit()
    conn.close()

def reset(done_list):
    for done_location in done_list:
        conn = sqlite3.connect(done_location)
        c = conn.cursor()
        try:
            c.execute('''DELETE FROM POST_TEXT''')
        except:
            c.execute('''CREATE TABLE POST_TEXT(PID INT, POST_DETAIL_TIME CHAR(20), POST_CONTENT CHAR(200))''')
        conn.commit()
        conn.close()

#### 初始化描述表
def main():
    try:
        os.remove('database_information.db')
    except:
        pass
    conn_database_information = sqlite3.connect('database_information.db')
    c_database_infromation = conn_database_information.cursor()
    c_database_infromation.execute('''CREATE TABLE DB_INFO (DB_LOCATION CHAR(50), ISPROCESSING INT, ISDONE INT)''')
    conn_database_information.commit()
    for i in all_path('v1'):
        c_database_infromation_command = '''INSERT INTO DB_INFO (DB_LOCATION, ISPROCESSING, ISDONE) VALUES (?, 0, 0)'''
        c_database_infromation_para = (i, )
        c_database_infromation.execute(c_database_infromation_command, c_database_infromation_para)
    conn_database_information.commit()

if __name__ == "__main__":
    main()
    print('reset_succssfully')
    