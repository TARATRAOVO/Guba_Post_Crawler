import asyncio
from aiohttp import request
from aiomultiprocess import Pool
from asyncio import tasks
import re
import requests
import sqlite3
from bs4 import BeautifulSoup
import time
import re
import threading
import time
import os
import pandas as pd
import random
import os
import shutil
import csv

def processing(infodb_location, db_location):
    conn_database_information = sqlite3.connect(infodb_location)
    c_database_infromation = conn_database_information.cursor()
    command = '''UPDATE DB_INFO SET ISPROCESSING = 1 WHERE DB_LOCATION = ?'''
    para = (db_location,)
    c_database_infromation.execute(command, para)
    conn_database_information.commit()
    conn_database_information.close()

def select_unprocessing(infodb_location):
    conn = sqlite3.connect(infodb_location)
    c = conn.cursor()
    cursor = c.execute('''SELECT DB_LOCATION FROM DB_INFO WHERE ISPROCESSING = 0''')
    for row in cursor:
        break
    conn.close()
    return row[0]

def get_ip():    
    while True:
        try:
            ip_pool_location = 'ip_pool.csv'
            ip = pd.read_csv(ip_pool_location)
            min_index = random.sample(range(len(ip['IP'])),1)
            ip_list = list(ip['IP'])
            break
        except:
            continue
    return ip_list[min_index[0]]

def get_urls_info(infodb_location):
    urls_info = []  
    db_location = select_unprocessing(infodb_location)
    print(db_location)
    processing(infodb_location=infodb_location, db_location=db_location)
    conn = sqlite3.connect(db_location)
    c = conn.cursor()
    
    relative_urls = []
    TIME = []
    title = []
    
    cursor = c.execute("SELECT RELATIVE_URL from POST")
    for row in cursor:
        relative_urls.append(row[0])

    cursor = c.execute("SELECT POST_TIME from POST")
    for row in cursor:
        TIME.append(row[0])    

    cursor = c.execute("SELECT TITLE from POST")
    for row in cursor:
        title.append(row[0])
    conn.close()
    
    for i in range(len(relative_urls)):
        urls_info.append([relative_urls[i],TIME[i],title[i],db_location])
    return urls_info



async def get_respon(url_info):
    count = 0
    while True:
        try:
            proxy = get_ip()
        except:
            continue
        url = 'http://guba.eastmoney.com' + url_info[0]
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36'}
        try:
            async with request("GET", url, proxy=proxy, headers=headers) as response:
                return [await response.text(), url_info[0], url_info[1], url_info[2], url_info[3]]
        except:
            if count>2:
                print(url_info[0])
                print("ERROR")
                return ["ERROR", url_info[0], url_info[1], url_info[2], url_info[3]]
            count += 1
            continue
     
def extract_uid(relative_url):
    relative_url = re.search(',(.*)html', relative_url).group(1)
    relative_url = re.search(',(.*).', relative_url).group(1)
    return relative_url

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
    shutil.move(db_location, '/done')

_content = {}
def savedata(respon):
    global _content
    try:
        post_content = respon[0]
        soup = BeautifulSoup(post_content, 'lxml')
        post_content = soup.find("div", {"class":"stockcodec .xeditor"}).text
        print(respon[1])
        print(post_content)
        _content[respon[1]] = post_content
    except:
        _content[respon[1]] = "DELETED"
        print(respon[1])
        print("DELETED")

async def main():
    start = time.time()
    infodb_location = 'database_information.db'
    urls_info = get_urls_info(infodb_location)
    time_dict = {}
    title_dict = {}
    relative_urls_list = []
    db_location = urls_info[0][3]
    
    for i in range(len(urls_info)):
        time_dict[urls_info[i][0]] = urls_info[i][1]
        title_dict[urls_info[i][0]] = urls_info[i][2]
        relative_urls_list.append(urls_info[i][0])
        
    async with Pool() as pool:
        async for result in pool.map(get_respon, urls_info):
            savedata(result)

    dict_to_db(relative_urls_list, _content, time_dict, title_dict, db_location)
    end = time.time()
    print(db_location)
    print(len(urls_info), start-end) 
            

if __name__ == "__main__":
    asyncio.run(main())



