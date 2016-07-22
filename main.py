#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import time

import Queue
import threading
import logging

import urllib
import urllib2

import httplib
import StringIO
import gzip
import json
import re
import string
import db.db


# yunData.SHARE_ID = "1653063156";
# yunData.PATH = "\/2016新番\/七月\/周三";
# yunData.DOCPREVIEW


PERSHAREPAGENUM = 60
PERFOLLOWPAGENUM = 24
PERFANSPAGENUM = 24

URL_GETSHARELIST = "http://yun.baidu.com/pcloud/feed/getsharelist?" \
                   "auth_type=1&start={start}&limit={perpagenum}&query_uk={uk}"

URL_SHARELIST = "http://yun.baidu.com/share/list?uk=792173160&shareid=526789938&page=1&num=100" \
                "&dir={path}&desc=1&channel=chunlei&web=1&clienttype=0"

URL_GETFOLLOWLIST = "http://yun.baidu.com/pcloud/friend/getfollowlist?query_uk={uk}" \
                    "&limit={perpagenum}&start={start}"

URL_GETFANSLIST = "http://yun.baidu.com/pcloud/friend/getfanslist?query_uk={uk}&limit={PERFANSPAGENUM}&start={start}"

headers = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Encoding": "gzip, deflate, sdch",
    "Accept-Language": "zh-CN,zh;q=0.8",
    "Cache-Control": "max-age=0",
    "Connection": "keep-alive",
    "Host": "pan.baidu.com",
    "Referer":"http://pan.baidu.com/",
    "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML,like Gecko) \
    Chrome/50.0.2661.94 Safari/537.36",
    "X-Requested-With": "XMLHttpRequest",
    # 'Cookie': ""
}

# 任务url
http_request = Queue.Queue(256)

# http_accept = Queue.Queue(256)


# 解压gzip
def gzdecode(data):
    compressedstream = StringIO.StringIO(data)
    gziper = gzip.GzipFile(fileobj=compressedstream)
    decodejson_data = gziper.read()
    return decodejson_data


# 下一页
def nextsharepage(share_url, total_count):
    pattern = re.compile("start=\d+")
    match = re.search(pattern, share_url)
    if match:
        match_str = match.group()
        current_index = string.atoi(match_str[6:])
        sum_page = total_count / PERFOLLOWPAGENUM
        if current_index < sum_page:
            next_page = "start=" + str(current_index + 1)
            share_url = re.sub(pattern, next_page, share_url)
            print share_url
            http_request.put(share_url)


# 下一页
def nextfallowpage(share_url, total_count):
    pattern = re.compile("start=\d+")
    match = re.search(pattern, share_url)
    if match:
        match_str = match.group()
        current_index = string.atoi(match_str[6:])
        sum_page = total_count / PERSHAREPAGENUM
        if current_index < sum_page:
            next_page = "start=" + str(current_index + 1)
            share_url = re.sub(pattern, next_page, share_url)
            print share_url
            http_request.put(share_url)


# 任务
def master():
    while True:
        onelist = db.db.select_one("select * from urlids where is_read=0 ")
        if onelist is None:
            continue
        db.db.update("update urlids set is_read=?,last_modified=? where uk=?", 1, time.time(), onelist.uk)
        share_url = URL_GETSHARELIST.format(uk=onelist.uk, start=0, perpagenum=PERSHAREPAGENUM).encode('utf-8')
        http_request.put(share_url)
        follow_url = URL_GETFOLLOWLIST.format(uk=onelist.uk, start=0, perpagenum=PERFOLLOWPAGENUM).encode('utf-8')
        http_request.put(follow_url)
        # fans_url = URL_GETFANSLIST.format(uk=onelist.uk, start=0, perpagenum=PERSHAREPAGENUM).encode('utf-8')
        # http_request.put(fans_url)


# 处理
def worker():
    try:
        # url = URL_GETSHARELIST.format(uk=uk, start=start, perpagenum=PERSHAREPAGENUM).encode('utf-8')
        # url = 'http://yun.baidu.com/pcloud/feed/getsharelist?auth_type=1&start=0&limit=60&query_uk=792173160'
        url = http_request.get()
        request = urllib2.Request(url, headers=headers)
        response = urllib2.urlopen(request)
        encodedjson = gzdecode(response.read())
        decodejson = json.loads(encodedjson)

        if 0 != decodejson["errno"]:
            return

        if 'getsharelist' in url:
            print decodejson["request_id"]
            print decodejson["total_count"]
            total_count = decodejson["total_count"]
            nextsharepage(url, total_count)
            if "records" in decodejson.keys():
                for item in decodejson["records"]:
                    # print item["uk"], item["shareid"]
                    # print item["title"], "http://pan.baidu.com/s/%s" % item["shorturl"]
                    title = item["title"]
                    http_link = "http://pan.baidu.com/s/%s" % item["shorturl"]
                    link_info = dict(title=title, http_link=http_link)
                    db.db.insert("link", **link_info)
        elif 'getfollowlist' in url:
            print decodejson["request_id"]
            print decodejson["total_count"]
            total_count = decodejson["total_count"]
            nextfallowpage(url, total_count)
            if "follow_list" in decodejson.keys():
                for item in decodejson["follow_list"]:
                    # print item["follow_uk"], item["follow_uname"]
                    # uk_info = dict(uk=item["follow_uk"], is_read=0, last_modified=0)
                    # db.db.insert("urlids", **uk_info)
                    uk = item["follow_uk"]
                    is_read = 0
                    last_modified = 0
                    sql = "INSERT INTO urlids(uk, is_read, last_modified) " \
                          "SELECT '{uk}', '{is_read}', '{last_modified}' FROM DUAL " \
                          "WHERE NOT EXISTS(SELECT uk FROM urlids WHERE uk = {primary_key})"
                    sql = sql.format(uk=uk, is_read=is_read, last_modified=last_modified, primary_key=uk).encode('utf-8')
                    db.db.update(sql)
        elif 'getfanslist' in url:
            pass
    except urllib2.URLError, e:
        if hasattr(e, "code"):
            print e.code
        if hasattr(e, "reason"):
            print e.reason


def worker_wrap():
    while True:
        worker()
        time.sleep(8)

if __name__ == '__main__':
    print 'started at:', time.time()
    logging.basicConfig(level=logging.DEBUG)
    db.db.create_engine('root', '465084127', 'wp', '192.168.1.101')
    for item in range(4):
        worker_thread = threading.Thread(target=worker_wrap, args=())
        worker_thread.setDaemon(True)
        worker_thread.start()

    # worker_thread.join()
    master()
    print 'done at:', time.time()
