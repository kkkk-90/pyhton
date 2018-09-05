# -*- coding: utf-8 -*-
import json
import re
import time

from scrapy.conf import settings

import db
from ..common import function
from ..items import *


class YoutubeKey(db.Db, scrapy.Spider):
    name = 'youtube_key'

    proxy_file = settings.get('PROXY_FILE', '')

    queue = 'zset:smusic:up:youtube:song'
    queue_doing = 'z:youtube:key:doing'
    notify_queue = 'lists:notice:smusic:up:youtube:song'
    batch = 32

    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.62 Safari/537.36',
        'DOWNLOADER_MIDDLEWARES': {
            'rotating_proxies.middlewares.RotatingProxyMiddleware': 610,
            'rotating_proxies.middlewares.BanDetectionMiddleware': 620,
        },
        'DOWNLOAD_DELAY': 10,
        'ROTATING_PROXY_LIST': function.load_lines(proxy_file),
    }

    def __init__(self, *args, **kwargs):
        super(YoutubeKey, self).__init__(*args, **kwargs)

    def test(self):
        query = 'Revelations Valdi Sabev'
        song_id = '905260226033602958'

        url = 'https://www.youtube.com/results?search_query={} official'

        yield scrapy.Request(url.format(query), meta={'song_id': song_id},
                             dont_filter=True, callback=self.parse)

    def prod(self):
        while 1:
            time_pass = time.time() - 60 * 5
            res_queue = self.redis_cli.zrangebyscore(self.queue_doing, 0, time_pass)
            if not res_queue:
                self.logger.info('%s Don\'t need add to pending' % (function.now()))
            else:
                for r in res_queue:
                    self.redis_cli_solo.zadd(self.queue, r, time.time() - 1000000000)
                    self.redis_cli.zrem(self.queue_doing, r)
                    msg_queue = json.loads(r)
                    self.logger.info('%s Add pending: %s' % (function.now(), msg_queue))

            res = self.redis_cli_solo.zrevrange(self.queue, 0, self.batch)
            if not res:
                function.std('empty', 'res')
                time.sleep(10)
                continue

            for r in res:
                self.redis_cli.zadd(self.queue_doing, r, time.time())
                self.redis_cli_solo.zrem(self.queue, r)
                msg = json.loads(r)
                if not msg:
                    self.redis_cli.zrem(self.queue_doing, r)
                    function.std('empty', 'msg')
                    continue

                function.std(msg, 'json')
                self.logger.info('%s get json: %s' % (function.now(), msg))

                query = msg['query']
                song_id = msg['song_id']

                url = 'https://www.youtube.com/results?search_query={} official'

                yield scrapy.Request(url.format(query), meta={'song_id': song_id, 'raw': r},
                                     dont_filter=True, callback=self.parse)

    def start_requests(self):
        func = getattr(self, 'func', 'prod')
        if func:
            return getattr(self, func)()

    def parse(self, response):
        raw = response.meta.get('raw', '')
        song_id = response.meta.get('song_id', '')

        html_data = re.findall('window\[\"ytInitialData\"\] =(.*?);', response.body)
        if not html_data:
            doc = DocItem('result')
            doc.push({
                '_id': song_id,
                'result': 'no data',
                'time': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
            })
            yield doc
            self.redis_cli.zrem(self.queue_doing, raw)
            return

        try:
            real = html_data[0].encode('utf-8')
        except:
            doc = DocItem('result')
            doc.push({
                '_id': song_id,
                'result': 'no real',
                'time': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
            })
            yield doc
            self.redis_cli.zrem(self.queue_doing, raw)
            return

        # get json
        try:
            data = json.loads(real)
        except:
            doc = DocItem('result')
            doc.push({
                '_id': song_id,
                'result': 'no real',
                'time': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
            })
            yield doc
            self.redis_cli.zrem(self.queue_doing, raw)
            return

        videoRenders = \
            data['contents']['twoColumnSearchResultsRenderer']['primaryContents']['sectionListRenderer']['contents'][0][
                'itemSectionRenderer']['contents']

        if not videoRenders or not isinstance(videoRenders, list) or not videoRenders[0]:
            doc = DocItem('result')
            doc.push({
                '_id': song_id,
                'result': 'no video',
                'time': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
            })
            yield doc
            self.redis_cli.zrem(self.queue_doing, raw)
            return

        try:
            youtube_key = videoRenders[0]['videoRenderer']['videoId']
        except:
            doc = DocItem('result')
            doc.push({
                '_id': song_id,
                'result': 'no key',
                'time': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
            })
            yield doc
            self.redis_cli.zrem(self.queue_doing, raw)
            return

        self.logger.info("youtube_key:" + youtube_key)

        if not youtube_key:
            doc = DocItem('result')
            doc.push({
                '_id': song_id,
                'result': 'no key',
                'time': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
            })
            yield doc

        # 通知songbook
        document = {
            'key': youtube_key,
            'song_id': song_id,
        }

        json_str = json.dumps(document)
        self.logger.info('%s notify json : %s' % (function.now(), json_str))
        self.redis_cli_solo.rpush(self.notify_queue, json_str)
        self.redis_cli.zrem(self.queue_doing, raw)
