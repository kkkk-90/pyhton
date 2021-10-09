# -*- coding: utf-8 -*-

from db import Base
from ..items import *
from ..common import function
from scrapy.conf import settings
import json
import re
import time


class YoutubeVideoCarwling(Base):
	name = 'youtubevideocarwling'
	dbname = 'youtube_video'
	host = 'https://www.youtube.com/'
	
	proxy_file = settings.get('PROXY_FILE', '')
	custom_settings = {
		'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36',
		'DOWNLOADER_MIDDLEWARES': {
			'rotating_proxies.middlewares.RotatingProxyMiddleware': 610,
			'rotating_proxies.middlewares.BanDetectionMiddleware': 620,
		},
		'DOWNLOAD_DELAY': 10,
		'ROTATING_PROXY_LIST': function.load_lines(proxy_file),
	}
	
	def prod(self):
		with open('/home/worker/ze.ll/video.csv') as f:
			for i in f:
				url = []
				page = 1
				arr = i.split('\t')
				if re.compile(r'youtube(.*)', re.I).findall(arr[2]):
					url_host = arr[2] + '&page=%s'
					print url_host
					yield scrapy.Request(url_host % (str(page)), callback=self.parse_infu,
					                     meta={'arr': arr, 'url': url, 'page': page}, dont_filter=True)
	
	def parse_infu(self, response):
		arr = response.meta.get('arr', '')
		url = response.meta.get('url', '')
		page = response.meta.get('page', '')
		html_data = re.findall('window\[\"ytInitialData\"\] =(.*?);', response.body)
		try:
			real = html_data[0].encode('utf-8')
			data = json.loads(real)
			videoRenders = \
				data['contents']['twoColumnSearchResultsRenderer']['primaryContents']['sectionListRenderer']['contents'][0][
					'itemSectionRenderer']['contents']
			for i in videoRenders:
				if 'videoRenderer' not in i:
					continue
				youtube_key = i['videoRenderer']['videoId']
				youtube_time = i['videoRenderer']['lengthText']['simpleText']
				a = youtube_time.split(':')
				if int(a[0]) < 4:
					url.append('https://www.youtube.com/watch?v=' + str(youtube_key))
		except:
			print 'error'
		if len(url) < 100:
			page += 1
			page_url = arr[2] + '&page=%s' % (str(page))
			yield scrapy.Request(page_url, callback=self.parse_infu, meta={'arr': arr, 'url': url, 'page': page},
			                     dont_filter=True)
		else:
			for item in url:
				yield scrapy.Request(item, callback=self.parse_video, meta={'arr': arr, 'get_url': item}, dont_filter=True)

	def parse_video(self, response):
		arr = response.meta.get('arr', '')
		get_url = response.meta.get('get_url', '')
		youtube_key = re.findall('https://www\.youtube\.com/watch\?v=(.*)', get_url)
		html_data = re.findall('window\[\"ytInitialData\"\] =(.*?);\s+window', response.body)
		try:
			real = html_data[0].encode('utf-8')
			data = json.loads(real)
			data_content = data['contents']['twoColumnWatchNextResults']['results']['results']['contents'][0][
				'videoPrimaryInfoRenderer']
			title = data_content['title']['simpleText']
			like_num = \
				data_content['videoActions']['menuRenderer']['topLevelButtons'][0]['toggleButtonRenderer']['defaultText'][
					'accessibility']['accessibilityData']['label'].split(' ')
			
			views_num = data_content['viewCount']['videoViewCountRenderer']['viewCount']['simpleText'].split(' ')
			doc = {
				'_id': self.function.to_md5(arr[0] + arr[1] + arr[3] + youtube_key[0]),
				'tag': arr[0],
				'hash_tag': arr[1],
				'lang': arr[3],
				'title': title,
				'like_num': like_num[0],    # 点赞数
				'share_num': 0,  		    # 分享数(没有分享数)
				'comment_num': 0,           # 评论数(未拿到)
				'views_num': views_num[0],  # 观看数
				'source_host': 'https://www.youtube.com/watch?v=' + str(youtube_key[0]),
				'resource': '',
				'storage_path': self.settings.get('FILES_STORE_COS', '') + 'youtube/%s.mp4' % youtube_key[0]
			}
			print(doc)
			
			# 存入mongo
			save = DocItem('youtube_video')
			save.push(doc)
			yield save
			# 存入cos
			youtube = YoutubeItem()
			youtube.push(youtube_key[0])
			yield youtube
		except:
			print 'data error'
