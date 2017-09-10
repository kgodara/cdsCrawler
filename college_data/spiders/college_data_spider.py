import scrapy
#import urllib2
import sys
from io import StringIO
from scrapy.contrib.spiders import Rule
from scrapy.linkextractors import LinkExtractor
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
import os, errno
import datetime
import fitz
import time
from urllib.parse import urljoin
from urllib.parse import urldefrag
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
import pandas as pd


done_domain = []
start_time = time.time()
cds = 'Common Data Set'
cds2 = 'CDS'
factbook = 'Fact Book'
#factbook2 = 'Facts'
factbook3 = 'Factbook'
df = pd.read_csv('SuperClean.csv')
domain_list = df['Institution_Web_Address']
college_name = df['Institution_Name']
college_dict = {}

def get_domain(domain):
	domain2 = domain.split('.edu')
	if '.' not in domain2[0]:
		if '://' in domain2[0]:
			domain2[0] = domain2[0][domain2[0].index('://')+3:len(domain2[0])]
	domain2 = domain2[0].split('.')[-1]
	return domain2

class CollegeDataSpider(scrapy.Spider):
	name = "college_data"
	num = 0



	def __init__(self):
		dispatcher.connect(self.spider_closed, signals.spider_closed)

	def spider_closed(self, spider):
		# second param is instance of spder about to be closed.
		thefile = open('domains.txt', 'w+')
		for domain in done_domain:
			thefile.write("%s\n" % domain)




	def start_requests(self):

		with open('domains.txt', 'r+') as myfile:
   			done_domain = myfile.read().split('\n')
		urls = [
			'http://irp.dpb.cornell.edu/wp-content/uploads/2012-Parent-Survey-presentation-to-SAS.pdf',
			'http://dpb.cornell.edu/documents/1000569.pdf'
		]
		rules = (
		# Extract links matching 'category.php' (but not matching 'subsection.php')
		# and follow links from them (since no callback means follow=True by default).
		Rule(LinkExtractor(deny=('facebook\.com', 'books\.google', '\.php', '\.aspx', '\.xls', '\.ppt', '\.pptx', '\.xlsx', '\.docx', 'mailto:', 'tel:', 'related:', 'cache:'))),

		# Extract links matching 'item.php' and parse them with the spider's method parse_item
		#Rule(LinkExtractor(allow=('item\.php', )), callback='parse_item'),
		)
		#for url in urls:
		#	yield scrapy.Request(url=url, callback=self.save_pdf)
		#df = pd.read_csv('SuperClean.csv')
		#domain_list = df['Institution_Web_Address']
		#college_name = df['Institution_Name']
		print('BINGALEE: ' + str(len(domain_list)))
		print('DINGALEE: ' + str(len(college_name)))
		print(*college_name[0:10], sep='\n')

		for idx, domain in enumerate(domain_list):
			domain = str(domain)
			if 'mailto:' not in domain and 'tel:' not in domain:
				if '.edu' in domain:

					domain2 = get_domain(domain)
					try:
						os.makedirs('colleges/' + domain2)
					except OSError as e:
						if e.errno != errno.EEXIST:
							raise

					if domain2 not in done_domain:
						college_dict[str(college_name[idx])] = idx
						yield scrapy.Request(url='https://www.google.com/search?q=%s+Common+Data+Set' % (college_name[idx].replace(' ', '+')), callback=self.college_parse)

		"""
		lineNum = 0
		with open("SlicedAccredited.csv") as infile:
			for line in infile:
				if 'Institution_Name' in line[15:31]:
		#			#print('FOUND IT')
					continue;
			#LINES BELOW THIS ARENT IN LOOP
				college_name = line[line.index(',')+1:line.index(',', line.index(',') + 1)].replace(' ', '+')
			#print('MAKING NEW REQUEST --------------------------------------------------')
		#		print('BONOBO: https://www.google.com/search?q=%s+Common+Data+Set' % college_name)
				#time.sleep(5.5)
				yield scrapy.Request(url='https://www.google.com/search?q=%s+Common+Data+Set' % college_name, callback=self.college_parse)
				if lineNum > 9:
					break
				lineNum += 1
			"""
		print(*done_domain, sep=' ')
		#yield scrapy.Request(url='https://www.reddit.com/r/chanceme/wiki/common_data_sets', callback=self.college_parse)

	def college_parse(self, response):
		#print('BONOBO: ' + response.url)

		#print('PRINTING CSS RESPONSE')
		#print(response.css('a[href*=\.edu]'))
		#print('PRINTING XPATH RESPONSE')
		#dirty_url = str(response.xpath('//a[contains(@href, ".edu")]').extract_first())
		
		#print(joe)
		#print('SLICE')
		#print(joe[int(joe.index('?q=') + 3):int(joe.index('&amp'))])

		#google search
		print('RESPONSE URL: ' + response.url)
		print(*done_domain, sep=' ')
		#if(response.meta['depth'] < 2):
		#if we are on college webiste
		print('BINGALEE: ' + str(len(domain_list)))
		print('DINGALEE: ' + str(len(college_name)))


		if(response.meta['depth'] < 2):
			if('.edu' not in response.url):
				# if response is in a pdf stop scraping
				college_name_extract =  response.url[response.url.index('?q=')+3:response.url.index('+Common+Data')].replace('+', ' ')
				index = college_dict[college_name_extract]
				print('INDEX: ' + str(index))
				print('LENGTH: ' + str(len(domain_list)))
				print('----------------------------------------')
				print('----------------------------------------')
				print('----------------------------------------')
				checkerDomain = get_domain(domain_list[index])




				if '.pdf' not in response.url or '.PDF' not in response.url:
					#print('TRYING SUSPECT')
					for dirty_url in response.css('h3.r a').xpath('@href').extract():#.xpath('//a[contains(@href, ".edu")]/@data-href').extract():
						
						dirty_url =  urldefrag(dirty_url)
						dirty_url = dirty_url[0]
						slice_url = str(dirty_url)
						print('CHECKER DOMAIN: ' + checkerDomain)
						if checkerDomain in slice_url:
							#print('--------------------------------------------------')
							# IF we are going from google
							if ('?q=' in slice_url):
								#print('SLICE URL: ' + slice_url + '\n')
								slice_url = slice_url[int(slice_url.index('?q=') + 3):int(slice_url.index('&'))]
								if ('.pdf' in slice_url):
									#if 'CDS' in slice_url or 'Common'
									#print('--------------------------------------------------')
									#print(slice_url)
									#print('--------------------------------------------------')
									if not dirty_url.endswith('.pdf'):
										dirty_url = dirty_url[0:dirty_url.rfind('.pdf')+4]
									yield scrapy.Request(slice_url, callback=self.save_pdf)
								else:
									#print('--------------------------------------------------')
									#print(slice_url)
									#print('--------------------------------------------------')
									#ADD CHECK TO MAKE SURE WE DONT GET REDIRECT LOOP
									if '.edu' in domain:
										domain = get_domain(response.url)
										if domain not in done_domain:
											yield response.follow(slice_url, callback=self.college_parse)
							else:
								slice_url = str(dirty_url)
								#print('SLICE URL: ' + slice_url + '\n')
								if ('.pdf' in slice_url):
									#if 'CDS' in slice_url or 'Common'
									#print('--------------------------------------------------')
									#print(slice_url)
									#print('--------------------------------------------------')
									if not dirty_url.endswith('.pdf'):
										dirty_url = dirty_url[0:dirty_url.rfind('.pdf')+4]
									yield scrapy.Request(slice_url, callback=self.save_pdf)
								else:
									#print('--------------------------------------------------')
									#print(slice_url)
									#print('--------------------------------------------------')
									#ADD CHECK TO MAKE SURE WE DONT GET REDIRECT LOOP
									domain = ''
									if '.edu' in response.url:
										domain = get_domain(response.url)
										if domain not in done_domain:
											yield response.follow(slice_url, callback=self.college_parse)



			elif('.edu' in response.url):
					# need to actually search for links that don't contain .edu
				if '.aspx' not in response.url:
					for dirty_url in response.xpath('//a[contains(@href, "www.") and contains(@href, ".edu")]/@href').extract():
						dirty_url =  urldefrag(dirty_url)
						dirty_url = dirty_url[0]
					#for dirty_url in response.xpath('//a[contains(@href, ".edu") and contains("www."))]/@href').extract():
						# IF we are going from college website
						if 'http' not in dirty_url:
							dirty_url = urljoin(response.url, dirty_url)

						#print('CHECK FOR GOOFS: ' + dirty_url)
						try:
							if ('/download' in dirty_url):
								#print('FOUND A GOOFER: ' + dirty_url)
								driver = webdriver.Chrome()
								driver.get(dirty_url)
								#find_elements
								nexto = driver.find_element_by_xpath('//a[contains(@href, "'+dirty_url+'")]')
								nexto.click()

						except :
							print('Handling run-time error:')

						if (dirty_url.endswith('.pdf')):
							#if 'CDS' in edu or 'Common'
							print(' PDF PDF PDF PDF PDF PDF PDF PDF PDF PDF')
							#print(dirty_url)
							#print(' PDF PDF PDF PDF PDF PDF PDF PDF PDF PDF')
							if not dirty_url.endswith('.pdf'):
								dirty_url = dirty_url[0:dirty_url.rfind('.pdf')+4]
							yield scrapy.Request(dirty_url, callback=self.save_pdf)
						else:
							#print('--------------------------------------------------')
							#print(slice_url)
							#print('--------------------------------------------------')
							# Track sites which mention a CDS but don't have a PDF link for further analysis
							allText = response.css('body').extract_first()
							if(cds.lower() in allText.lower() or cds2.lower() in allText.lower() or factbook.lower() in allText.lower() or factbook3.lower() in allText.lower()):
								#print('DIRTY STUFF: ' + dirty_url)
								if(response.meta['depth'] < 2):
									print('DEPTH: ' + str(response.meta['depth']) + '<' + response.url + '>')
									yield response.follow(dirty_url, callback=self.college_parse)



					for dirty_url in response.xpath('//a[@href[not(contains(.,"://"))]]/@href').extract():
						dirty_url =  urldefrag(dirty_url)
						dirty_url = dirty_url[0]
						# IF we are going from college website
						#print('CHECK FOR GOOFS 2: ' + dirty_url)
						if 'http' not in dirty_url:
							dirty_url = urljoin(response.url, dirty_url)
						
						
						if ('/download' in dirty_url):
							driver = webdriver.Chrome()
							driver.get(dirty_url)
							nexto = driver.find_element_by_xpath('//a[@href="'+dirty_url+'"]')
							nexto.click()
						elif ('.pdf' in dirty_url):
							#if 'CDS' in edu or 'Common'
							print(' PDF PDF PDF PDF PDF PDF PDF PDF PDF PDF')
							#print(dirty_url)
							#print(' PDF PDF PDF PDF PDF PDF PDF PDF PDF PDF')
							if not dirty_url.endswith('.pdf'):
								dirty_url = dirty_url[0:dirty_url.rfind('.pdf')+4]
							yield scrapy.Request(dirty_url, callback=self.save_pdf)
						else:
							#print('--------------------------------------------------')
							#print(slice_url)
							#print('--------------------------------------------------')
							# Track sites which mention a CDS but don't have a PDF link for further analysis
							allText = response.css('body').extract_first()
							if(cds.lower() in allText.lower() or cds2.lower() in allText.lower() or factbook.lower() in allText.lower() or factbook3.lower() in allText.lower()):
							#print('DIRTY STUFF: ' + dirty_url)
								if(response.meta['depth'] < 2):
									print('DEPTH: ' + str(response.meta['depth']) + '<' + response.url + '>')
									yield response.follow(dirty_url, callback=self.college_parse)
		else:
			if '.pdf' not in response.url or '.PDF' not in response.url:
				for dirty_url in response.xpath('//a[contains(@href, ".edu") and contains(@href, ".pdf")]/@href').extract():
					dirty_url =  urldefrag(dirty_url)
					dirty_url = dirty_url[0]
					if 'http' not in dirty_url:
								dirty_url = urljoin(response.url, dirty_url)
					if not dirty_url.endswith('.pdf'):
						dirty_url = dirty_url[0:dirty_url.rfind('.pdf')+4]
					if ('/download' in dirty_url):
						driver = webdriver.Chrome()
						driver.get(dirty_url)
						nexto = driver.find_element_by_xpath('//a[@href="'+dirty_url+'"]')
						nexto.click()
					else:	
						print(' PDF PDF PDF PDF PDF PDF PDF PDF PDF PDF')
						#print(dirty_url)
						#print(' PDF PDF PDF PDF PDF PDF PDF PDF PDF PDF')
						yield scrapy.Request(dirty_url, callback=self.save_pdf)



	def save_pdf(self, response):
		print('BONOBO: ' + response.url)
		path = response.url.split('/')[-1]
		#path = str(response.url)
	#	path = path[path.index('http'):path.index('.pdf')+4]

		# get rid of things after domain
		domain = get_domain(response.url)
		#print('FINAL DOMAIN: ' + domain)

		# get second to last '.' and go to beginning of edu
		try:
			os.makedirs('colleges/' + domain)
		except OSError as e:
			if e.errno != errno.EEXIST:
				raise

		path = 'colleges/' + domain + '/' + path

		with open(path, "wb+") as f:
			f.write(response.body)

		cds = 'Common Data Set'
		cds2 = 'CDS'
		factbook = 'Fact Book'
		#factbook2 = 'Facts'
		factbook3 = 'Factbook'	

		doc = fitz.open(path)
		n = 0
		found = 0
		pages_read = 0
		while n < doc.pageCount:
			page = doc.loadPage(n)
			text = page.getText("text")

			if(cds.lower() in text.lower() or cds2.lower() in text.lower() or factbook.lower() in text.lower() or factbook3.lower() in text.lower()):
				found = 1
				break
			#print(text)
			pages_read += 1
			if pages_read > 3:
				break


		if found == 0:
			print('DELETED')
			os.remove(path)
		if found == 1:
			if domain not in done_domain:
				done_domain.append(domain)


		print('--------------------------------------------------')
		print('--------------------------------------------------')
		print("--- %s seconds ---" % (time.time() - start_time))