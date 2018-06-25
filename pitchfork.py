import scrapy
from scrapy import Item, Field, Spider
from scrapy.http import Request
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose, Join
import time

class DataItem(Item):
    score = Field()
    date = Field()
    album = Field(
        input_processor=MapCompose(str.strip),
        output_processor= Join(';')  
    )
    artist = Field(
        input_processor=MapCompose(str.strip),
        output_processor= Join(';')  
    )
    reviewer = Field()
    url = Field()
    bnm = Field()
    bnr = Field()

class PitchforkSpider(Spider):
    name = 'pitchfork_spider'
    num_pages = 1688 # might change as more reviews are added, currently 1688
    cur_page = 1
    base_url = 'https://pitchfork.com/reviews/albums/?page=%d'
    start_urls = [base_url % 1]
    # download_delay = 0.2
    custom_settings = {
        'FEED_FORMAT': 'csv',
        'FEED_URI': 'reviews_stripped.csv',
        'FEED_EXPORT_ENCODING': 'utf-8',
        'LOG_LEVEL': 'WARNING',
        'LOG_FILE': 'log.txt',
        'SCHEDULER_DEBUG': True,
        'MEMDEBUG_ENABLED': True,
        'DOWNLOAD_FAIL_ON_DATALOSS': False
    }

    def parse(self, response):
        # get each review on the page and visit the link
        for review in response.css('div.review'):
            yield Request('https://pitchfork.com' + review.css('a::attr(href)').extract_first(),
                          callback=self.parse_review)
        # print every page so you can just restart when it fails
        if self.cur_page % 100 == 0:
            print(self.cur_page, 'pages scraped')
        self.cur_page = self.cur_page + 1
        if self.cur_page <= self.num_pages:
            yield Request(self.base_url % self.cur_page)

    # oh baby this parses those 'multi-album' reviews and gets all names and scores
    def parse_review(self, response):
        # get the relevant data
        l = ItemLoader(item=DataItem(), response=response)
        # l.default_output_processor = MapCompose(str.strip)
        l.add_css('score', 'span.score::text')
        l.add_css('date', 'time.pub-date::text')
        l.add_css('album', 'h1.single-album-tombstone__review-title::text')
        l.add_xpath('artist', '//hgroup[@class="single-album-tombstone__headings"]//a/text()')
        l.add_css('reviewer', 'a.authors-detail__display-name::text')
        l.add_value('url', response.url)
        bnm = response.css('p.bnm-txt::text').extract_first()
        if bnm == 'Best new music':
            l.add_value('bnm', '1')
        else:
            l.add_value('bnm', '0')
        if bnm == 'Best new reissue':
            l.add_value('bnr', '1')
        else:
            l.add_value('bnr', '0')

        yield l.load_item()


        # data['date'] = response.css('time.pub-date::text').extract_first()
        # data['album'] = response.css('h1.single-album-tombstone__review-title::text').extract_first()
        # data['artist'] = response.xpath('//hgroup[@class="single-album-tombstone__headings"]//a/text()').extract() # 1 or more
        # data['reviewer'] = response.css('a.authors-detail__display-name::text').extract_first()
        # # data['text'] = ' '.join(response.xpath('//div[@class="contents dropcap"]//p/text()').extract())
        # data['url'] = response.url
        # bnm = response.css('p.bnm-txt::text').extract_first()
        # data['bnm'] = 1 if bnm == 'Best new music' else 0 # get best new music
        # data['bnr'] = 1 if bnm == 'Best new reissue' else 0

