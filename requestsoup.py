import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import time
from tqdm import tqdm

class Scraper:
    def __init__(self, base_url, num_pages):
        self.base_url = base_url
        self.num_pages = num_pages
        self.cur_page  = 1
        self.website = 'https://pitchfork.com'
        self.data = []

    def parse(self):
        # loop through every page of Pitchfork reviews
        for i in tqdm(range(1, self.num_pages+1)):
            url = self.base_url + str(i)
            web_page = requests.get(url)
            soup = BeautifulSoup(web_page.content, 'lxml')
            # loop through each review on the page (usually 12)
            for review in soup.find_all('div', class_='review'):
                # get each review link, get the page and scrape it using parse_review function
                rev_ext = review.find('a', class_='review__link')['href']
                rev_url = self.website + rev_ext
                rev_page = requests.get(rev_url)
                rev_data = self.parse_review(rev_page)
                # save the returned data 
                self.data.append(rev_data)

            if self.cur_page % 100 == 0:
                print('%d pages scraped' % self.cur_page)
            self.cur_page = self.cur_page + 1
            
        print('Scraping is complete!')


    # get all relevant data from the review
    def parse_review(self, rev_page):
        rev_data = {}
        rev_soup = BeautifulSoup(rev_page.content, 'lxml')

        album = rev_soup.find('h1', class_='single-album-tombstone__review-title')
        if album: # cathes occassional error where album name wasn't listed
            rev_data['album'] = album.string
        else:
            rev_data['album'] = 'N/A'

        artists = rev_soup.find('ul', class_='artist-links artist-list single-album-tombstone__artist-links')
        if artists: # catching error if no artists are found
            artists = artists.find_all('a')
            artist_list = []
            for artist in artists: # loop to check if there is more than one artist
                artist_list.append(artist.string)
            rev_data['artist'] = ', '.join(artist_list)
        else:
            rev_data['artist'] = 'Various Artists'

        rev_data['score'] = float(rev_soup.find('span', class_='score').string)
        rev_data['date'] = rev_soup.find('time', class_='pub-date').string
        rev_data['reviewer'] = rev_soup.find('a', class_='authors-detail__display-name').string

        bnm = rev_soup.find('p', class_='bnm-txt')
        if bnm: 
            if bnm.string == 'Best new music':
                rev_data['bnm'] = 1
            elif bnm.string == 'Best new reissue':
                rev_data['bnr'] = 1
        else:
            rev_data['bnm'] = 0
            rev_data['bnr'] = 0

        # # text
        # paragraphs = rev_soup.find('div', class_='contents dropcap')
        # if paragraphs:
        #     paragraphs = paragraphs.find_all('p')
        #     paragraph_list = []
        #     for paragraph in paragraphs:
        #         paragraph_list.append(''.join(list(paragraph.strings)))
        #     rev_data['text'] = ' '.join(paragraph_list)
        # else:
        #     rev_data['text'] = None

        # header
        # rev_data['abstract'] = rev_soup.find('div', class_='review-detail__abstract').p.string
        # url
        rev_data['url'] = rev_page.url
        # are there multiple albums being reviewed at once
        # if rev_soup.find('nav', class_='album-picker'):
        #     rev_data['mult_albums'] = 1
        # else:
        #     rev_data['mult_albums'] = 0

        return rev_data

if __name__ == '__main__':

    base_url = 'https://pitchfork.com/reviews/albums/?page='
    total_pages = 1685
    print('Initializing scraper...')
    pitchfork_scraper = Scraper(base_url, total_pages)

    print('Running parse function...')
    pitchfork_scraper.parse()

    print('Saving data to csv...')
    final_data = pd.DataFrame(pitchfork_scraper.data)
    final_data.to_csv('pitchfork_reviews_data3.csv')
    print("Done!")
