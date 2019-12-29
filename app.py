from flask import Flask
import scrapy
from scrapy import *
import urllib.parse as urlparse
import json
import pandas as pd
import numpy as np
import requests

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

import twitter_credentials

app = Flask(__name__)

@app.route('/')
def hello_world():
    return 'Hello World!'



@app.route('/bms/<string:mid>')
def bookMyShow(mid):
    url1 = 'https://in.bookmyshow.com/ahmedabad/movies/'
    url=url1+mid+'/user-reviews'

    response = scrapy.Request(url)
    res = requests.get(url)
    response = scrapy.Selector(text=res.text)

    scripts = response.xpath('//script/text()').extract()
    script = scripts[9]

    data = scrapy.Selector(text=script).re('\{".+":".+",')[0]
    data_ls = data.split('\",\"')

    productId = data_ls[0].split(":")[1][1:]
    movieName = data_ls[2].split(":")[1][1:]
    genre = data_ls[3].split(":")[1][1:]
    language = data_ls[4].split(":")[1][1:]
    eventGroup = data_ls[6].split(":")[1][1:]
    cast = data_ls[7].split(":")[1][1:]

    turl1 = "https://in.bookmyshow.com/serv/getData.bms?cmd=GETREVIEWSGROUP&eventGroupCode=" + str(eventGroup) + "&type=UR&pageNum="
    turl2 = "&perPage=50&sort=LATEST"

    reviewCount = 0
    date = ""
    time = ""
    rating = 0
    review = ""
    title = ""
    reviewId = ""
    likes = 0
    dislikes = 0
    name = ""
    verified = ""

    data = pd.DataFrame(
        columns=['ReviewID', 'Name', 'Varified', 'Date', 'Time', 'Title', 'Review', 'Likes', 'Dislikes', 'Rating'])
    for i in range(100000, 100001):
        next_url = turl1 + str(i) + turl2
        response = requests.get(next_url)
        response = json.loads(response.text)
        reviewCount = int(response['data']['ReviewCount'])

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'
    }

    for ind in range(10000):
        if (ind + 1) * 9 >= reviewCount:
            next_url = turl1 + str(ind) + turl2
            response = requests.get(next_url)
            response = json.loads(response.text)
            print(response)
            break
        else:
            next_url = turl1 + str(ind) + turl2
            response = requests.get(next_url, headers=headers)
            response = json.loads(response.text)
            #         if ind%20 == 0:
            #             print(ind)
            print(len(response['data']['Reviews']))
            for i in response['data']['Reviews']:
                date = i['Date'].split(" ")[0]
                time = i['Date'].split(" ")[1]
                rating = (float(i['Rating'])) / 10
                review = i['Review']
                title = i['Title']
                reviewId = i['ReviewId']
                likes = int(i['Likes'])
                dislikes = int(i['Dislikes'])
                name = i['Name']
                verified = i['Verified']
                sample_data = {'ReviewID': reviewId,
                               'Name': name,
                               'Varified': verified,
                               'Date': date,
                               'Time': time,
                               'Title': title,
                               'Review': review,
                               'Likes': likes,
                               'Dislikes': dislikes,
                               'Rating': rating}
                data = data.append(sample_data, ignore_index=True)



@app.route('/imdb/<string:mid>')
def imdb(mid):
    url = 'https://www.imdb.com/title/'+mid+'/reviews?ref_=tt_ql_3'
    next_page_url1 = url.split('?')[0]
    next_page_url2 = '_ajax?ref_=undefined&paginationKey='
    next_page_url = next_page_url1 + '/' + next_page_url2
    response = scrapy.Request(url)

    res = requests.get('https://www.imdb.com/title/tt7286456/reviews?ref_=tt_ql_3')
    response = scrapy.Selector(text=res.text)
    load_more_data = response.xpath('//*[@id="main"]/section/div[2]/div[4]').get()

    def get_data_key(load_more_data):
        #     print(load_more_data)
        data_key = scrapy.Selector(text=load_more_data).re(' data-key=".+"')
        #     print(data_key)
        data_key = data_key[0].split("\"")[1]
        #     print(data_key)
        return data_key

    data_key = get_data_key(load_more_data)

    review_date = []
    review_title = []
    review = []
    reviewer_name = []
    rating = []
    likes = []

    for i in range(0, 1000):
        res = requests.get(next_page_url + data_key)
        response = scrapy.Selector(text=res.text)
        containers = response.css("div.review-container")
        for container in containers:
            t_review_date = container.css("span.review-date::text").get()
            t_review_title = container.css("a::text").get()
            temp_cont = container.css("div.display-name-date")
            t_review = container.css('div.text.show-more__control').get().split('>')[1].split('<')[0]
            t_reviewer_name = temp_cont.css("a::text").get()
            tmp_cont = container.css("span.rating-other-user-rating")
            if len(tmp_cont.css("span")) != 0:
                tmp_cont = tmp_cont.css("span")[1]
                t_rating = tmp_cont.css("span::text").get()
            else:
                t_rating = "NaN"
            t_likes = container.css('div.actions.text-muted').get().split('\n')[1].strip().split(" ")[0]
            likes.append(t_likes)
            review.append(t_review)
            review_date.append(t_review_date)
            review_title.append(t_review_title)
            reviewer_name.append(t_reviewer_name)
            rating.append(t_rating)
        load_more_data = response.xpath('/html/body/div/div[2]').get()
        print(i)
        data_key = get_data_key(load_more_data)

        dataset = pd.DataFrame()
        data_list = []
        for i in range(0, len(review)):
            data_list.append([reviewer_name[i], review_title[i], review[i], review_date[i], likes[i], rating[i]])

        dataset = pd.DataFrame(data_list,
                               columns=['Reviewer_Name', 'Review_Title', 'Review', 'Review_Date', 'Likes', 'Rating'])
        dataset.to_csv('Infinity_IMDB.csv')



@app.route('/tweet/<string:tags>')
def tweets(tags):
    hash_tag_list = tags.split()
    fetched_tweets_filename = "tweets.txt"

    twitter_streamer = TwitterStreamer()
    twitter_streamer.stream_tweets(fetched_tweets_filename, hash_tag_list)

class TwitterStreamer():
    """
    Class for streaming and processing live tweets.
    """

    def __init__(self):
        pass

    def stream_tweets(self, fetched_tweets_filename, hash_tag_list):
        # This handles Twitter authetification and the connection to Twitter Streaming API
        listener = StdOutListener(fetched_tweets_filename)
        auth = OAuthHandler(twitter_credentials.CONSUMER_KEY, twitter_credentials.CONSUMER_SECRET)
        auth.set_access_token(twitter_credentials.ACCESS_TOKEN, twitter_credentials.ACCESS_TOKEN_SECRET)
        stream = Stream(auth, listener)

        # This line filter Twitter Streams to capture data by the keywords:
        stream.filter(track=hash_tag_list)


# # # # TWITTER STREAM LISTENER # # # #
class StdOutListener(StreamListener):
    """
    This is a basic listener that just prints received tweets to stdout.
    """

    def __init__(self, fetched_tweets_filename):
        self.fetched_tweets_filename = fetched_tweets_filename

    def on_data(self, data):
        try:
            print(data)
            with open(self.fetched_tweets_filename, 'a') as tf:
                tf.write(data)
            return True
        except BaseException as e:
            print("Error on_data %s" % str(e))
        return True

    def on_error(self, status):
        print(status)


if __name__ == '__main__':
    app.run()
