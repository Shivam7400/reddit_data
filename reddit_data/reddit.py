import requests
import html
import hashlib
import logging
import datetime
import sqlite3
from pymongo import MongoClient
from settings import LOGGING_LEVEL, MY_CLIENT_ID, MY_USER_AGENT, MY_REDDIT_USERNAME, MY_REDDIT_PASSWORD, MY_CLIENT_SECRET, DB_SETTINGS

TOKEN_ACCESS_ENDPOINT = 'https://www.reddit.com/api/v1/access_token'
post_data = {'grant_type': 'password', 'username': MY_REDDIT_USERNAME, 'password': MY_REDDIT_PASSWORD}
headers = {'User-Agent': MY_USER_AGENT}
params_get = {'limit': 100,'after': 'after_key'}

#logging
logging_level_mapping = {'DEBUG':logging.DEBUG,'INFO':logging.INFO,'WARNING':logging.WARNING,'ERROR':logging.ERROR,'CRITICAL':logging.CRITICAL}
log_level = logging_level_mapping.get(LOGGING_LEVEL,'ERROR')
logging.basicConfig(filename='reddit.log', level=logging.ERROR, format='Time: %(asctime)s | File: %(filename)s | Line: %(lineno)d | Level: %(levelname)s | Message: %(message)s')
reddit_logger = logging.getLogger('reddit_data')

def reddit_data(reddit_key, subreddit_name):
    client = MongoClient(DB_SETTINGS['mongo_uri'])
    db = client[DB_SETTINGS['mongo_db']]
    collection = db["elasticfeeds"]

    client_auth = requests.auth.HTTPBasicAuth(MY_CLIENT_ID, MY_CLIENT_SECRET)
    response = requests.post(TOKEN_ACCESS_ENDPOINT, data=post_data, headers=headers, auth=client_auth)

    try:
        if response.status_code == 200:
            token_id = response.json().get('access_token')
            if not token_id:
                raise ValueError("Access token not found in the response.")
            
            OAUTH_ENDPOINT = 'https://oauth.reddit.com'
            headers_get = {'User-Agent': MY_USER_AGENT,'Authorization': 'Bearer ' + token_id} 
            listings = ['hot', 'new', 'top']
            
            for listing in listings:
                response = requests.get(OAUTH_ENDPOINT + '/r/'+reddit_key+'/'+listing+'/', headers=headers_get, params=params_get)
                response.raise_for_status()  # Raise an error for non-200 status codes
                
                data_response = response.json()
                items = data_response['data']['children']
                
                for i in items:
                    item = {}
                    try:
                        item['source_id'] = "reddit"
                        item['title'] = ' '.join(i['data']['title'].split())
                        item['link'] = "https://www.reddit.com" + i['data']['permalink']
                        item['link_hash'] = hashlib.md5((item['link']).encode()).hexdigest()
                        item['keywords'] = [' '.join(i['data']['subreddit'].split())]
                        item['creator'] = [' '.join(i['data']['author'].split())]
                        item['content'] = None
                        item['description'] = None
                        item['language'] = None
                        item['country'] = None 
                        item['category'] = ' '.join(i['data']['category'].split()) if 'category' in i['data'] and i['data']['category'] is not None else 'top'
                        item['coll_list'] = subreddit_name   
                        
                        image_url = i['data']['thumbnail']
                        if image_url and image_url.startswith('http'):
                            item['image_url'] = image_url
                            item['has_image'] = True
                        else:
                            item['image_url'] = None
                            item['has_image'] = False

                        video_url = i['data']['secure_media_embed'].get('media_domain_url') if 'secure_media_embed' in i['data'] else None
                        if video_url:
                            item['video_url'] = video_url
                            item['has_video'] = True
                        else:
                            item['video_url'] = None
                            item['has_video'] = False

                        pub_date_utc = datetime.datetime.fromtimestamp(i['data']['created_utc'], tz=datetime.timezone.utc)
                        item['pubDate'] = pub_date_utc.strftime("%Y-%m-%d %H:%M:%S")

                        current_time_utc = datetime.datetime.now(datetime.timezone.utc)
                        item['created_at'] = current_time_utc.replace(microsecond=0).strftime('%Y-%m-%d %H:%M:%S')
                        item['updated_at'] = current_time_utc.replace(microsecond=0).strftime('%Y-%m-%d %H:%M:%S')
                        
                        full_description = html.unescape(i['data']['selftext']) if 'selftext' in i['data'] else None
                        if full_description and len(full_description) != 0:
                            item['full_description'] = ' '.join(full_description.split())
                            item['full_description_status'] = 1
                        else:
                            item['full_description_status'] = 0
                        
                        item['stat'] = {
                            'post_id': ' '.join(i['data']['id'].split()) if 'id' in i['data'] else None,
                            'subreddit_Id': i['data']['subreddit_id'] if 'subreddit_id' in i['data'] else None,
                            'likeCount': i['data']['ups'] if 'ups' in i['data'] else None,
                            'downs': i['data']['downs'] if 'downs' in i['data'] else None,
                            'commentCount': i['data']['num_comments'] if 'num_comments' in i['data'] else None
                        } if 'id' in i['data'] and 'subreddit_id' in i['data'] and 'ups' in i['data'] and 'downs' in i['data'] and 'num_comments' in i['data'] else None
                        
                        item['subreddit_stat'] = {
                            'subscriberCount': i['data']['subreddit_subscribers'] if 'subreddit_subscribers' in i['data'] else None,
                            'followerGain': i['data']['no_follow'] if 'no_follow' in i['data'] else None,
                            'viewCount': i['data']['view_count'] if 'view_count' in i['data'] else None
                        } if 'subreddit_subscribers' in i['data'] and 'no_follow' in i['data'] and 'view_count' in i['data'] else None
                        
                        if item['title'] is not None and item['link'] is not None and item['pubDate'] is not None:
                            collection.create_index([('link_hash', 1)], unique=True)  
                            collection.insert_one(item)
                    except Exception as e:
                        reddit_logger.error(f"Error processing item: {str(e)}")
    except requests.HTTPError as e:
        reddit_logger.error(f"HTTP error occurred: {str(e)}")
    except Exception as e:
        reddit_logger.error(f"An unexpected error occurred: {str(e)}")

def reddit_start():
    try:
        sqlite_conn = sqlite3.connect('reddit.db')
        sql_cursor = sqlite_conn.cursor()

        sql_cursor.execute("SELECT keywords, name FROM Connection WHERE is_active=1 AND is_reddit=1")
        keyword1 = sql_cursor.fetchall()
        
        for key in keyword1:
            try:
                reddit_key = str(key[0])
                subreddit_name = str(key[1])
                reddit_data(reddit_key, subreddit_name)
            except Exception as e:
                reddit_logger.error(f"Error processing subreddit: {str(e)}")
    except sqlite3.Error as e:
        reddit_logger.error(f"SQLite error occurred: {str(e)}")
    except Exception as e:
        reddit_logger.error(f"An unexpected error occurred: {str(e)}")

reddit_start()