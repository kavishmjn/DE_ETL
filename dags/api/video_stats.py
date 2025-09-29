import requests
import json

from datetime import date
from airflow.decorators import task 
from airflow.models import Variable 


API_KEY = Variable.get("API_KEY")

Channel_Handle = Variable.get("CHANNEL_HANDLE")

@task
def get_playlist_id():
    try:
        url = f'https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={Channel_Handle}&key={API_KEY}'
        print(Channel_Handle)
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        channel_items = data['items'][0]
        channel_playlist_id = channel_items['contentDetails']['relatedPlaylists']['uploads']
        return channel_playlist_id
    except requests.exceptions.RequestException as e  :
        raise e


@task
def get_video_ids(playlist_id,maxResults=50):
    video_ids=[]
    page_token=None
    base_url = f'https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={maxResults}&playlistId={playlist_id}&key={API_KEY}'
    try:
        while True:
            url = base_url
            if page_token:
                url += f'&pageToken={page_token}'
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            for item in data['items']:
                video_id = item['contentDetails']['videoId']
                video_ids.append(video_id)
            
            page_token = data.get('nextPageToken')

            if not page_token: break
        return video_ids
    except requests.exceptions.RequestException as e  :
        raise e


    
@task
def extract_video_data(video_ids):
   
    extracted_data=[]
    
    def batch_list(video_id_list,batch_size=50):
        for video_id in range(0,len(video_id_list),batch_size):
            yield video_id_list[video_id:video_id+batch_size]
    try:
        for batch in batch_list(video_ids,batch_size=50):
            video_ids = ','.join(batch)
            url =  url = f'https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails,snippet,statistics&id={video_ids}&key={API_KEY}'
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            for item in data.get('items',[]):
                video_id = item['id']
                snippet = item['snippet']
                contentDetails = item['contentDetails']
                statistics = item['statistics']
                video_data = {
                        "video_id": video_id,
                        "title": snippet["title"],
                        "publishedAt": snippet["publishedAt"],
                        "duration": contentDetails["duration"],
                        "viewCount": statistics.get("viewCount", None),
                        "likeCount": statistics.get("likeCount", None),
                        "commentCount": statistics.get("commentCount", None)
                        }
                extracted_data.append(video_data)
        return extracted_data    
    except requests.exceptions.RequestException as e  : 
        raise e

@task
def save_to_json(extracted_data):
    file_path = f"./data/YT_data_{date.today()}.json"
    with open(file_path,'w',encoding='utf-8') as json_file:
        json.dump(extracted_data,json_file,indent=4,ensure_ascii=False)


if __name__ == "__main__":    
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    video_data = extract_video_data(video_ids)
    save_to_json(video_data) 








