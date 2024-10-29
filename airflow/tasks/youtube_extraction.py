from datetime import datetime
import json
import googleapiclient.discovery
import pandas as pd


def extract_youtube_data(**kwargs):
    """
    Task to extract youtube data
    """
    api_key = kwargs["api_key"]
    keyword = kwargs["keyword"]

    df_videos = get_videos_by_search(api_key, keyword)

    date = datetime.now().strftime("%Y%m%d")

    path_csv_file = f"/opt/airflow/data/Youtube_data_{keyword}_{date}"

    df_videos.to_csv(path_csv_file, index=False)


def get_videos_by_search(api_key: str, keyword: str) -> pd.DataFrame:
    """Get video using keyword

    Args:
        api_key (str): Youtube API key
        keyword (str): Keyword used for the request

    Returns:
        pd.DataFrame: data in dataframe format
    """
    videos_data = []

    youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=api_key)

    request = youtube.search().list(
        part="id,snippet",
        type="video",
        q=keyword,
        videoDuration="short",
        videoDefinition="high",
        maxResults=50,
    )

    response = request.execute()

    videos = response["items"]

    # Extract relevant data
    for video in videos:
        video_info = {
            "_id": video["id"]["videoId"],
            "title": video["snippet"]["title"],
            "published_at": video["snippet"]["publishedAt"],
            "channel": video["snippet"]["channelTitle"],
        }

        videos_data.append(video_info)

    return pd.DataFrame(videos_data)
