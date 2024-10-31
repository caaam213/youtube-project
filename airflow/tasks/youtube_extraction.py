from datetime import datetime
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

    path_csv_file = f"/opt/airflow/data/extracted_data/Youtube_data_{keyword}_{date}"

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

    number_of_queries = 10
    next_page = None
    # TODO : Manage errors like 403
    while True:
        request_search = youtube.search().list(
            part="id,snippet",
            type="video",
            q=keyword,
            videoDuration="short",
            videoDefinition="high",
            maxResults=50,
            pageToken=next_page,
        )

        response = request_search.execute()
        next_page = response.get("nextPageToken")

        # Get all ids and separate them with a comma
        ids = [video["id"]["videoId"] for video in response["items"]]
        ids_str = ",".join(ids)

        # Make request to get additional infos
        request_videos = youtube.videos().list(
            part="id, snippet, statistics, topicDetails", id=ids_str, maxResults=50
        )

        response = request_videos.execute()
        videos = response.get("items", [])

        url_video = "https://www.youtube.com/watch?v={}"

        # Extract relevant data
        for video in videos:
            video_info = {
                "url_video": url_video.format(video["id"]),
                "title": video["snippet"]["title"],
                "published_at": video["snippet"]["publishedAt"],
                "channel": video["snippet"]["channelTitle"],
                "view_count": int(video["statistics"].get("viewCount", 0)),
                "like_count": int(video["statistics"].get("likeCount", 0)),
                "comment_count": int(video["statistics"].get("commentCount", 0)),
                "tags": video["snippet"].get("tags", []),
                "language": video["snippet"].get("defaultAudioLanguage", ""),
            }

            videos_data.append(video_info)

        # Verify if we stop or continue the requests
        number_of_queries -= 1

        if not next_page or number_of_queries == 0:
            break

    return pd.DataFrame(videos_data)
