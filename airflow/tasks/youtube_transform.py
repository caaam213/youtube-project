from datetime import datetime
from pyspark.sql import SparkSession
import emoji
from pyspark.sql.functions import col, udf, to_date
from pyspark.sql.types import StringType, LongType


def process_data(keyword: str):
    """Processes YouTube data stored in a CSV file, cleans certain fields, and saves the cleaned data.

    This function:
    - Reads YouTube data from a CSV file with a specified keyword and date.
    - Cleans specific columns like `title` and `channel` by removing emojis.
    - Casts numerical columns (`view_count`, `like_count`, `comment_count`) to integers.
    - Extracts only the language code from the `language` field.
    - Saves the cleaned DataFrame as a new CSV file in the cleaned data folder.

    Args:
        keyword (str): Keyword used to identify the YouTube data file.

    Returns:
        None
    """
    spark = SparkSession.builder.appName("YoutubeProcess").getOrCreate()
    date = datetime.now().strftime("%Y%m%d")
    path_csv_file = f"/opt/airflow/data/extracted_data/Youtube_data_{keyword}_{date}"
    df = spark.read.csv(path_csv_file, header=True)

    clean_text_udf = udf(clean_text, StringType())

    cleaned_df = (
        df.withColumn("title", clean_text_udf(col("title")))
        .withColumn("published_at", to_date(col("published_at")))
        .withColumn("channel", clean_text_udf(col("channel")))
        .withColumn("view_count", col("view_count").cast(LongType()))
        .withColumn("like_count", col("like_count").cast(LongType()))
        .withColumn("comment_count", col("comment_count").cast(LongType()))
        .withColumn("language", col("language").substr(1, 2))
    )

    path_csv_file = (
        f"/opt/airflow/data/cleaned_data/Youtube_data_{keyword}_{date}_cleaned"
    )

    cleaned_df.write.csv(path_csv_file, header=True)


def clean_text(text):
    """Cleans the text by removing emojis and hashtags.

    Args:
        text (str): Text to be cleaned.

    Returns:
        str: Cleaned text without emojis or hashtags.
    """
    # Remove emojis
    text = emoji.demojize(text, delimiters=("", ""))

    # Remove hashtags
    split_text = text.split("#")
    text = split_text[0].strip()

    return text
