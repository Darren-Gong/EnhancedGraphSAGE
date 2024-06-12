from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, max as spark_max, mean as spark_mean, length, unix_timestamp, year, min as spark_min
from pyspark.sql.types import StringType
import math
import json
import re
import unicodedata

# 创建 SparkSession 并配置内存
try:
    spark = SparkSession.builder \
        .appName("HDFS JSONL Data Processing") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
        .config("spark.executor.memory", "8g") \
        .config("spark.driver.memory", "8g") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()
except Exception as e:
    print(f"Error creating SparkSession: {e}")
    raise

# 读取 JSONL 文件
try:
    df = spark.read.json("hdfs://localhost:9000/mydir/zh/")
except Exception as e:
    print(f"Error reading JSON files: {e}")
    spark.stop()
    raise


# 自定义函数删除特殊字符和 emoji
def clean_text(text):
    # 删除 emoji
    emoji_pattern = re.compile(
        "["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
        u"\U00002702-\U000027B0"
        u"\U000024C2-\U0001F251"
        "]+", flags=re.UNICODE)
    text = emoji_pattern.sub(r'', text)

    # 删除非字母数字和非汉字字符
    text = re.sub(r'[^\w\s\u4e00-\u9fa5]', '', text)

    return text


clean_text_udf = udf(clean_text, StringType())

# 删除特殊字符和 emoji
df = df.withColumn("content_clean", clean_text_udf(col("content")))


# 排除非汉语和英语以外的语言内容
def is_chinese_or_english(text):
    for char in text:
        if not ('\u4e00' <= char <= '\u9fff' or 'a' <= char <= 'z' or 'A' <= char <= 'Z'):
            return False
    return True


is_chinese_or_english_udf = udf(is_chinese_or_english, StringType())

df = df.filter(is_chinese_or_english_udf(col("content_clean")) == 'True')


# 去除重复的段落
def remove_duplicate_paragraphs(text):
    paragraphs = text.split("\n")
    unique_paragraphs = list(dict.fromkeys(paragraphs))
    return "\n".join(unique_paragraphs)


remove_duplicate_paragraphs_udf = udf(remove_duplicate_paragraphs, StringType())

df = df.withColumn("content_unique", remove_duplicate_paragraphs_udf(col("content_clean")))


# 添加年份列
df = df.withColumn("year", year(col("date")))

# 统计 JSON 对象数量
count = df.count()
output_text = f"Total number of JSON objects: {count}\n"

# 计算 num_words 最大值和平均值
max_num_words = df.agg(spark_max(col("num_words"))).collect()[0][0]
avg_num_words = df.agg(spark_mean(col("num_words"))).collect()[0][0]
output_text += f"Maximum number of words: {max_num_words}\n"
output_text += f"Average number of words: {avg_num_words}\n"

# 计算 max_word_length 最大值
max_word_length = df.agg(spark_max(col("max_word_length"))).collect()[0][0]
output_text += f"Maximum word length: {max_word_length}\n"

# 添加年份列
df = df.withColumn("year", year(col("date")))

# 计算年份跨度
max_year = df.agg(spark_max(col("year"))).collect()[0][0]
min_year = df.agg(spark_min(col("year"))).collect()[0][0]
year_difference = max_year - min_year
output_text += f"year: {year_difference}\n"
# 保存输出到文件
output_file = "json_processing_output1.txt"
with open(output_file, "w") as f:
    f.write(output_text)

print(f"Output saved to {output_file}")

# 停止 SparkSession
spark.stop()
