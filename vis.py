from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count as spark_count, when, month, year
import pandas as pd
import matplotlib.pyplot as plt
import json
import math

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

# 统计 JSON 对象数量
count = df.count()
print("Total number of JSON objects:", count)

# 计算 num_words 和 max_word_length 的平均长度
avg_num_words = df.groupBy().avg("num_words").collect()[0][0]
avg_max_word_length = df.groupBy().avg("max_word_length").collect()[0][0]

# 计算区间的间隔并取整
interval_num_words = math.ceil(avg_num_words / 10)
interval_max_word_length = math.ceil(avg_max_word_length / 10)

# 创建新的列来表示 num_words 的区间
df = df.withColumn("num_words_bin",
                   when(col("num_words") <= interval_num_words, f"0-{interval_num_words}")
                   .otherwise(when(col("num_words") <= interval_num_words * 2, f"{interval_num_words + 1}-{interval_num_words * 2}")
                              .otherwise(when(col("num_words") <= interval_num_words * 3, f"{interval_num_words * 2 + 1}-{interval_num_words * 3}")
                                         .otherwise(when(col("num_words") <= interval_num_words * 4, f"{interval_num_words * 3 + 1}-{interval_num_words * 4}")
                                                    .otherwise(when(col("num_words") <= interval_num_words * 5, f"{interval_num_words * 4 + 1}-{interval_num_words * 5}")
                                                               .otherwise(when(col("num_words") <= interval_num_words * 6, f"{interval_num_words * 5 + 1}-{interval_num_words * 6}")
                                                                          .otherwise(when(col("num_words") <= interval_num_words * 7, f"{interval_num_words * 6 + 1}-{interval_num_words * 7}")
                                                                                     .otherwise(when(col("num_words") <= interval_num_words * 8, f"{interval_num_words * 7 + 1}-{interval_num_words * 8}")
                                                                                                .otherwise(when(col("num_words") <= interval_num_words * 9, f"{interval_num_words * 8 + 1}-{interval_num_words * 9}")
                                                                                                           .otherwise(f"{interval_num_words * 9 + 1}-{interval_num_words * 10}"))))))))))

# 创建新的列来表示 max_word_length 的区间
df = df.withColumn("max_word_length_bin",
                   when(col("max_word_length") <= interval_max_word_length, f"0-{interval_max_word_length}")
                   .otherwise(when(col("max_word_length") <= interval_max_word_length * 2, f"{interval_max_word_length }-{interval_max_word_length * 2}")
                              .otherwise(when(col("max_word_length") <= interval_max_word_length * 3, f"{interval_max_word_length * 2 }-{interval_max_word_length * 3}")
                                         .otherwise(when(col("max_word_length") <= interval_max_word_length * 4, f"{interval_max_word_length * 3 }-{interval_max_word_length * 4}")
                                                    .otherwise(when(col("max_word_length") <= interval_max_word_length * 5, f"{interval_max_word_length * 4 }-{interval_max_word_length * 5}")
                                                               .otherwise(when(col("max_word_length") <= interval_max_word_length * 6, f"{interval_max_word_length * 5 }-{interval_max_word_length * 6}")
                                                                          .otherwise(when(col("max_word_length") <= interval_max_word_length * 7, f"{interval_max_word_length * 6 }-{interval_max_word_length * 7}")
                                                                                     .otherwise(when(col("max_word_length") <= interval_max_word_length * 8, f"{interval_max_word_length * 7 }-{interval_max_word_length * 8}")
                                                                                                .otherwise(when(col("max_word_length") <= interval_max_word_length * 9, f"{interval_max_word_length * 8 }-{interval_max_word_length * 9}")
                                                                                                           .otherwise(f"{interval_max_word_length * 9 }-{interval_max_word_length * 10}"))))))))))

# 统计每个区间的文章数量 - num_words
num_words_distribution = df.groupBy("num_words_bin").agg(spark_count("*").alias("count"))

# 统计每个区间的文章数量 - max_word_length
max_word_length_distribution = df.groupBy("max_word_length_bin").agg(spark_count("*").alias("count"))

# 显示结果 - num_words
print("Distribution of num_words:")
num_words_distribution.show()

# 显示结果 - max_word_length
print("Distribution of max_word_length:")
max_word_length_distribution.show()
# 定义阈值
short_threshold = 1000
medium_threshold = 2000

# 创建新的列来表示文章长度类别
df = df.withColumn("length_category",
                   when(col("num_words") <= short_threshold, "Short")
                   .when((col("num_words") > short_threshold) & (col("num_words") <= medium_threshold), "Medium")
                   .otherwise("Long"))

# 统计每个类别的文章数量
length_category_distribution = df.groupBy("length_category").agg(spark_count("*").alias("count"))

# 显示结果
print("Distribution of article length categories:")
length_category_distribution.show()

# 提取日期列中的年份信息，并为其指定不同的名称
df = df.withColumn("entry_year", year(col("date")))

# 创建一个包含2015年到2024年的DataFrame
year_range_df = spark.range(2015, 2025, step=1).select(col("id").alias("year"))

# 将年份范围DataFrame与原始数据进行左连接
joined_df = year_range_df.join(df, year_range_df.year == df.entry_year, "left_outer")

# 按年份进行分组并计数
yearly_count = joined_df.groupBy("year").agg(spark_count("*").alias("count")).orderBy("year")

# 显示结果
print("Number of entries for each year (2015-2024):")
yearly_count.show()
# 提取日期列中的月份信息
df = df.withColumn("month", month(col("date")))

# 按月份进行分组并计数
monthly_count = df.groupBy("month").agg(spark_count("*").alias("count")).orderBy("month")

# 显示结果
print("Number of entries for each month (January to December):")
monthly_count.show()
# 计算 frac_chars_non_alphanumeric 的平均值
avg_frac_chars_non_alphanumeric = df.groupBy().avg("frac_chars_non_alphanumeric").collect()[0][0]

# 计算区间的间隔并保留三位小数
interval_frac_chars_non_alphanumeric = round(avg_frac_chars_non_alphanumeric / 5, 3)

# 创建新的列来表示 frac_chars_non_alphanumeric 的区间
df = df.withColumn("frac_chars_non_alphanumeric_bin",
                   when(col("frac_chars_non_alphanumeric") <= interval_frac_chars_non_alphanumeric, f"0-{interval_frac_chars_non_alphanumeric:.3f}")
                   .otherwise(when(col("frac_chars_non_alphanumeric") <= interval_frac_chars_non_alphanumeric * 2, f"{interval_frac_chars_non_alphanumeric + 0.001:.3f}-{interval_frac_chars_non_alphanumeric * 2:.3f}")
                              .otherwise(when(col("frac_chars_non_alphanumeric") <= interval_frac_chars_non_alphanumeric * 3, f"{interval_frac_chars_non_alphanumeric * 2 + 0.001:.3f}-{interval_frac_chars_non_alphanumeric * 3:.3f}")
                                         .otherwise(when(col("frac_chars_non_alphanumeric") <= interval_frac_chars_non_alphanumeric * 4, f"{interval_frac_chars_non_alphanumeric * 3 + 0.001:.3f}-{interval_frac_chars_non_alphanumeric * 4:.3f}")
                                                    .otherwise(f"{interval_frac_chars_non_alphanumeric * 4 + 0.001:.3f}-{interval_frac_chars_non_alphanumeric * 5:.3f}")))))

# 统计每个区间的数量
frac_chars_non_alphanumeric_distribution = df.groupBy("frac_chars_non_alphanumeric_bin").agg(spark_count("*").alias("count")).orderBy("frac_chars_non_alphanumeric_bin")

# 显示结果
print("Distribution of frac_chars_non_alphanumeric:")
frac_chars_non_alphanumeric_distribution.show()


# 将结果保存到 JSON 文件
result_dict = {
    "chart1": {
        "labels": [],
        "data": []
    },
    "chart2": {
        "labels": [],
        "data": []
    },
    "chart3": {
        "labels": [],
        "data": []
    },
    "chart4": {
        "labels": [],
        "data": []
    },
    "chart5": {
        "labels": [],
        "data": []
    },
    "chart6": {
        "labels": [],
        "data": []
    }
}
for row in num_words_distribution.collect():
    result_dict["chart1"]["labels"].append(row["num_words_bin"])
    result_dict["chart1"]["data"].append(row["count"])
for row in max_word_length_distribution.collect():
    result_dict["chart2"]["labels"].append(row["max_word_length_bin"])
    result_dict["chart2"]["data"].append(row["count"])
for row in length_category_distribution.collect():
    result_dict["chart3"]["labels"].append(row["length_category"])
    result_dict["chart3"]["data"].append(row["count"])
for row in yearly_count.collect():
    result_dict["chart4"]["labels"].append(row["year"])
    result_dict["chart4"]["data"].append(row["count"])
for row in monthly_count.collect():
    result_dict["chart5"]["labels"].append(row["month"])
    result_dict["chart5"]["data"].append(row["count"])
for row in frac_chars_non_alphanumeric_distribution.collect():
    result_dict["chart6"]["labels"].append(row["frac_chars_non_alphanumeric_bin"])
    result_dict["chart6"]["data"].append(row["count"])

output_file = "static/data/distribution_data.json"
with open(output_file, "w") as json_file:
    json.dump(result_dict, json_file, indent=2)

print(f"Data saved to {output_file}")

# 停止 SparkSession
spark.stop()
