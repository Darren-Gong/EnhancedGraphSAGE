from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, length ,lower,expr,col, regexp_replace, udf
from pyspark.sql.types import StringType
import jieba
import json

# 创建 SparkSession
spark = SparkSession.builder \
    .appName("Chinese Title Word Frequency") \
    .getOrCreate()

# 读取 JSONL 文件
df = spark.read.json("hdfs://localhost:9000/mydir/zh/")

# 提取标题列并转换为小写
titles_lower = df.select(lower(col("title")).alias("title_lower"))

# 定义普通 UDF 函数对中文标题进行分词
def jieba_cut(text):
    return " ".join(jieba.cut(text))

# 注册 UDF
spark.udf.register("jieba_cut_udf", jieba_cut, StringType())

# 对标题进行分词
words = titles_lower.select(explode(expr("split(jieba_cut_udf(title_lower), ' ')")).alias("word"))

# 过滤掉空单词和单个字的词
words_filtered = words.filter((col("word") != "") & (length(col("word")) > 1))

# 过滤掉标点符号和数字
words_filtered = words_filtered.filter(~col("word").rlike("[^\u4e00-\u9fa5a-zA-Z]"))
# 计算每个单词的出现频率
word_counts = words_filtered.groupBy("word").count()

# 按照单词的出现频率降序排列
word_counts_sorted = word_counts.orderBy("count", ascending=False)

# 提取前一百个高频词
top_100_words = word_counts_sorted.limit(100)

# 将结果转换为字典
result_dict = top_100_words.collect()
result_dict = [{"word": row["word"], "count": row["count"]} for row in result_dict]
# 指定输出文件路径
output_file = "static/data/top_100_chinese_words.js"
with open(output_file, "w", encoding="utf-8") as f:
    # 写入 JavaScript 变量声明
    f.write("var jsonData = ")

    # 将数据转换为 JSON 格式并写入文件
    json.dump(result_dict, f, ensure_ascii=False, indent=4)


print(f"Top 100 Chinese words saved to {output_file}")

# 停止 SparkSession
spark.stop()
