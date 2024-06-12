# GNN
本项目完成了基于spark的数据处理分析及可视化任务，基于GNN的新闻推荐任务以及认知大模型API接入，并将其部署在网页上。
采用数据集OpenNewsArchive，27GB，880万篇文章。在项目中文件路径采用了分布式文件系统hdfs。
项目结构如下：
--project
----bert-base-chinese 预训练模型，用于新闻推荐任务
----static
--------css
--------data spark处理数据后保存的内容，为json,js格式
--------font
--------images
--------js
---templates
--------index.html
----app.py 运行此脚本，启动项目，打开网页http://127.0.0.1:5000
----在第一次运行时需要把被注释的调用pyspark的函数恢复，运行以产生前端所需的数据
----dataprocess.py 基本数据预处理，数据统计代码，无需运行。
----forecast.py GNN新闻预测，无需运行，在app.py中调用
----graphsage_model.pth 模型权重
----mychat.py api接入语言大模型，无需运行，app.py中调用
----node_features.pt 权重文件
----vis.py spark处理数据所得可视化基础数据
----word.py spark分词处理数据，统计高频词
