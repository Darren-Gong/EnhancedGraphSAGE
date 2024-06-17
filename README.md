本项目完成了基于spark的数据处理分析及可视化任务，基于GNN的新闻推荐任务以及认知大模型API接入，并将其部署在网页上。
采用数据集OpenNewsArchive，27GB，880万篇文章。在项目中文件路径采用了分布式文件系统hdfs。
项目结构如下：

此外，在第一次运行本系统时，需要取消 pyspark 代码的注释，即在第一次运行时，要在 app.py 中通过调用运行 pyspark 代码，生成前端构建的 js，json文件。后续运行本系统，不需运行 pyspatk 代码，直接运行 app.py 启动 web 即可。

```markdown
project
├── bert-base-chinese/  # 预训练模型，用于新闻推荐任务
├── static/
│   ├── css/           # 样式表文件
│   ├── data/          # Spark处理数据后保存的内容，格式为json, js
│   ├── font/          # 字体文件
│   ├── images/        # 图片文件
│   └── js/            # JavaScript文件
├── templates/
│   └── index.html     # 前端模板文件
├── app.py             # 启动项目脚本，运行此脚本，打开网页http://127.0.0.1:5000
                       # 在第一次运行时需要恢复被注释的调用pyspark的函数，生成前端所需数据
├── dataprocess.py     # 基本数据预处理和数据统计代码，无需运行
├── forecast.py        # GNN新闻预测代码，通过在app.py中调用预训练权重实现
├── graphsage_model.pth # 模型权重文件
├── mychat.py          # API接入语言大模型，无需运行，由app.py调用
├── node_features.pt   # 权重文件
├── vis.py             # Spark处理数据所得的可视化基础数据
└── word.py            # Spark分词处理数据，统计高频词
```
