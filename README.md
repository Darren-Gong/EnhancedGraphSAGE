├── bert-base-chinese/  # 预训练模型，用于新闻推荐任务
├── static/
│   ├── css/        # 样式表文件
│   ├── data/       # Spark处理数据后保存的内容，格式为json, js
│   ├── font/       # 字体文件
│   ├── images/     # 图片文件
│   └── js/         # JavaScript文件
├── templates/
│   └── index.html  # 前端模板文件
├── model/
│   ├── graphsage_model.pth   # 模型权重文件
│   └── node_features.pt   # 节点特征权重文件
├── app.py    # 启动项目脚本，运行此脚本，打开网页http://127.0.0.1:5000 在第一次运行时需要恢复被注释的调用pyspark的函数，生成前端所需数据。
├── dataprocess.py  # 基本数据预处理和数据统计代码，无需运行
├── forecast.py    # GNN新闻预测代码，直接调用预训练权重实现
├── mychat.py         # API接入语言大模型，无需运行，由app.py调用
├── vis.py             # Spark处理数据所得的可视化基础数据
└── word.py            # Spark分词处理数据，统计高频词
