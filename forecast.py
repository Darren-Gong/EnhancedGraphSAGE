import json
import os

import dgl
import pandas as pd
import torch
import torch.nn as nn
import torch.nn.functional as F
from dgl.nn.pytorch import SAGEConv, GATConv
from transformers import AutoTokenizer, AutoModel


# 定义图神经网络模型
class EnhancedGraphSAGE(nn.Module):
    def __init__(self, in_feats, h_feats, num_classes):
        super(EnhancedGraphSAGE, self).__init__()
        self.fc_embed = nn.Linear(in_feats, h_feats)
        self.conv1 = SAGEConv(h_feats, h_feats, 'mean')
        self.conv2 = SAGEConv(h_feats, h_feats, 'mean')
        self.attn1 = GATConv(h_feats, h_feats, num_heads=4, feat_drop=0.6, attn_drop=0.6)
        self.fc1 = nn.Linear(h_feats * 4, h_feats)
        self.fc2 = nn.Linear(h_feats, num_classes)
        self.dropout = nn.Dropout(p=0.5)

    def forward(self, g, in_feat):
        h = self.fc_embed(in_feat)  # 使用全连接层对特征进行嵌入
        h = self.conv1(g, h)
        h = F.relu(h)
        h = self.conv2(g, h)
        h = F.relu(h)
        h = self.attn1(g, h)
        h = h.view(h.shape[0], -1)  # 合并注意力头
        h = self.fc1(h)
        h = F.relu(h)
        h = self.dropout(h)
        h = self.fc2(h)
        return h


def get_bert_embedding(text):
    # 使用 BERT 生成新样本的节点特征
    model_dir = './bert-base-chinese'
    tokenizer = AutoTokenizer.from_pretrained(model_dir)
    model_bert = AutoModel.from_pretrained(model_dir)
    inputs = tokenizer(text, return_tensors='pt', truncation=True, padding='max_length', max_length=512)
    outputs = model_bert(**inputs)
    return outputs.last_hidden_state.mean(dim=1).squeeze().detach()


def forecast(new_sample_text):
    # 加载保存的模型权重
    node_feature_dim = 768  # BERT 输出的特征维度为 768
    hidden_dim = 128  # 隐藏层特征数量
    num_classes = 128  # 类别数量，根据具体任务调整
    model = EnhancedGraphSAGE(node_feature_dim, hidden_dim, num_classes)
    model.load_state_dict(torch.load('./model/graphsage_model.pth'))

    # 加载保存的节点特征
    node_features = torch.load('./model/node_features.pt')

    # 创建图对象
    num_nodes = node_features.shape[0]
    g = dgl.graph(([], []), num_nodes=num_nodes)
    g.ndata['feat'] = node_features

    # new_sample_text = "高管“知法”状况将被列入IPO的审核指标当中。中国证监会主席助理吴利军11日在2012年第1期上市公司控股股东、董事长研修班上表示，目前证监会正在研究拟将高管“知法”状况作为IPO" \
    #                   "审核硬指标的条例。\n**高管“知法”将成硬指标**\n高管知法犯法的情况在资本市场屡见不鲜，在不久的将来，IPO审核过程中，高管是否知法将会被列入IPO" \
    #                   "的审核指标。证监会主席助理吴利军11日表示，目前证监会正在研究，除了财务指标外，准备把拟上市公司高管“知法”情况作为上市审核的硬指标，推动上市公司高管知法、懂法和守法。他认为，不触犯法律这是企业的生命线。"
    new_sample_feature = get_bert_embedding(new_sample_text).unsqueeze(0)

    # 添加新样本的节点特征到现有节点特征中
    all_features = torch.cat([node_features, new_sample_feature], dim=0)
    g.add_nodes(1)
    g.ndata['feat'] = all_features

    # 重新计算图中的边
    new_edges = []
    for i in range(all_features.shape[0] - 1):
        cos_sim = F.cosine_similarity(new_sample_feature, all_features[i].unsqueeze(0))
        if cos_sim > 0.001:
            new_edges.append((i, all_features.shape[0] - 1))
            new_edges.append((all_features.shape[0] - 1, i))

    if new_edges:
        new_src, new_dst = zip(*new_edges)
        g.add_edges(new_src, new_dst)

    # 执行前向传播进行预测
    with torch.no_grad():
        model.eval()
        output = model(g, g.ndata['feat'])

    # 输出新样本的预测结果
    new_sample_output = output[-1]

    # 计算新样本与所有节点的相似度
    cos_similarities = F.cosine_similarity(all_features[:-1], new_sample_feature.repeat(all_features.shape[0] - 1, 1))
    top_indices = torch.argsort(cos_similarities, descending=True)[:5]

    # 输出推荐的五篇文章的内容和标题
    data_folder = "Data"  # 数据文件夹路径
    all_data = []
    for filename in os.listdir(data_folder):
        if filename.endswith(".jsonl"):
            with open(os.path.join(data_folder, filename), 'r', encoding='utf-8') as file:
                for line in file:
                    all_data.append(json.loads(line))

    # 转换为DataFrame
    df = pd.DataFrame(all_data)

    # 只保留需要的列
    df = df[['id', 'content', 'title']]

    # 准备 JSON 输出
    recommendations = []
    for idx in top_indices:
        idx_int = idx.item()  # 将张量索引转换为整数
        recommendations.append({
            "title": df.loc[idx_int]['title'],
            "content": df.loc[idx_int]['content']
        })

    # 打印 JSON 格式的推荐结果
    return json.dumps(recommendations, ensure_ascii=False, indent=4)
