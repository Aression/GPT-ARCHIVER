"""
收藏夹预处理模块
1. 读取导出的html文件，提取网页标题、链接、收藏时间
2. 根据标题是否含有敏感词进行筛分，返回正常、色情类、政治类、涉暴类四类收藏夹网址对应的dataframe
"""

import csv
import re, os
from bs4 import BeautifulSoup
import pandas as pd

def load_sensitive_words(file_path):
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        sensitive_words = f.read().splitlines()
    return sensitive_words

class Preprocess:
    def __init__(self, file_path):
        self.file_path = file_path
        
        # sensitive words
        self.porn_words = load_sensitive_words(
            f'sensitive-words/色情类.txt')
        self.political_words = load_sensitive_words(
            f'sensitive-words/政治类.txt')
        self.violent_words = load_sensitive_words(
            f'sensitive-words/涉枪涉爆违法信息关键词.txt')
        
        # DFs
        self.normal_df = None
        self.porn_df = None
        self.political_df = None
        self.violent_df = None
        
    def extract_info(self):
        # 读取 HTML 文件
        with open(self.file_path, 'r', encoding='utf-8') as file:
            html_content = file.read()

        # 创建 Beautiful Soup 对象
        soup = BeautifulSoup(html_content, 'html.parser')

        # 查找所有 <DT><A> 标签
        dt_a_tags = soup.find_all('dt')  # 查找所有 <DT> 标签
        data = []
        for tag in dt_a_tags:
            if tag.a:  # 如果存在 <A> 标签
                link_text = tag.a.get_text()  # 提取链接文本
                link_href = tag.a.get('href')  # 提取链接地址
                link_date = tag.a.get('add_date')  # 提取链接日期
                data.append((link_text, link_href, link_date))

        # 去重
        data = sorted(list(set(data)), key=lambda x:x[2])
        
        # 筛选并保存为四类dataframe
        normal = []
        porn = []
        political = []
        violent = []

        def log_and_append(category, words, title, items):
            for word in words:
                if word in title:
                    print(f"找到 {category} 词汇 '{word}' 在标题 '{title}'")
                    items.append(item)
                    return True
            return False

        for item in data:
            title = item[0].lower()
            if log_and_append('色情', self.porn_words, title, porn):
                continue
            if log_and_append('政治', self.political_words, title, political):
                continue
            if log_and_append('暴力', self.violent_words, title, violent):
                continue
            # print(f"未找到特定词汇，归为正常类别: {title}")
            normal.append(item)


        columns = ['Title', 'URL', 'Date']
        self.normal_df = pd.DataFrame(normal, columns=columns)
        self.porn_df = pd.DataFrame(porn, columns=columns)
        self.political_df = pd.DataFrame(political, columns=columns)
        self.violent_df = pd.DataFrame(violent, columns=columns)

    def get_dataframes(self):
        return {
            'normal': self.normal_df,
            'porn': self.porn_df,
            'political': self.political_df,
            'violent': self.violent_df
        }

# 使用示例
if __name__ == "__main__":
    preprocessor = Preprocess("D:\\CODES\\GPT-ARCHIVER\\html_data\\favorites_2024_5_14.html")
    preprocessor.extract_info()
    dataframes = preprocessor.get_dataframes()
    
    for category, df in dataframes.items():
        print(f"{category.capitalize()} bookmarks count: {len(df)}")
        if not df.empty:
            print(df.head())
        print("\n")