import re
import pandas as pd
import json
from collections import Counter
from pathlib import Path

def load_processed_data(file_path):
    data = pd.read_csv(file_path).dropna()
    return data

pattern = r'\[([^]]+)\]-\[([^]]+)\]'
output_dir = Path('D:/CODES/GPT-ARCHIVER/output')  # 使用Path对象处理跨平台路径问题

def get_main_class(text):
    match = re.search(pattern, text)
    return match.group(1) if match else 'None'

def get_sub_class(text):
    match = re.search(pattern, text)
    return match.group(2) if match else 'None'

if __name__ == "__main__":
    data = load_processed_data("D:/CODES/GPT-ARCHIVER/legal_data.csv")
    
    data['main_class'] = data['cls_result'].apply(get_main_class)
    data['sub_class'] = data['cls_result'].apply(get_sub_class)
    
    # 创建输出目录
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # 根据主类输出子表
    for main_class, group in data.groupby('main_class'):
        group.to_csv(output_dir / f"{main_class}.csv", index=False)
    
    # 统计各个子类的数量
    sub_class_counts = Counter(data['sub_class'])
    
    # 输出子类数量统计为 JSON
    with open(output_dir / "sub_class_counts.json", 'w', encoding='utf-8') as f:
        json.dump(sub_class_counts, f, ensure_ascii=False, indent=4)
    
    # 输出处理后的数据
    data.to_csv(output_dir / "processed_data.csv", index=False)
