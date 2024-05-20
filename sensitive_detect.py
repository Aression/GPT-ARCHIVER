import re, os

def detect_sensitive_strings(string_list, sensitive_words):
    # 构建正则表达式模式
    pattern = '|'.join(re.escape(word) for word in sensitive_words)

    # 初始化结果列表
    matched_strings = []
    unmatched_strings = []

    # 遍历字符串列表进行检测
    for string in string_list:
        # print(f'正在处理第{string_list.index(string)+1}条数据，字符串内容为：{string}, 类型为：{type(string)}')
        
        if not isinstance(string, str):
            continue
        
        # 在字符串中查找匹配的敏感字符
        matches = re.findall(pattern, string, flags=re.IGNORECASE)

        # 根据匹配结果将字符串分类
        if matches:
            matched_strings.append(string)
        else:
            unmatched_strings.append(string)

    return unmatched_strings, matched_strings

# 敏感字符列表
sensitive_words_files = ['色情类','涉枪涉爆违法信息关键词','政治类']
sensitive_words_dir= 'D:\CODES\GPT-ARCHIVER\sensitive-words'
def load_strings_from_txt(file_path):
    strings = []
    with open(f'{file_path}.txt', 'r', encoding="utf-8", errors='ignore') as file:
        for line in file:
            # 去除行尾的换行符和空格
            line = line.strip('\n').strip()
            
            # 检查行尾是否以逗号结尾
            if line.endswith(','):
                # 去除逗号并添加到列表中
                strings.append(line[:-1])
            else:
                # 直接添加到列表中
                strings.append(line)

    # 检查类型，如果不是str则删掉
    strings = [text for text in strings if isinstance(text, str)]
    
    return strings

sensitive_words = []

for sensitive_words_file in sensitive_words_files:
    sensitive_words_file_path = os.path.join(sensitive_words_dir, sensitive_words_file)
    sensitive_words_file_strings = load_strings_from_txt(sensitive_words_file_path)
    sensitive_words.extend(sensitive_words_file_strings)

def filter_strings(string_list):
    return detect_sensitive_strings(string_list, sensitive_words)