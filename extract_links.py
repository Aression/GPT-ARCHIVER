from bs4 import BeautifulSoup
import csv

input_file = 'D:/CODES/GPT-ARCHIVER/html_data/favorites_2024_5_14.html'
output_file = "output.csv"

# 读取 HTML 文件
with open(input_file, 'r', encoding='utf-8') as file:
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
data = sorted(list(set(data)),key=lambda x:x[2])

# 将提取的数据写入到 CSV 文件
with open(output_file, 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(['Link Text', 'Link Href', "Link Date"])  # 写入表头
    writer.writerows(data)  # 写入数据