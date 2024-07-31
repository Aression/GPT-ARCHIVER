import pandas as pd
import hydra
import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from zhipuai import ZhipuAI
from sensitive_detect import filter_strings

import logging
# A logger for this file
log = logging.getLogger(__name__)

def get_extracted_csv(file_path):
    df = pd.read_csv(file_path)
    df = df[df['Link Text'] != '']
    return df

def save_results(legal_data, illegal_data, legal_lock, illegal_lock):
    with legal_lock:
        legal_data.to_csv("legal_data.csv", index=False)
    with illegal_lock:
        illegal_data.to_csv("illegal_data.csv", index=False)

@hydra.main(version_base='1.3', config_path="conf", config_name="cfg.yaml")
def main(cfg):
    client = ZhipuAI(api_key=str(cfg.GPT.GLM4_KEY))
    
    data = get_extracted_csv("D:\\CODES\\GPT-ARCHIVER\\output.csv")
    link_texts = list(data["Link Text"])

    # illegal texts are ignored now
    legal_texts, illegal_texts = filter_strings(link_texts)
    log.info(f'敏感词过滤完成，合法网页名称共计{len(legal_texts)}个，不合法网页名称共计{len(illegal_texts)}个。')
    
    legal_data = data[data['Link Text'].isin(legal_texts)]
    illegal_data = data[data['Link Text'].isin(illegal_texts)]

    legal_lock = threading.Lock()
    illegal_lock = threading.Lock()
    
    def get_cls_result(title):
        try:
            response = client.chat.completions.create(
                model="glm-4",
                messages=[
                    {
                        "role": "user", 
                        "content": f"{cfg.cls_prompt}{title}。"
                    },
                ],
                temperature=0.1,
                max_tokens=8192,
            )
            return json.loads(response.json())['choices'][0]['message']['content']
        except Exception as e:
            log.info(f"Error while processing title: {title}. Error message: {str(e)}")
            with illegal_lock:
                illegal_data.loc[len(illegal_data) + 1] = data.loc[data['Link Text'] == title].values[0]
            with legal_lock:
                legal_data.drop(legal_data[legal_data['Link Text'] == title].index, inplace=True)
            return None
    
    def classify_title(title):
        cls_result = get_cls_result(title)
        if cls_result:
            with legal_lock:
                legal_data.loc[legal_data['Link Text'] == title, 'cls_result'] = cls_result

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(classify_title, title) for title in legal_texts]
        for future in as_completed(futures):
            future.result()
            save_results(legal_data, illegal_data, legal_lock, illegal_lock)
    
    save_results(legal_data, illegal_data, legal_lock, illegal_lock)

if __name__ == "__main__":
    main()
