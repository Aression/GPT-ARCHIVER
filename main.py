import pandas as pd
import hydra, json
from zhipuai import ZhipuAI
from sensitive_detect import filter_strings

def get_extracted_csv(file_path):
    df = pd.read_csv(file_path)
    df = df[df['Link Text'] != '']
    return df

@hydra.main(version_base='1.3', config_path="conf", config_name="cfg.yaml")
def main(cfg):
    client = ZhipuAI(api_key=str(cfg.GPT.GLM4_KEY))
    
    data = get_extracted_csv("D:\CODES\GPT-ARCHIVER\output.csv")
    link_texts = list(data["Link Text"])

    # illegal texts are ignored now
    legal_texts, illegal_texts = filter_strings(link_texts)
    print(f'敏感词过滤完成，合法网页名称共计{len(legal_texts)}个，不合法网页名称共计{len(illegal_texts)}个。')
    
    legal_data = data[data['Link Text'].isin(legal_texts)]
    illegal_data = data[data['Link Text'].isin(illegal_texts)]
    
    def get_cls_result(title):
        try:
            response = client.chat.completions.create(
                model="glm-4",
                messages=[
                    {
                        "role": "user", 
                        "content": f"{cfg.cls_prompt}。这是网页标题：{title}。"
                    },
                ],
                temperature=0.1,
                max_tokens=8192,
            )
            return json.loads(response.json())['choices'][0]['message']['content']
        except Exception as e:
            print(f"Error while processing title: {title}. Error message: {str(e)}")
            # 将非法标题转移到illegal_data中，同时删掉legal_data中对应的行
            illegal_data.loc[len(illegal_data) + 1] = data.loc[data['Link Text'] == title].values[0]
            legal_data.drop(legal_data[legal_data['Link Text'] == title].index, inplace=True)
    
    legal_data['cls_result'] = legal_data['Link Text'].apply(get_cls_result)
    
    pd.DataFrame.to_csv(legal_data, "legal_data.csv", index=False)
    pd.DataFrame.to_csv(illegal_data, "illegal_data.csv", index=False)


if __name__ == "__main__":
    main()