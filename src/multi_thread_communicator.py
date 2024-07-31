import pandas as pd
from zhipuai import ZhipuAI
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock, Thread
from queue import Queue
import time

class MultiThreadCommunicator:
    def __init__(self, data: pd.DataFrame, client: ZhipuAI, output_path: str, max_workers: int = 5) -> None:
        self.data = data
        self.client = client
        self.output_path = output_path
        self.max_workers = max_workers

        # CSV file locks
        self.legal_lock = Lock()
        self.illegal_lock = Lock()

        # Queues for results
        self.legal_queue = Queue()
        self.illegal_queue = Queue()

        # Initialize dataframes for results
        self.legal_data = pd.DataFrame(columns=['Title', 'URL', 'Date', 'Category'])
        self.illegal_data = pd.DataFrame(columns=['Title', 'URL', 'Date', 'Category'])

    def process_item(self, item):
        title, url, date, category = item

        prompt = f"Please analyze this webpage title and determine if it's legal or illegal content: '{title}'. Respond with only 'legal' or 'illegal'."

        try:
            response = self.client.chat.completions.create(
                model="glm-4",
                messages=[{"role": "user", "content": prompt}]
            )

            result = response.choices[0].message.content.strip().lower()

            if result == 'legal':
                self.legal_queue.put((title, url, date, category))
            elif result == 'illegal':
                self.illegal_queue.put((title, url, date, category))

        except Exception as e:
            print(f"Error processing {title}: {str(e)}")

    def save_results(self):
        while True:
            try:
                item = self.legal_queue.get(timeout=1)
                with self.legal_lock:
                    self.legal_data = self.legal_data.append(pd.Series(item, index=self.legal_data.columns), ignore_index=True)
                self.legal_queue.task_done()
            except Queue.Empty:
                pass

            try:
                item = self.illegal_queue.get(timeout=1)
                with self.illegal_lock:
                    self.illegal_data = self.illegal_data.append(pd.Series(item, index=self.illegal_data.columns), ignore_index=True)
                self.illegal_queue.task_done()
            except Queue.Empty:
                pass

            if self.legal_queue.empty() and self.illegal_queue.empty():
                break

        with self.legal_lock:
            self.legal_data.to_csv(f"{self.output_path}/legal_data.csv", index=False)
        with self.illegal_lock:
            self.illegal_data.to_csv(f"{self.output_path}/illegal_data.csv", index=False)

    def run(self):
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_item = {executor.submit(self.process_item, (row['Title'], row['URL'], row['Date'], row['Category'])): row for _, row in self.data.iterrows()}

            # Start result saving thread
            save_thread = Thread(target=self.save_results)
            save_thread.start()

            # Wait for all tasks to complete
            for future in as_completed(future_to_item):
                item = future_to_item[future]
                try:
                    future.result()
                except Exception as exc:
                    print(f'{item} generated an exception: {exc}')

            # Wait for save thread to finish
            save_thread.join()

        print("All tasks completed and results saved.")

# Usage example
if __name__ == "__main__":
    data = pd.read_csv("processed_bookmarks.csv")
    client = ZhipuAI(api_key="your-api-key")
    
    communicator = MultiThreadCommunicator(data, client, output_path="results")
    communicator.run()