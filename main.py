import praw
import psutil
import json
import os
import requests
import re
import time
from multiprocessing import Process, Manager, cpu_count
import traceback
import glob
import clip
import torch
import re
from requests_html import HTMLSession

# Replace the following variables with your Reddit API credentials
client_id = "zqO6GY2S1tsXXXXXXX"
client_secret = "Q-XXXXXXXXXXXXXXXXXXXXXXX"
user_agent = "dataset-generator_humor"

# Initialize the Reddit API client
reddit = praw.Reddit(client_id=client_id, client_secret=client_secret, user_agent=user_agent)

# Create a folder named "dataset" to store the dataset files
if not os.path.exists("dataset"):
    os.makedirs("dataset")


def process_text(text):
    # Replace URLs with their final destination or the text of the link
    urls = re.findall(r'https?://\S+', text)
    for url in urls:
        try:
            response = requests.head(url, allow_redirects=True, timeout=5)
            if response.status_code == 200:
                text = text.replace(url, response.url)
            else:
                text = text.replace(url, f"<{url}>")
        except Exception as e:
            text = text.replace(url, f"<{url}>")
    return text


def fetch_subreddit_data(subreddit_name, dataset, min_upvotes=50, resume_from=None):
    subreddit = reddit.subreddit(subreddit_name)
    if resume_from:
        print(f"Resuming from {resume_from}...")
        top_posts = subreddit.top(limit=None, params={"after": resume_from})
    else:
        print(f"Fetching data from {subreddit_name}...")
        top_posts = subreddit.top(limit=None)

    for post in top_posts:
        post_title = process_text(post.title)

        post_entry = {
            "User": post_title,
            "upvotes": post.score,
            "post_id": post.id,
            "Answers": []
        }

        if post.score >= min_upvotes:
            post.comments.replace_more(limit=None)
            for comment in post.comments.list():
                if comment.score >= min_upvotes:
                    if comment.author is None:
                        continue
                    if comment.author.name == "[deleted]":
                        continue
                    if comment.author.name == "AutoModerator":
                        continue
                    if comment.author.name == "reddit":
                        continue
                    if comment.author.name == "reddit.com":
                        continue
                    if comment.body == "[deleted]":
                        continue
                    comment_body = process_text(comment.body)
                    post_entry["Answers"].append({
                        "Answer": comment_body,
                        "upvotes": comment.score
                    })

            if post_entry["Answers"]:
                post_entry["Answers"].sort(key=lambda x: x["upvotes"], reverse=True)
                dataset.append(post_entry)
    print(f"Found {len(dataset)} posts with at least {min_upvotes} upvotes...")
    return dataset


def find_last_saved_file():
    dataset_files = glob.glob("dataset/dataset_*.json")
    if not dataset_files:
        return None

    last_file = max(dataset_files, key=os.path.getctime)
    return last_file


def save_dataset(dataset, save_interval=10):
    last_saved_file = find_last_saved_file()
    if last_saved_file:
        start_file_number = int(last_saved_file.split("_")[1].split("-")[0]) + 1
    else:
        start_file_number = 0

    saved_entries = start_file_number

    while True:
        time.sleep(save_interval)

        if not dataset:
            continue

        # Remove duplicates
        unique_dataset = []
        existing_ids = set()
        for entry in dataset:
            if entry["post_id"] not in existing_ids:
                unique_dataset.append(entry)
                existing_ids.add(entry["post_id"])

        # Save the dataset to the existing file
        start_idx = saved_entries
        # end_idx = saved_entries + len(unique_dataset) - 1
        end_idx = 0
        filename = f"dataset/dataset_{start_idx}-{end_idx}.json"

        with open(filename, "w") as f:
            json.dump(unique_dataset, f, indent=2)

        print(f"Dataset saved to {filename}")

        # If the dataset size is larger than 100 MB, create a new file
        if sum([len(json.dumps(entry)) for entry in unique_dataset]) > 100 * 1024 * 1024:
            saved_entries += len(unique_dataset)
            dataset.clear()




def main():
    subreddits = ["learnpython", "python", "programming"]
    min_upvotes = 50
    resume_from = None  # Replace with a post ID to resume from a specific post, e.g., "t3_jy5lkd"

    # Calculate the maximum number of workers
    cpu_count = psutil.cpu_count()
    target_cpu_usage = cpu_count * 0.9
    max_workers = max(1, int(target_cpu_usage))

    # Create a Manager to share the dataset list between processes
    with Manager() as manager:
        shared_dataset = manager.list()

        # Create a list of processes to fetch subreddit data
        fetch_data_processes = [
            Process(target=fetch_subreddit_data, args=(subreddit, shared_dataset, min_upvotes, resume_from))
            for subreddit in subreddits
        ]

        # Start the processes
        for process in fetch_data_processes:
            process.start()

        # Create an additional process to save the dataset periodically
        total_entries = manager.Value("i", 0)
        save_process = Process(target=save_dataset, args=(shared_dataset,))
        save_process.start()

        # Wait for the fetch_data_processes to complete
        for process in fetch_data_processes:
            process.join()

        # Terminate the save_process
        save_process.terminate()

        # Save the final dataset
        save_dataset(shared_dataset, total_entries.value)


if __name__ == "__main__":
    main()