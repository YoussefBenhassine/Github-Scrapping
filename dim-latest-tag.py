import requests
import csv
from dotenv import load_dotenv
import os

load_dotenv()

owner = os.getenv("OWNER")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")

def fetch_repository_tags(repo_url):
    try:
        response = requests.get(repo_url + "/tags", headers={'Authorization': f'token {ACCESS_TOKEN}'})
        response.raise_for_status()  
        tags = response.json()
        num_tags = len(tags)
        latest_version = None
        if num_tags > 0:
            latest_version = tags[0]['name']
        return num_tags, latest_version
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch tags. Error: {e}")
        return None, None

def fetch_and_store_repository_tags(username):
    csv_file = f"dim_latest_tags.csv"
    try:
        with open(csv_file, mode='w', newline='') as file:
            fieldnames = ['Repository ID', 'Repository Name', 'Latest Tag']
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()

            page = 1
            while True:
                url = f"https://api.github.com/users/{username}/repos"
                response = requests.get(url, params={'page': page, 'per_page': 100, 'sort': 'created'}, headers={'Authorization': f'token {ACCESS_TOKEN}'})
                response.raise_for_status()
                repos = response.json()
                if not repos:
                    break  
                for repo in repos:
                    repo_id = repo["id"]
                    repo_name = repo["name"]
                    repo_url = repo["url"]
                    num_tags, latest_version = fetch_repository_tags(repo_url)
                    if num_tags is not None:
                        writer.writerow({'Repository ID': repo_id, 'Repository Name': repo_name, 'Latest Tag': latest_version})
                page += 1  
        print(f"Repository tags stored in {csv_file}.")
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch repositories. Error: {e}")

fetch_and_store_repository_tags(owner)