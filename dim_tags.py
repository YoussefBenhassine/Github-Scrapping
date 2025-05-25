import requests
import csv
from dotenv import load_dotenv
import os

load_dotenv()

ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")

def fetch_repositories(owner):
    url = f"https://api.github.com/users/{owner}/repos"
    params = {
        "per_page": 100,
        "sort": "created"
    }
    headers = {
        "Authorization": f"token {ACCESS_TOKEN}"
    }
    repositories = []

    while url:
        response = requests.get(url, params=params, headers=headers)
        data = response.json()
        repositories.extend(data)
        if "next" in response.links:
            url = response.links["next"]["url"]
        else:
            url = None

    return repositories

def fetch_tags(owner, repo):
    url = f"https://api.github.com/repos/{owner}/{repo}/tags"
    headers = {
        "Authorization": f"token {ACCESS_TOKEN}"
    }
    tags_info = []

    page = 1
    while True:
        params = {"per_page": 100, "page": page}
        response = requests.get(url, params=params, headers=headers)
        if response.status_code == 200:
            data = response.json()
            if not data:
                break
            tags_info.extend(data)
            page += 1
        else:
            print(f"Failed to fetch tags for {repo}. Status code: {response.status_code}")
            break

    return tags_info

def main():
    owner = os.getenv("OWNER")  
    repositories = fetch_repositories(owner)

    with open("dim_tags.csv", "w", newline="", encoding="utf-8") as csvfile:
        fieldnames = ["Repository ID", "Repository", "Tag Name"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for repo in repositories:
            repo_name = repo["name"]
            repo_id = repo["id"]
            tags_info = fetch_tags(owner, repo_name)
            if tags_info:  
                for tag_info in tags_info:
                    tag_name = tag_info["name"]
                    writer.writerow({
                        "Repository ID": f"{repo_id}",
                        "Repository": f"{repo_name}",
                        "Tag Name": tag_name,
                    })
            else:  
                writer.writerow({
                    "Repository ID": f"{repo_id}",
                    "Repository": f"{repo_name}",
                    "Tag Name": "null",
                })

if __name__ == "__main__":
    main()
