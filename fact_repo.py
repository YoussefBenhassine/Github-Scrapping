import aiohttp
import asyncio
import csv
from dotenv import load_dotenv
import os

load_dotenv()


async def fetch_data(url, headers, params):
    async with aiohttp.ClientSession() as session:
        retries = 3
        for attempt in range(retries):
            try:
                async with session.get(url, headers=headers, params=params) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        return None
            except aiohttp.ClientError as e:
                print(f"Request failed. Retrying.(Attempt {attempt + 1}/{retries})")
                if attempt == retries - 1:
                    print("Max retries exceeded")
                    return None
                await asyncio.sleep(2)  

async def get_all_repositories(user, token):
    repos = []
    page = 1
    while True:
        url = f"https://api.github.com/users/{user}/repos"
        headers = {"Authorization": f"token {token}"}
        params = {"per_page": 100, "page": page}
        repo_data = await fetch_data(url, headers, params)
        if repo_data is None or len(repo_data) == 0:
            break  
        repos.extend(repo_data)
        page += 1
    return repos

async def get_repo_details(repo, token):
    repo_id = repo['id']
    repo_name = repo['name']
    repo_description = repo['description']
    commits_count = await get_commits_count(repo['owner']['login'], repo_name, token)
    tags_count = await get_tags_count(repo['owner']['login'], repo_name, token)
    branches_count = await get_branches_count(repo['owner']['login'], repo_name, token)
    return repo_id, repo_name, repo_description, commits_count, tags_count, branches_count

async def get_commits_count(user, repo_name, token):
    url = f"https://api.github.com/repos/{user}/{repo_name}/commits"
    headers = {"Authorization": f"token {token}"}
    commits_count = 0
    page = 1
    while True:
        params = {"per_page": 100, "page": page}
        commits_data = await fetch_data(url, headers, params)
        if commits_data is None or len(commits_data) == 0:
            break 
        commits_count += len(commits_data)
        page += 1
    return commits_count

async def get_tags_count(user, repo_name, token):
    url = f"https://api.github.com/repos/{user}/{repo_name}/tags"
    headers = {"Authorization": f"token {token}"}
    tags_count = 0
    page = 1
    while True:
        params = {"per_page": 100, "page": page}
        tags_data = await fetch_data(url, headers, params)
        if tags_data is None or len(tags_data) == 0:
            break  
        tags_count += len(tags_data)
        page += 1
    return tags_count

async def get_branches_count(user, repo_name, token):
    url = f"https://api.github.com/repos/{user}/{repo_name}/branches"
    headers = {"Authorization": f"token {token}"}
    branches_count = 0
    page = 1
    while True:
        params = {"per_page": 100, "page": page}
        branches_data = await fetch_data(url, headers, params)
        if branches_data is None or len(branches_data) == 0:
            break  
        branches_count += len(branches_data)
        page += 1
    return branches_count

async def main():
    user = os.getenv("OWNER")
    token = os.getenv("ACCESS_TOKEN")

    repositories = await get_all_repositories(user, token)
    if repositories:
        repo_details = []
        tasks = [get_repo_details(repo, token) for repo in repositories]
        repo_details = await asyncio.gather(*tasks)

        csv_filename = f"fact_repositories.csv"
        save_to_csv(repo_details, csv_filename)
        print(f"Repository details saved to {csv_filename}")
    else:
        print("No repositories found for the user.")

def save_to_csv(repo_details, filename):
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['Repository ID', 'Name', 'Description', 'Commits Count', 'Tags Count', 'Branches Count'])
        writer.writerows(repo_details)

if __name__ == "__main__":
    asyncio.run(main())
