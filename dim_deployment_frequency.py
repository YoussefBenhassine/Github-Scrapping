import aiohttp
import asyncio
import csv
import time
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()

ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
TARGET_ACCOUNT = os.getenv("OWNER")

GITHUB_API_URL = "https://api.github.com"

async def fetch_data(session, url, headers, params=None):
    retries = 3
    for attempt in range(retries):
        try:
            async with session.get(url, headers=headers, params=params) as response:
                if response.status == 200:
                    return await response.json()
                elif response.status == 403 and 'X-RateLimit-Reset' in response.headers:
                    reset_time = int(response.headers['X-RateLimit-Reset'])
                    sleep_time = max(reset_time - time.time(), 0) + 1
                    print(f"Rate limit exceeded. Sleeping for {sleep_time} seconds.")
                    await asyncio.sleep(sleep_time)
                else:
                    print(f"Request failed with status: {response.status}, {response.reason}")
                    return None
        except aiohttp.ClientError as e:
            print(f"Request failed: {e}. Retrying ({attempt + 1}/{retries})")
            if attempt == retries - 1:
                print("Max retries exceeded")
                return None
        await asyncio.sleep(2)

async def get_repositories(session, user, token):
    url = f"{GITHUB_API_URL}/users/{user}/repos"
    headers = {"Authorization": f"token {token}"}
    repositories = []
    page = 1
    while True:
        params = {"per_page": 100, "page": page}
        repos = await fetch_data(session, url, headers, params)
        if not repos:
            break
        repositories.extend(repos)
        page += 1
    return repositories

async def get_pull_requests(session, repo, token, branch):
    url = f"{GITHUB_API_URL}/repos/{repo['full_name']}/pulls"
    headers = {"Authorization": f"token {token}"}
    params = {"state": "closed", "base": branch, "per_page": 100}  
    pull_requests = []
    page = 1
    while True:
        params["page"] = page
        prs = await fetch_data(session, url, headers, params)
        if not prs:
            break
        pull_requests.extend(prs)
        page += 1
    return pull_requests

async def get_branch(session, repo, token):
    url = f"{GITHUB_API_URL}/repos/{repo['full_name']}/branches/{repo['default_branch']}"
    headers = {"Authorization": f"token {token}"}
    return await fetch_data(session, url, headers)

async def get_pull_requests_for_repos(session, repositories, token):
    tasks = []
    for repo in repositories:
        branch = await get_branch(session, repo, token)
        if branch:
            task = asyncio.create_task(get_pull_requests(session, repo, token, branch['name']))
            tasks.append((repo, task))
    results = []
    for repo, task in tasks:
        prs = await task
        if prs:
            results.append((repo, prs))
    return results

async def main():
    async with aiohttp.ClientSession() as session:
        repositories = await get_repositories(session, TARGET_ACCOUNT, ACCESS_TOKEN)

        with open('dim_deployment_frequency.csv', mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(['Repository ID', 'Repository', 'Pull Request Number', 'Title', 'Created At', 'Merged At', 'Time to Merge (days)'])

            repo_prs = await get_pull_requests_for_repos(session, repositories, ACCESS_TOKEN)

            for repo, prs in repo_prs:
                for pr in prs:
                    if pr['merged_at'] is not None:
                        created_at = datetime.strptime(pr['created_at'], '%Y-%m-%dT%H:%M:%SZ')
                        merged_at = datetime.strptime(pr['merged_at'], '%Y-%m-%dT%H:%M:%SZ')
                        time_to_merge = (merged_at - created_at).total_seconds() / (3600 * 24)
                        writer.writerow([repo['id'], repo['name'], pr['number'], pr['title'], pr['created_at'], pr['merged_at'], time_to_merge])

        print("Data has been successfully written to dim_deployment_frequency.csv")

if __name__ == "__main__":
    asyncio.run(main())
