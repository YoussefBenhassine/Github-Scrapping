import aiohttp
import csv
from datetime import datetime
import asyncio
from dotenv import load_dotenv
import os

load_dotenv()

owner = os.getenv("OWNER")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")

async def fetch_commit_history(session, username, repo):
    url = f"https://api.github.com/repos/{username}/{repo}/commits"
    try:
        async with session.get(url, headers={'Authorization': f'token {ACCESS_TOKEN}'}) as response:
            response.raise_for_status()  # Mettre en place une exception pour les erreurs http
            commits = await response.json()
            return commits
    except aiohttp.ClientError as e:
        print(f"Failed to fetch commits for {repo}. Error: {e}")
        return []

def calculate_deployment_speed(commits):
    if len(commits) < 2:
        return None
    
    # ordre chronologique
    commits.sort(key=lambda x: x['commit']['author']['date'])
    
    total_time = 0
    for i in range(1, len(commits)):
        timestamp1 = datetime.strptime(commits[i-1]['commit']['author']['date'], "%Y-%m-%dT%H:%M:%SZ")
        timestamp2 = datetime.strptime(commits[i]['commit']['author']['date'], "%Y-%m-%dT%H:%M:%SZ")
        time_diff = (timestamp2 - timestamp1).total_seconds()
        
        if time_diff >= 0:
            total_time += time_diff
    
    if total_time > 0:
        average_time_between_commits_days = total_time / (len(commits) - 1) / 86400
    else:
        return None
    
    return average_time_between_commits_days

async def fetch_and_store_deployment_speed():
    csv_file = f"dim_deployment_speed.csv"

    try:
        with open(csv_file, mode='w', newline='') as file:
            fieldnames = ['Repository ID','Repository', 'Deployment Speed (days)']
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()

            async with aiohttp.ClientSession() as session:
                page = 1
                repo_count = 0
                while True:
                    url = f"https://api.github.com/users/{owner}/repos"
                    async with session.get(url, params={'page': page, 'per_page': 100, 'sort': 'created'}) as response:
                        response.raise_for_status()
                        repos = await response.json()
                        if not repos:
                            break 
                        for repo in repos:
                            repo_name = repo["name"]
                            repo_id = repo["id"]
                            commits = await fetch_commit_history(session, owner, repo_name)
                            deployment_speed = calculate_deployment_speed(commits)
                            if deployment_speed is not None:
                                writer.writerow({'Repository ID': repo_id,'Repository': repo_name, 'Deployment Speed (days)': deployment_speed})
                                repo_count += 1
                        page += 1  
        print(f"Deployment speed stored in {csv_file}.")
    except aiohttp.ClientError as e:
        print(f"Failed to fetch repositories. Error: {e}")

asyncio.run(fetch_and_store_deployment_speed())
