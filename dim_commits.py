import aiohttp
import csv
import asyncio
from dotenv import load_dotenv
import os

load_dotenv()

owner = os.getenv("OWNER")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")

async def fetch_all_repositories(session):
    url = f"https://api.github.com/users/{owner}/repos"
    repositories = []

    async with session.get(url, headers={'Authorization': f'token {ACCESS_TOKEN}'}) as response:
        response.raise_for_status()  # Mettre en place une exception pour les erreurs http
        repositories = await response.json()

    while 'next' in response.links:
        async with session.get(response.links['next']['url'], headers={'Authorization': f'token {ACCESS_TOKEN}'}) as response:
            response.raise_for_status()
            repositories.extend(await response.json())

    return repositories

async def fetch_commits(session, repo_name):
    url = f"https://api.github.com/repos/{owner}/{repo_name}/commits"
    commits_data = []

    async with session.get(url, params={'per_page': 100}, headers={'Authorization': f'token {ACCESS_TOKEN}'}) as response:
        response.raise_for_status()  
        commits_data.extend(await response.json())

    while 'next' in response.links:
        async with session.get(response.links['next']['url'], headers={'Authorization': f'token {ACCESS_TOKEN}'}) as response:
            response.raise_for_status()
            commits_data.extend(await response.json())

    return commits_data

async def fetch_and_store_commits():
    csv_file = f"dim_commits.csv"

    try:
        with open(csv_file, mode='w', newline='', encoding='utf-8') as file:
            fieldnames = ['Repository ID','Repository', 'Commit ID', 'Author', 'Message', 'Date']
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()

            async with aiohttp.ClientSession() as session:
                repositories = await fetch_all_repositories(session)

                for repo in repositories:
                    commits_data = await fetch_commits(session, repo["name"])
                    for commit in commits_data:
                        writer.writerow({
                            'Repository ID' : repo["id"],
                            'Repository': repo["name"],
                            'Commit ID': commit["sha"],
                            'Author': commit["commit"]["author"]["name"],
                            'Date': commit["commit"]["author"]["date"],
                            'Message': commit["commit"]["message"]
                        })

        print(f"Commit details stored in {csv_file}.")
    except aiohttp.ClientError as e:
        print(f"Failed to fetch repositories. Error: {e}")

asyncio.run(fetch_and_store_commits())
