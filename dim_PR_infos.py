import aiohttp
import asyncio
import csv
from dotenv import load_dotenv
import os

load_dotenv()

owner = os.getenv("OWNER")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")

async def fetch_repositories(session):
    repositories = []
    page = 1
    while True:
        url = f"https://api.github.com/users/{owner}/repos"
        params = {'sort': 'created', 'direction': 'desc', 'per_page': 100, 'page': page}
        async with session.get(url, params=params, headers={'Authorization': f'token {ACCESS_TOKEN}'}) as response:
            if response.status == 200:
                repos = await response.json()
                if not repos:
                    break
                repositories.extend(repos)
                page += 1
            else:
                print(f"Failed to fetch repositories. Status code: {response.status}")
                break
    return repositories

async def fetch_pull_requests(session, repository):
    open_url = f"https://api.github.com/repos/{owner}/{repository['name']}/pulls?state=open"
    closed_url = f"https://api.github.com/repos/{owner}/{repository['name']}/pulls?state=closed"

    async with session.get(open_url, headers={'Authorization': f'token {ACCESS_TOKEN}'}) as open_response:
        open_pull_requests = await open_response.json()

    async with session.get(closed_url, headers={'Authorization': f'token {ACCESS_TOKEN}'}) as closed_response:
        closed_pull_requests = await closed_response.json()

    return open_pull_requests, closed_pull_requests

async def fetch_comments_count(session, pull_request):
    url = pull_request['comments_url']
    async with session.get(url, headers={'Authorization': f'token {ACCESS_TOKEN}'}) as response:
        if response.status == 200:
            comments = await response.json()
            return len(comments)
        else:
            print(f"Failed to fetch comments for pull request {pull_request['number']} in repository {pull_request['base']['repo']['full_name']}. Status code: {response.status}")
            return 0

async def process_repositories(session, csv_writer):
    repositories = await fetch_repositories(session)
    for repository in repositories:
        await process_repository(session, repository, csv_writer)

async def process_repository(session, repository, csv_writer):
    print(f"Processing repository: {repository['name']}")
    open_pull_requests, closed_pull_requests = await fetch_pull_requests(session, repository)
    
    open_count = len(open_pull_requests)
    closed_count = len(closed_pull_requests)
    merged_count = sum(1 for pr in closed_pull_requests if pr['merged_at'] is not None)
    refused_count = closed_count - merged_count
    total_count = open_count + closed_count
    
    pr_with_comments = 0
    pr_without_comments = 0

    for pr in open_pull_requests:
        comments_count = await fetch_comments_count(session, pr)
        if comments_count > 0:
            pr_with_comments += 1
        else:
            pr_without_comments += 1

    for pr in closed_pull_requests:
        comments_count = await fetch_comments_count(session, pr)
        if comments_count > 0:
            pr_with_comments += 1
        else:
            pr_without_comments += 1

    csv_writer.writerow([repository['id'], repository['name'], open_count, closed_count, merged_count, refused_count, total_count, pr_with_comments, pr_without_comments])
    print(f"Processed {repository['name']}: Open PRs: {open_count}, Closed PRs: {closed_count}, Merged PRs: {merged_count}, Refused PRs: {refused_count}, Total PRs: {total_count}, PRs with comments: {pr_with_comments}, PRs without comments: {pr_without_comments}")

async def fetch_and_store_pull_request_info():
    csv_file = f"dim_pull_requests_status.csv"
    try:
        with open(csv_file, mode='w', newline='') as file:
            fieldnames = ['Repository ID', 'Repository', 'Open Pull Requests', 'Closed Pull Requests', 'Merged Pull Requests', 'Refused Pull Requests', 'Total Pull Requests', 'PR with Comments', 'PR without Comments']
            writer = csv.writer(file)
            writer.writerow(fieldnames)

            async with aiohttp.ClientSession() as session:
                await process_repositories(session, writer)

        print(f"Pull request info stored in {csv_file}.")
    except IOError as e:
        print(f"Failed to write to file: {e}")

async def main():
    await asyncio.gather(fetch_and_store_pull_request_info())

if __name__ == "__main__":
    asyncio.run(main())
