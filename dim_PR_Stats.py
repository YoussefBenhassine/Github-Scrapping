import requests
import csv
from dotenv import load_dotenv
import os

load_dotenv()

owner = os.getenv("OWNER")
access_token = os.getenv("ACCESS_TOKEN")

def fetch_pull_requests(username, repo_name, repo_id):
    url = f"https://api.github.com/repos/{username}/{repo_name}/pulls"
    params = {'state': 'all'}  # pour prendre en compte les closed PR
    headers = {'Authorization': f'token {access_token}'}
    pull_requests_data = []
    while url:
        try:
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()  
            pull_requests = response.json()
            for pr in pull_requests:
                pull_request_data = {
                    'Repository ID': repo_id,  
                    'Repository': repo_name,
                    'Number': pr['number'],
                    'Title': pr['title'],
                    'State': pr['state'],
                    'User': pr['user']['login'],
                    'Created At': pr['created_at'],
                    'Updated At': pr['updated_at'],
                    'Closed At': pr['closed_at'],
                    'Merged At': pr['merged_at'],
                }
                pull_requests_data.append(pull_request_data)
            
            if 'next' in response.links:
                url = response.links['next']['url']
            else:
                url = None
        except requests.exceptions.RequestException as e:
            print(f"Failed to fetch pull requests for {repo_name}. Error: {e}")
            break
    return pull_requests_data

def fetch_all_repositories(username):
    url = f"https://api.github.com/users/{username}/repos"
    headers = {'Authorization': f'token {access_token}'}
    repositories = []
    page = 1
    while True:
        try:
            response = requests.get(url, params={'per_page': 100, 'page': page}, headers=headers)
            response.raise_for_status()  
            repos = response.json()
            if not repos:
                break  
            repositories.extend(repos)
            page += 1
        except requests.exceptions.RequestException as e:
            print(f"Failed to fetch repositories. Error: {e}")
            return []
    return repositories

def fetch_all_pull_requests(username, repositories):
    pull_requests_data = []
    for repo in repositories:
        repo_name = repo["name"]
        repo_id = repo["id"]  
        pull_requests = fetch_pull_requests(username, repo_name, repo_id)
        pull_requests_data.extend(pull_requests)
    return pull_requests_data

def store_pull_requests_to_csv(data, csv_file):
    try:
        with open(csv_file, mode='w', newline='', encoding='utf-8') as file:
            fieldnames = ['Repository ID', 'Repository', 'Number', 'Title', 'State', 'User', 'Created At', 'Updated At', 'Closed At', 'Merged At']
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            for pr_data in data:
                writer.writerow(pr_data)
        print(f"Pull requests data stored in {csv_file}.")
    except IOError as e:
        print(f"Error writing to CSV file: {e}")

repositories = fetch_all_repositories(owner)


pull_requests_data = fetch_all_pull_requests(owner, repositories)


csv_file = f"dim_pull_requests_stats.csv"
store_pull_requests_to_csv(pull_requests_data, csv_file)
