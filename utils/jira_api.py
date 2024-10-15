import requests
import pandas as pd

class JiraIssue:
    def __init__(self, issue_data, project_site):
        self.parent = issue_data['fields'].get('parent', {}).get('key') if issue_data['fields'].get('parent') else issue_data['key']
        self.key = issue_data['key']
        self.type = issue_data['fields']['issuetype']['name']
        self.url = f"{project_site}/browse/{self.key}"
        self.summary = issue_data['fields']['summary']
        self.description = issue_data['fields']['description']
        self.status = issue_data['fields']['status']['name']
        self.assignee = issue_data['fields']['assignee']['displayName'] if issue_data['fields']['assignee'] else "Unassigned"
        self.reporter = issue_data['fields']['reporter']['displayName']
        self.created = issue_data['fields']['created']
        self.updated = issue_data['fields']['updated']
        self.due = issue_data['fields'].get('duedate')

    def to_dict(self):
        return {
            'parent': self.parent,
            'key': self.key,
            'type': self.type,
            'url': self.url,
            'summary': self.summary,
            'description': self.description,
            'status': self.status,
            'assignee': self.assignee,
            'reporter': self.reporter,
            'created': self.created,
            'updated': self.updated,
            'due': self.due
        }

class JiraProject:
    """
    Example usage:
    jira_project = JiraProject(project_key, auth, project_site)
    jira_project.fetch_issues()
    df = jira_project.to_dataframe()
    """
    def __init__(self, project_key, auth, project_site):
        self.project_key = project_key
        self.auth = auth
        self.project_site = project_site
        self.headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        self.url = f"{project_site}/rest/api/3/search?jql=project={project_key}"
        self.issues = []

    def fetch_raw_issues(self):
        response = requests.get(self.url, headers=self.headers, auth=self.auth)
        return response.json().get('issues', [])

    def fetch_issues(self):
        response = requests.get(self.url, headers=self.headers, auth=self.auth)
        issues_data = response.json().get('issues', [])
        self.issues = [JiraIssue(issue, self.project_site) for issue in issues_data]

    def to_list(self):
        return [issue.to_dict() for issue in self.issues]
    
    def to_dataframe(self):
        rows = [issue.to_dict() for issue in self.issues]
        df = pd.DataFrame(rows)
        df.sort_values(by=['parent', 'key'], inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df
