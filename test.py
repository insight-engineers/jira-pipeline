from utils.jira_api import JiraProject
from dotenv import load_dotenv
import os

load_dotenv()

project_site = os.getenv('JIRA_PROJECT_SITE')
project_key = os.getenv('JIRA_PROJECT_KEY')
auth = (os.getenv('JIRA_EMAIL'), os.getenv('JIRA_TOKEN'))

jira_project = JiraProject(project_key, auth, project_site)
jira_project.fetch_issues()
df = jira_project.to_dataframe()

print(df.head())