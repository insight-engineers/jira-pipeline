from utils.jira_api import JiraProject
from dotenv import load_dotenv
from datetime import datetime
import os

def main():
    load_dotenv()
    project_site = os.getenv('JIRA_PROJECT_SITE')
    project_key = os.getenv('JIRA_PROJECT_KEY')
    auth = (os.getenv('JIRA_EMAIL'), os.getenv('JIRA_TOKEN'))

    jira_project = JiraProject(project_key, auth, project_site)
    jira_project.fetch_issues()
    df = jira_project.to_dataframe()

    output_dir = os.getenv('OUTPUT_DIR', os.getcwd())
    # if not exists, create /data folder
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    csv_filename = os.path.join(output_dir, f'test_{datetime.now().strftime("%Y-%m-%d")}.csv')

    df.to_csv(csv_filename, index=False)
    
if __name__ == '__main__':
    main()