import pandas as pd
import requests
from bs4 import BeautifulSoup
from github import Github
import os
from typing import Union, List
import json
from pathlib import Path
import logging
from sqlalchemy import create_engine
import subprocess

class UnifiedDataIngestion:
    def __init__(self, github_token: str = None):
        self.github_token = github_token
        self.setup_logging()
    
    def setup_logging(self):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
    
    def ingest_github(self, repo_url: str) -> str:
        """Ingest a GitHub repository and return formatted text."""
        try:
            # Extract owner and repo name from URL
            parts = repo_url.rstrip('/').split('/')
            owner, repo_name = parts[-2], parts[-1]
            
            if self.github_token:
                g = Github(self.github_token)
            else:
                g = Github()
            
            repo = g.get_repo(f"{owner}/{repo_name}")
            
            # Get repository information
            content_parts = [
                f"Repository: {repo.full_name}",
                f"Description: {repo.description}",
                f"Main Language: {repo.language}",
                "Contents:\n"
            ]
            
            # Get file contents recursively
            def process_contents(contents, path=""):
                text_content = []
                for content in contents:
                    if content.type == "file":
                        if content.name.endswith(('.md', '.txt', '.py', '.js', '.java', '.cpp', '.h', '.c', '.json', '.yaml', '.yml')):
                            try:
                                file_content = content.decoded_content.decode('utf-8')
                                text_content.append(f"\nFile: {path + content.name}\n{file_content}")
                            except:
                                self.logger.warning(f"Could not decode {content.name}")
                    elif content.type == "dir":
                        text_content.extend(process_contents(content.get_contents(), path + content.name + "/"))
                return text_content
            
            content_parts.extend(process_contents(repo.get_contents("")))
            
            return "\n".join(content_parts)
        except Exception as e:
            self.logger.error(f"Error ingesting GitHub repository: {e}")
            raise
    
    def ingest_url(self, url: str) -> str:
        """Ingest web content and return formatted text."""
        try:
            if "github.com" in url:
                return self.ingest_github(url)
                
            response = requests.get(url)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Remove script and style elements
            for script in soup(["script", "style"]):
                script.decompose()
                
            # Extract text
            text = soup.get_text(separator='\n', strip=True)
            
            # Clean up text
            lines = [line.strip() for line in text.split('\n') if line.strip()]
            cleaned_text = '\n'.join(lines)
            
            return f"URL: {url}\nContent:\n{cleaned_text}"
        except Exception as e:
            self.logger.error(f"Error ingesting URL: {e}")
            raise
    
    def ingest_file(self, file_path: Union[str, Path]) -> str:
        """Ingest local file and return formatted text."""
        try:
            file_path = Path(file_path)
            
            if not file_path.exists():
                raise FileNotFoundError(f"File not found: {file_path}")
                
            if file_path.suffix == '.csv':
                df = pd.read_csv(file_path)
                return f"CSV File: {file_path.name}\n{df.to_string()}"
            elif file_path.suffix in ['.xls', '.xlsx']:
                df = pd.read_excel(file_path)
                return f"Excel File: {file_path.name}\n{df.to_string()}"
            elif file_path.suffix == '.json':
                with open(file_path) as f:
                    data = json.load(f)
                return f"JSON File: {file_path.name}\n{json.dumps(data, indent=2)}"
            else:
                with open(file_path) as f:
                    content = f.read()
                return f"File: {file_path.name}\n{content}"
        except Exception as e:
            self.logger.error(f"Error ingesting file: {e}")
            raise
    
    def ingest_database(self, connection_string: str, query: str) -> str:
        """Ingest database content and return formatted text."""
        try:
            engine = create_engine(connection_string)
            df = pd.read_sql_query(query, engine)
            return f"Database Query Result:\n{df.to_string()}"
        except Exception as e:
            self.logger.error(f"Error ingesting database: {e}")
            raise

# Usage example:
if __name__ == "__main__":
    ingester = UnifiedDataIngestion(github_token="your_github_token")  # Token is optional
    
    # Ingest from GitHub
    github_text = ingester.ingest_github("https://github.com/username/repo")
    
    # Ingest from web
    web_text = ingester.ingest_url("https://example.com")
    
    # Ingest local file
    file_text = ingester.ingest_file("data.csv")
    
    # Ingest from database
    db_text = ingester.ingest_database(
        "postgresql://user:password@localhost:5432/db",
        "SELECT * FROM table"
    )