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
        try:
            parts = repo_url.rstrip('/').split('/')
            owner, repo_name = parts[-2], parts[-1]
            
            if self.github_token:
                g = Github(self.github_token)
            else:
                g = Github()
            
            repo = g.get_repo(f"{owner}/{repo_name}")
            
            content_parts = [
                f"Repository: {repo.full_name}",
                f"Description: {repo.description}",
                f"Main Language: {repo.language}",
                f"Stars: {repo.stargazers_count}",
                "Contents:\n"
            ]
            
            def process_contents(contents, path=""):
                text_content = []
                if isinstance(contents, list):
                    for content in contents:
                        if content.type == "file":
                            try:
                                file_content = content.decoded_content.decode('utf-8')
                                text_content.append(f"\nFile: {path + content.name}\n{file_content}")
                            except:
                                self.logger.warning(f"Could not decode {content.name}")
                        elif content.type == "dir":
                            try:
                                sub_contents = repo.get_contents(content.path)
                                text_content.extend(process_contents(sub_contents, path + content.name + "/"))
                            except:
                                self.logger.warning(f"Could not access directory {content.path}")
                else:
                    try:
                        file_content = contents.decoded_content.decode('utf-8')
                        text_content.append(f"\nFile: {path}\n{file_content}")
                    except:
                        self.logger.warning(f"Could not decode {path}")
                return text_content
            
            content_parts.extend(process_contents(repo.get_contents("")))
            return "\n".join(content_parts)
            
        except Exception as e:
            self.logger.error(f"Error ingesting GitHub repository: {e}")
            raise
    
    def ingest_url(self, url: str) -> str:
        try:
            if "github.com" in url:
                return self.ingest_github(url)
                
            response = requests.get(url)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            for script in soup(["script", "style"]):
                script.decompose()
                
            text = soup.get_text(separator='\n', strip=True)
            lines = [line.strip() for line in text.split('\n') if line.strip()]
            cleaned_text = '\n'.join(lines)
            
            return f"URL: {url}\nContent:\n{cleaned_text}"
        except Exception as e:
            self.logger.error(f"Error ingesting URL: {e}")
            raise
    
    def ingest_file(self, file_path: Union[str, Path]) -> str:
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
            elif file_path.suffix == '.pdf':
                try:
                    import PyPDF2
                    with open(file_path, 'rb') as f:
                        reader = PyPDF2.PdfReader(f)
                        text = []
                        for page in reader.pages:
                            text.append(page.extract_text())
                        return f"PDF File: {file_path.name}\n{'\n'.join(text)}"
                except ImportError:
                    raise ImportError("Please install PyPDF2: pip install PyPDF2")
            elif file_path.suffix == '.docx':
                try:
                    import docx
                    doc = docx.Document(file_path)
                    full_text = []
                    for para in doc.paragraphs:
                        full_text.append(para.text)
                    return f"DOCX File: {file_path.name}\n{'\n'.join(full_text)}"
                except ImportError:
                    raise ImportError("Please install python-docx: pip install python-docx")
            else:
                with open(file_path, encoding='utf-8') as f:
                    content = f.read()
                return f"File: {file_path.name}\n{content}"
        except Exception as e:
            self.logger.error(f"Error ingesting file: {e}")
            raise
    
    def ingest_database(self, connection_string: str, query: str) -> str:
        try:
            engine = create_engine(connection_string)
            df = pd.read_sql_query(query, engine)
            return f"Database Query Result:\n{df.to_string()}"
        except Exception as e:
            self.logger.error(f"Error ingesting database: {e}")
            raise