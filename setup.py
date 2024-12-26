from setuptools import setup, find_packages

setup(
    name="openingest",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "typer",
        "rich",
        "pandas",
        "requests",
        "beautifulsoup4",
        "PyGithub",
        "sqlalchemy",
        "python-docx",
        "PyPDF2",
    ],
    entry_points={
        "console_scripts": [
            "openingest=openingest.cli:app",
        ],
    },
)