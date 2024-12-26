# OpenIngest

Universal data ingestion CLI tool for easy data collection and LLM context preparation. Supports GitHub repos, web content, local files, and databases.


## Features

- GitHub repository ingestion (code, documentation, metadata)
- Web content scraping with text cleaning
- Local file support (CSV, Excel, JSON, text)
- Database querying
- Rich CLI interface with progress indicators
- Output to file or preview in terminal

## Installation

```bash
git clone https://github.com/wansatya/openingest
cd openingest
pip install -e .
```

## Usage

```bash
# Show help and version
openingest --help
openingest -v

# GitHub repositories
openingest git https://github.com/user/repo --output data.txt
openingest git --help  # Show git command help

# Web content
openingest web https://example.com --output web_data.txt
openingest web --help  # Show web command help

# Local files
openingest file data.csv --output processed.txt
openingest file --help  # Show file command help

# Database
openingest db "postgresql://user:pass@localhost/db" "SELECT * FROM table"
openingest db --help  # Show database command help
```

### Environment Variables

- `GITHUB_TOKEN`: GitHub personal access token (optional)

## Requirements

- Python 3.8+
- Dependencies:
  - typer
  - rich
  - pandas
  - requests
  - beautifulsoup4 
  - PyGithub
  - sqlalchemy

## License

[MIT](LICENSE)

## Contributing

1. Fork the repository
2. Create feature branch
3. Commit changes
4. Push to branch
5. Create Pull Request