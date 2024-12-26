import typer
from rich.console import Console
from rich.panel import Panel
from rich import print as rprint
from pathlib import Path
from typing import Optional
from openingest import UnifiedDataIngestion
import os

app = typer.Typer(help="🚀 Universal data ingestion tool for LLM context preparation")
console = Console()

def print_version(value: bool):
    if value:
        console.print("[cyan]OpenIngest[/] [green]v0.1.0[/]")
        raise typer.Exit()

@app.callback()
def main(
    version: bool = typer.Option(None, "--version", "-v", callback=print_version, help="Show version"),
):
    """
    \b
    [cyan]╔═╗┌─┐┌┬┐┌─┐  ╦┌┐┌┌─┐┌─┐┌─┐┌┬┐┌─┐┬─┐[/]
    [cyan]╠═╣├─┤ │ ├─┤  ║││││ ┬├┤ └─┐ │ │ │├┬┘[/]
    [cyan]╩ ╩┴ ┴ ┴ ┴ ┴  ╩┘└┘└─┘└─┘└─┘ ┴ └─┘┴└─[/]
    
    [green]Universal data ingestion for LLM context preparation[/]
    """

ASCII_ART = """
╔═╗┌─┐┌┬┐┌─┐  ╦┌┐┌┌─┐┌─┐┌─┐┌┬┐┌─┐┬─┐
╠═╣├─┤ │ ├─┤  ║││││ ┬├┤ └─┐ │ │ │├┬┘
╩ ╩┴ ┴ ┴ ┴ ┴  ╩┘└┘└─┘└─┘└─┘ ┴ └─┘┴└─
"""

def display_header():
    console.print(Panel(ASCII_ART, style="bold blue"))
    console.print("Data Ingestion CLI", style="bold green")

@app.command()
def github(url: str, output: Optional[Path] = None, token: Optional[str] = None):
    """Ingest content from a GitHub repository"""
    display_header()
    with console.status("[bold green]Ingesting GitHub repository..."):
        try:
            ingester = UnifiedDataIngestion(github_token=token or os.getenv('GITHUB_TOKEN'))
            content = ingester.ingest_github(url)
            if output:
                output.write_text(content)
                rprint(f"[green]Content saved to {output}")
            else:
                rprint(Panel(content[:500] + "...", title="Preview"))
        except Exception as e:
            rprint(f"[red]Error: {e}")

@app.command()
def web(url: str, output: Optional[Path] = None):
    """Ingest content from a web URL"""
    display_header()
    with console.status("[bold green]Ingesting web content..."):
        try:
            ingester = UnifiedDataIngestion()
            content = ingester.ingest_url(url)
            if output:
                output.write_text(content)
                rprint(f"[green]Content saved to {output}")
            else:
                rprint(Panel(content[:500] + "...", title="Preview"))
        except Exception as e:
            rprint(f"[red]Error: {e}")

@app.command()
def file(path: Path, output: Optional[Path] = None):
    """Ingest content from a local file"""
    display_header()
    with console.status("[bold green]Ingesting file..."):
        try:
            ingester = UnifiedDataIngestion()
            content = ingester.ingest_file(path)
            if output:
                output.write_text(content)
                rprint(f"[green]Content saved to {output}")
            else:
                rprint(Panel(content[:500] + "...", title="Preview"))
        except Exception as e:
            rprint(f"[red]Error: {e}")

@app.command()
def db(connection: str, query: str, output: Optional[Path] = None):
    """Ingest content from a database"""
    display_header()
    with console.status("[bold green]Ingesting database content..."):
        try:
            ingester = UnifiedDataIngestion()
            content = ingester.ingest_database(connection, query)
            if output:
                output.write_text(content)
                rprint(f"[green]Content saved to {output}")
            else:
                rprint(Panel(content[:500] + "...", title="Preview"))
        except Exception as e:
            rprint(f"[red]Error: {e}")

if __name__ == "__main__":
    app()