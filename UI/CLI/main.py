import typer
import secrets
import requests
import webbrowser
import time
import json
from pathlib import Path
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn

console = Console()
app = typer.Typer(help="MapReduce Distributed System CLI")

# --- CONFIGURATION ---
# Target our own UI Gateway
UI_SERVICE_URL = "http://localhost:8000"
TOKEN_FILE = Path.home() / ".mapreduce_auth_token"

def get_token():
    """Read the stored JWT token from the local file"""
    if not TOKEN_FILE.exists():
        console.print("[bold red] Error: Not logged in.[/bold red]")
        raise typer.Exit()
    return TOKEN_FILE.read_text().strip()

def show_welcome():
    console.print(Panel.fit(
        "[bold cyan]MapReduce Web Terminal[/bold cyan]\n\n"
        "Available commands:\n"
        "• [bold blue]register[/bold blue] : register to system\n"
        "• [bold green]login[/bold green]    : Login to system\n"
        "• [bold yellow]submit[/bold yellow]   : Submit a JSON job file\n"
        "• [bold red]exit[/bold red]      : exit\n",
        title="[white]v1.1[/white]", border_style="bright_blue"
    ))

# --- COMMANDS ---

@app.command()
def login():
    """Starts the login process via the Universal Gateway"""
    state = secrets.token_urlsafe(16)
    # Redirect user to the Gateway root with the state parameter
    login_url = f"{UI_SERVICE_URL}/?state={state}"
    
    console.print("[yellow]Login process started[/yellow]")
    console.print(f"[link={login_url}][bold cyan]Click here to Login via Browser[/bold cyan][/link]")
    
    # Auto-open browser for convenience
    webbrowser.open(login_url)

    # Polling: Ask the UI Service if the user has finished the login
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        transient=True,
    ) as progress:
        progress.add_task(description="Waiting for authentication...", total=None)
        
        while True:
            try:
                # Polling the endpoint we created in UI/main.py
                response = requests.get(f"{UI_SERVICE_URL}/api/cli/check/{state}", timeout=2)
                
                if response.status_code == 200:
                    token = response.json().get("access_token")
                    TOKEN_FILE.write_text(token)
                    console.print("\n[bold green]✅ Login Successful! Token saved locally.[/bold green]")
                    break
                elif response.status_code != 404:
                    console.print(f"\n[red] Login error: {response.status_code}[/red]")
                    break
            except Exception as e:
                console.print(f"\n[red] Failed to connect to UI Service: {e}[/red]")
                break
                
            time.sleep(2)

@app.command()
def register():
    """Redirect to the registration action in UI Service"""
    # Using the same UI logic for registration
    reg_url = f"{UI_SERVICE_URL}/login?state=reg&action=registration"
    console.print("[magenta]Registration process started[/magenta]")
    console.print(f"[link={reg_url}][bold cyan]Click here to Register[/bold cyan][/link]\n")
    webbrowser.open(reg_url)

@app.command()
def submit(file_path: str):
    """Submit a job (JSON file) to the system"""
    token = get_token()
    path = Path(file_path)
    
    if not path.exists():
        console.print(f"[red] File not found: {file_path}[/red]")
        return

    # Pass identity to the Gateway via Authorization header
    headers = {"Authorization": f"Bearer {token}"}
    
    with console.status("[bold blue]Uploading job to cluster..."):
        try:
            with open(path, 'rb') as f:
                # The UI Service expects multipart/form-data
                files = {'file': (path.name, f, 'application/json')}
                response = requests.post(f"{UI_SERVICE_URL}/api/submit-job", headers=headers, files=files)
            
            if response.status_code == 200:
                result = response.json()
                console.print("[bold green]🚀 Job Submitted Successfully![/bold green]")
                console.print(f"Manager response: [white]{result.get('message', 'N/A')}[/white]")
            else:
                console.print(f"[red] Failed: {response.status_code} - {response.text}[/red]")
        except Exception as e:
            console.print(f"[red] Connection Error: {e}[/red]")

@app.command()
def exit_cli():
    """Successfully exited and clear token"""
    if TOKEN_FILE.exists():
        try:
            TOKEN_FILE.unlink() 
        except Exception as e:
            console.print(f"[red] Error on exit: {e}[/red]")
    
    console.print("[bold red] Successfully exited.[/bold red]\n")
    raise typer.Exit()

@app.callback(invoke_without_command=True)
def main(ctx: typer.Context):
    if ctx.invoked_subcommand is None:
        show_welcome()
        
        while True:
            try:
                command = typer.prompt("mapreduce> ").strip().lower()
                
                if command == "login":
                    login()
                elif command == "register":
                    register()
                elif command.startswith("submit "):
                    file = command.replace("submit ", "").strip()
                    submit(file)
                elif command == "exit":
                    exit_cli()
                elif command == "":
                    continue
                else:
                    console.print(f"[red]Unknown Command! : {command}[/red]")
            except (EOFError, KeyboardInterrupt, typer.Exit):
                break

if __name__ == "__main__":
    app()