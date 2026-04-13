import typer
import secrets
import hashlib
import base64
from pathlib import Path
from rich.console import Console
from rich.panel import Panel

console = Console()
app = typer.Typer()

# --- CONFIGURATION ---
KEYCLOAK_AUTH_URL = "http://localhost:8080/realms/map-project/protocol/openid-connect/auth"
CLIENT_ID = "map-reduce-cli"
REDIRECT_URI = "http://localhost:8000/auth/callback"
TOKEN_FILE = Path.home() / ".mapreduce_auth_token"

def generate_pkce():
    """pkce verifier for keycloak"""
    code_verifier = secrets.token_urlsafe(64)
    code_challenge = base64.urlsafe_b64encode(
        hashlib.sha256(code_verifier.encode()).digest()
    ).decode().replace('=', '').replace('+', '-').replace('/', '_')
    return code_verifier, code_challenge

def show_welcome():
    
    console.print(Panel.fit(
        "[bold cyan]MapReduce Web Terminal[/bold cyan]\n\n"
        "Available commands:\n"
        "• [bold blue]register[/bold blue] : register to system\n"
        "• [bold green]login[/bold green]    : Login to system\n"
        "• [bold red]exit[/bold red]     : exit\n",
        title="[white]v1.0[/white]",
        border_style="bright_blue"
    ))

@app.command()
def login():
    verifier, challenge = generate_pkce()
    state = secrets.token_urlsafe(16)
    
    auth_url = (
        f"{KEYCLOAK_AUTH_URL}?response_type=code&client_id={CLIENT_ID}"
        f"&redirect_uri={REDIRECT_URI}&state={state}"
        f"&code_challenge={challenge}&code_challenge_method=S256"
        f"&scope=openid%20profile%20email"
    )
    
    console.print("[yellow]Login process started[/yellow]")
    console.print(f"[link={auth_url}][bold cyan]Click here to Login via Browser[/bold cyan][/link]")

@app.command()
def register():
    reg_url = (
        f"{KEYCLOAK_AUTH_URL}?response_type=code&client_id={CLIENT_ID}"
        f"&redirect_uri={REDIRECT_URI}&kc_action=registration"
        f"&scope=openid%20profile%20email"
    )
    console.print("[magenta]Registration process started[/magenta]")
    console.print(f"[link={reg_url}][bold cyan]Click here to Register[/bold cyan][/link]\n")

@app.command()
def exit_cli():
    #
    if TOKEN_FILE.exists():
        try:
            TOKEN_FILE.unlink() 
        except Exception as e:
            console.print(f"[red]❌ Error on exit{e}[/red]")
    else:
        console.print("[bold red] Sucessfully exited.[/bold red]\n")
        raise typer.Exit()

    console.print("[bold red] Sucessfully exited.[/bold red]\n")
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