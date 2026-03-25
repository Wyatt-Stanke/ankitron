from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.progress import (
    Progress,
    SpinnerColumn,
    TextColumn,
    BarColumn,
    TaskProgressColumn,
)
from rich.text import Text
from rich import box

console = Console()
_warning_count = 0


def warning_count() -> int:
    return _warning_count


# Consistent visual language
ICON_SUCCESS = "[bold green]✔[/bold green]"
ICON_FAIL = "[bold red]✘[/bold red]"
ICON_CACHE = "[bold yellow]🗲[/bold yellow]"
ICON_NETWORK = "[bold cyan]↓[/bold cyan]"
ICON_INFO = "[bold blue]ℹ[/bold blue]"
ICON_WARN = "[bold yellow]⚠[/bold yellow]"


def section_header(title: str) -> None:
    console.print()
    console.rule(f"[bold cyan]{title}[/bold cyan]")


def log_info(message: str) -> None:
    console.print(f"  {ICON_INFO} {message}")


def log_success(message: str) -> None:
    console.print(f"  {ICON_SUCCESS} {message}")


def log_warn(message: str) -> None:
    global _warning_count
    _warning_count += 1
    console.print(f"  {ICON_WARN} {message}")


def log_error(message: str) -> None:
    console.print(f"  {ICON_FAIL} [bold red]{message}[/bold red]")


def log_cache_hit(remaining_seconds: float) -> None:
    hours = remaining_seconds / 3600
    if hours >= 24:
        time_str = f"{hours / 24:.1f} days"
    else:
        time_str = f"{hours:.1f} hours"
    console.print(f"  {ICON_CACHE} Using cached data (expires in {time_str})")


def log_network(url: str) -> None:
    console.print(f"  {ICON_NETWORK} Fetching from network: [dim]{url}[/dim]")


def make_progress() -> Progress:
    return Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        console=console,
    )


def print_error_panel(title: str, body: str) -> None:
    console.print(
        Panel(body, title=f"[bold red]{title}[/bold red]", border_style="red")
    )
