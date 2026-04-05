from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
)

console = Console()
_warning_count = 0
_quiet = False


def warning_count() -> int:
    return _warning_count


def reset_warning_count() -> None:
    global _warning_count  # noqa: PLW0603
    _warning_count = 0


# Consistent visual language
ICON_SUCCESS = "[bold green]✔[/bold green]"
ICON_FAIL = "[bold red]✘[/bold red]"
ICON_CACHE = "[bold yellow]🗲[/bold yellow]"
ICON_NETWORK = "[bold cyan]↓[/bold cyan]"
ICON_INFO = "[bold blue]\N{INFORMATION SOURCE}[/bold blue]"
ICON_WARN = "[bold yellow]⚠[/bold yellow]"


def section_header(title: str) -> None:
    if _quiet:
        return
    console.print()
    console.rule(f"[bold cyan]{title}[/bold cyan]")


def log_info(message: str) -> None:
    if _quiet:
        return
    console.print(f"  {ICON_INFO} {message}")


def log_success(message: str) -> None:
    if _quiet:
        return
    console.print(f"  {ICON_SUCCESS} {message}")


def log_warn(message: str) -> None:
    _increment_warning_count()
    if _quiet:
        return
    console.print(f"  {ICON_WARN} {message}")


def _increment_warning_count() -> None:
    global _warning_count  # noqa: PLW0603
    _warning_count += 1


def log_error(message: str) -> None:
    console.print(f"  {ICON_FAIL} [bold red]{message}[/bold red]")


def log_cache_hit(remaining_seconds: float) -> None:
    hours = remaining_seconds / 3600
    time_str = f"{hours / 24:.1f} days" if hours >= 24 else f"{hours:.1f} hours"
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
    from rich.panel import Panel

    console.print(Panel(body, title=f"[bold red]{title}[/bold red]", border_style="red"))
