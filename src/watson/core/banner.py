
import sys
from pathlib import Path
from git import Repo
from git.exc import GitCommandError, InvalidGitRepositoryError


def get_git_commit_sha():
    """Get the current git commit SHA in short form."""
    try:
        repo = Repo(Path(__file__).parent.parent.parent.parent)
        return repo.head.object.hexsha[:7]
    except (GitCommandError, InvalidGitRepositoryError, TypeError, FileNotFoundError):
        return None


def get_version():
    """Get the current version from pyproject.toml or default."""
    try:
        # Try to get version from pyproject.toml
        import tomllib
        with open('pyproject.toml', 'rb') as f:
            data = tomllib.load(f)
            return data.get('project', {}).get('version', '0.1.0')
    except (FileNotFoundError, ImportError, KeyError):
        return "0.1.0"


def welcome():
    """Display Watson ASCII art banner with version information."""
    banner = """
    ╔══════════════════════════════════════════════════════════════╗
    ║                                                              ║
    ║    ██╗    ██╗ █████╗ ████████╗ ███████╗  ██████╗ ███╗   ██╗  ║
    ║    ██║    ██║██╔══██╗╚══██╔══╝ ██╔════╝ ██╔═══██╗████╗  ██║  ║
    ║    ██║ █╗ ██║███████║   ██║    ╚█████╗  ██║   ██║██╔██╗ ██║  ║
    ║    ██║███╗██║██╔══██║   ██║     ╚═══██╗ ██║   ██║██║╚██╗██║  ║
    ║    ╚███╔███╔╝██║  ██║   ██║    ██████╔╝ ╚██████╔╝██║ ╚████║  ║
    ║     ╚══╝╚══╝ ╚═╝  ╚═╝   ╚═╝    ╚═════╝   ╚═════╝ ╚═╝  ╚═══╝  ║
    ║                                                              ║
    ║                        Trading Bot                           ║
    ║                                                              ║
    ╚══════════════════════════════════════════════════════════════╝
    """
    
    version = get_version()
    commit_sha = get_git_commit_sha()
    
    version_info = f"""
    Version: {version}
    Commit:  {commit_sha}
    """
    
    print(banner)
    print(version_info)
    print("=" * 60)
    print()


if __name__ == "__main__":
    welcome()
