"""Nox sessions."""

import tempfile
from pathlib import Path

import nox
from nox import Session

nox.options.sessions = "lint", "tests"
locations = "src", "tests", "./noxfile.py"


def install_with_constraints(session, *args, **kwargs):
    """Install packages constrained by Poetry's lock file."""
    with tempfile.NamedTemporaryFile(delete=False) as requirements:
        session.run(
            "poetry",
            "export",
            "--with=dev",
            "--format=constraints.txt",
            "--without-hashes",
            f"--output={requirements.name}",
            external=True,
        )
        session.install(f"--constraint={requirements.name}", *args, **kwargs)


@nox.session(python=["3.11", "3.10"])
def tests(session):
    """Run the test suite."""
    session.run("poetry", "install", external=True)
    install_with_constraints(session, ".", "coverage[toml]", "pytest", "pytest-cov", "pytest-mock")
    session.run("coverage", "run", "--parallel", "-m", "pytest")


@nox.session(python=["3.11", "3.10"])
def lint(session):
    """Lint using flake8."""
    args = session.posargs or locations
    install_with_constraints(
        session,
        "flake8",
        "flake8-bandit",
        "flake8-black",
        "flake8-bugbear",
        "flake8-docstrings",
        "flake8-isort",
    )
    session.run("flake8", *args)


@nox.session(python=["3.11", "3.10"])
def black(session):
    """Format code using black."""
    args = session.posargs or locations
    install_with_constraints(session, "black")
    session.run("black", *args)


@nox.session(python=["3.11", "3.10"])
def isort(session):
    """Organize imports using isort."""
    args = session.posargs or locations
    install_with_constraints(session, "isort")
    session.run("isort", *args)


@nox.session(python=["3.11", "3.10"])
def safety(session):
    """Scan dependencies for insecure packages."""
    with tempfile.NamedTemporaryFile(delete=False) as requirements:
        session.run(
            "poetry",
            "export",
            "--with=dev",
            "--format=requirements.txt",
            "--without-hashes",
            f"--output={requirements.name}",
            external=True,
        )
        install_with_constraints(session, "safety")
        session.run("safety", "check", f"--file={requirements.name}", "--full-report")


@nox.session(python=["3.11"])
def docs(session):
    """Build the documentation."""
    args = session.posargs
    install_with_constraints(
        session,
        ".",
        "mkdocs",
        "mkdocs-material",
        "mknotebooks",
        "mkdocstrings[python]",
        "mkdocs-autorefs",
        "pygments",
        "jupyter",
        "mkdocs-click",
        "black",
    )
    session.run("mkdocs", "build", *args)


@nox.session(name="docs-deploy", python=["3.11"])
def docs_deploy(session):
    """Deploy the documentation."""
    args = session.posargs
    install_with_constraints(
        session,
        ".",
        "mkdocs",
        "mkdocs-material",
        "mknotebooks",
        "mkdocstrings[python]",
        "mkdocs-autorefs",
        "pygments",
        "jupyter",
        "mkdocs-click",
        "black",
    )
    session.run("mkdocs", "gh-deploy", *args)


@nox.session(python=["3.11", "3.10"])
def coverage(session: Session) -> None:
    """Upload coverage data."""
    args = session.posargs or ["report"]
    install_with_constraints(session, "coverage[toml]")
    if not session.posargs and any(Path().glob(".coverage.*")):
        session.run("coverage", "combine")
    session.run("coverage", *args)
