"""Nox sessions."""
import tempfile

import nox

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
            "--output={}".format(requirements.name),
            external=True,
        )
        session.install("--constraint={}".format(requirements.name), *args, **kwargs)


@nox.session(python=["3.11", "3.10"])
def tests(session):
    """Run the test suite."""
    args = session.posargs or ["--cov"]
    session.run("poetry", "install", external=True)
    session.run("pytest", *args, external=True)


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
            "--output={}".format(requirements.name),
            external=True,
        )
        install_with_constraints(session, "safety")
        session.run("safety", "check", f"--file={requirements.name}", "--full-report")


@nox.session(python=["3.11"])
def docs(session):
    """Build the documentation."""
    args = session.posargs or ["-s"]
    install_with_constraints(session, "mkdocs", "mkdocs-material")
    session.run("mkdocs", "build", *args)
