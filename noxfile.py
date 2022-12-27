import nox

nox.options.sessions = "lint", "tests"
locations = "src", "tests", "./noxfile.py"


@nox.session(python=["3.10"])
def tests(session):
    args = session.posargs or ["--cov"]
    session.run("poetry", "install", external=True)
    session.run("pytest", *args, external=True)


@nox.session(python=["3.10"])
def lint(session):
    args = session.posargs or locations
    session.install(
        "flake8", "flake8-black", "flake8-isort", "flake8-bugbear", "flake8-bandit"
    )
    session.run("flake8", *args)


@nox.session(python=["3.10"])
def black(session):
    args = session.posargs or locations
    session.install("black")
    session.run("black", *args)


@nox.session(python=["3.10"])
def isort(session):
    args = session.posargs or locations
    session.install("isort")
    session.run("isort", *args)
