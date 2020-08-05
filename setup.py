from setuptools import setup

setup(
    name="wikipedia_revisions",
    version="0.1",
    description="Download every wikipedia edit.",
    author="Dominic Burkart",
    author_email="@DominicBurkart",
    packages=["wikipedia_revisions"],
    install_requires=[
        "requests==2.23.0",
        "click==7.1.1",
        "dill==0.3.1.1",
        "python-dateutil==2.8.1",
        "psycopg2==2.8.5",
        "psycopg2cffi==2.8.1",
        "SQLAlchemy==1.3.16",
        "SQLAlchemy-Utils==0.36.6",
    ],
)
