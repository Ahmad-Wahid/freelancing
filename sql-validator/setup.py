from setuptools import setup, find_packages

setup(
    name="sql_validator",
    version="0.1",
    packages=find_packages(),
    description="A python package to validate SQL Queries",
    author="Ahmad Wahid",
    author_email="ahmedwahid16101@email.com",
    url="https://github.com/yourusername/simplemath",
    entry_points={
        "console_scripts": ["sql_validator=sql_validator.validate:main"],
    },
)
