from setuptools import setup, find_packages
from pathlib import Path

# Read dependencies from requirements.txt
def load_requirements(filename="requirements.txt"):
    try:
        return Path(filename).read_text().splitlines()
    except FileNotFoundError:
        return []  # Return empty list if requirements.txt is missing

# Read long description from README.md
def load_readme(filename="README.md"):
    try:
        return Path(filename).read_text(encoding="utf-8")
    except FileNotFoundError:
        return ""


setup(
    name="mqpipeline",
    version="0.1.0",
    packages=find_packages(),
    install_requires=load_requirements(),
    author="Kazım SARIKAYA",
    description="A reusable RabbitMQ pipeline handler for publishing and consuming messages.",
    long_description=load_readme(),
    classifiers=[
        "Programming Language :: Python :: 3",
    ],
)
