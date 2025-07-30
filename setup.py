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
    license="MIT",
    long_description=load_readme(),
    long_description_content_type="text/markdown",  # Important for README.md
    url="https://github.com/kazimsarikaya/mqpipeline",  # Optional but encouraged
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8"
)
