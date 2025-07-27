from setuptools import setup, find_packages
from pathlib import Path

# Read dependencies from requirements.txt
def load_requirements(filename="requirements.txt"):
    try:
        return Path(filename).read_text().splitlines()
    except FileNotFoundError:
        return []  # Return empty list if requirements.txt is missing

setup(
    name="mqpipeline",
    version="0.1.0",
    packages=find_packages(),
    install_requires=load_requirements(),
    author="Your Name",
    description="A reusable RabbitMQ pipeline handler for publishing and consuming messages.",
    classifiers=[
        "Programming Language :: Python :: 3",
    ],
)
