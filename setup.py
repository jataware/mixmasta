#!/usr/bin/env python

"""The setup script."""

from setuptools import find_packages, setup


def read_requirements(path: str):
    with open(path) as f:
        return f.read().splitlines()


with open("README.md") as readme_file:
    readme = readme_file.read()

with open("HISTORY.md") as history_file:
    history = history_file.read()

install_requirements = read_requirements("requirements.txt")

setup(
    author="Brandon Rose",
    author_email="brandon@jataware.com",
    python_requires=">=3.5",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    description="A library for common scientific model transforms",
    entry_points={
        "console_scripts": [
            "mixmasta=mixmasta.cli:cli",
        ],
    },
    setup_requires=["numpy>=1.20.1"],  # This is not working as expected
    install_requires=install_requirements,
    license="MIT license",
    long_description=readme + "\n\n" + history,
    long_description_content_type="text/markdown",
    include_package_data=True,
    keywords="mixmasta",
    name="mixmasta",
    package_data={'mixmasta' : ['data/*']},
    packages=find_packages(include=["mixmasta", "mixmasta.*"]),
    test_suite="tests",
    url="https://github.com/jataware/mixmasta",
    version='0.6.4',
    zip_safe=False,
)
