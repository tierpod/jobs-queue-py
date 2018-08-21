#!/usr/bin/python

# Always prefer setuptools over distutils
from setuptools import setup, find_packages

setup(
    name="jobs-queue",
    version="0.1",
    description="jobs queue with limiter",
    author="Pavel Podkorytov",
    author_email="pod.pavel@gmail.com",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 3",
    ],
    packages=find_packages(),
    scripts=["bin/jobs-queue.py"],
    install_requires=[
        "cachetools",
    ]
)
