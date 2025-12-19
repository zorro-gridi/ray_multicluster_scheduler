from setuptools import setup, find_packages

# Read the contents of README.md for the long description
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="ray_multicluster_scheduler",
    version="0.1.0",
    author="Ray Multi-Cluster Scheduler Team",
    author_email="team@ray-multicluster-scheduler.example.com",
    description="A scheduler for managing tasks across multiple Ray clusters",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/example/ray_multicluster_scheduler",
    packages=find_packages(where="."),
    package_dir={"": "."},
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    python_requires=">=3.8",
    install_requires=[
        "ray>=2.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=6.0",
            "pytest-cov>=2.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "ray-multicluster-scheduler=ray_multicluster_scheduler.main:main",
        ],
    },
)