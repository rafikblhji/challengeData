from setuptools import find_packages, setup

setup(
    name="rafik_pipeline",
    packages=find_packages(exclude=["rafik_pipeline_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
