from setuptools import find_packages, setup

setup(
    name="binge_works",
    packages=find_packages(include=["binge_works", "fantasy_navigator"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
