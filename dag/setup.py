from setuptools import find_packages, setup

setup(
    name="odapi",
    packages=find_packages(exclude=["odapi_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
