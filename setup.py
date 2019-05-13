from setuptools import setup, find_packages

LONG_DESC = open("README.rst").read()

setup(
    name="distkv",
    use_scm_version={"version_scheme": "guess-next-dev", "local_scheme": "dirty-tag"},
    description="A distributed no-master key-value store",
    url="https://github.com/smurfix/distkv",
    long_description=LONG_DESC,
    author="Matthias Urlichs",
    author_email="matthias@urlichs.de",
    license="MIT -or- Apache License 2.0",
    packages=find_packages(),
    setup_requires=["setuptools_scm", "pytest-runner", "trustme >= 0.5"],
    install_requires=[
        "trio_click",
        "trio >= 0.11",
        "range_set >= 0.2",
        "attrs >= 18.2",
        "asyncserf >= 0.11.0",
        "jsonschema >= 2.5",
        "pyyaml >= 3",
        # "argon2 >= 18.3",
        "PyNaCl >= 1.3",
        "diffiehellman",
        "psutil",
    ],
    tests_require=["trustme >= 0.5", "pytest", "flake8 >= 3.7"],
    keywords=["async", "key-values", "distributed"],
    python_requires=">=3.7",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Information Technology",
        "License :: OSI Approved :: MIT License",
        "License :: OSI Approved :: Apache Software License",
        "Framework :: AsyncIO",
        "Framework :: Trio",
        "Operating System :: POSIX :: Linux",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: Implementation :: CPython",
        "Topic :: Database",
        "Topic :: Home Automation",
        "Topic :: System :: Distributed Computing",
    ],
    entry_points="""
    [console_scripts]
    distkv = distkv.command:cmd
    """,
    zip_safe=True,
)
