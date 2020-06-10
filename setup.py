from setuptools import setup

from db_handler import __version__

VERSION = __version__

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='db-handler',
    version=VERSION,
    packages=['db_handler'],
    url='https://realmatch.visualstudio.com/Algo%20-%20Pandologic/_git/db-handler',
    license='MIT License',
    author='Pandologic',
    author_email='ebrill@pandologic.com',
    description='Python integration with multiple databases types',
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
            "Programming Language :: Python :: 3",
            "License :: OSI Approved :: MIT License",
            "Operating System :: OS Independent",
        ],
    python_requires='>=3.6',
    install_requires=[
                        'pandas==1.0.3',
                        'pyodbc==4.0.30'
                    ]
)
