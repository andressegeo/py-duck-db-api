
from setuptools import setup

setup(
    name='db-api',    # This is the name of your PyPI-package.
    version='0.2',                          # Update the version number for new releases
    packages=['db_api'],
    install_requires=[
        u'MySQL-python',
        u'flask'
    ]
)