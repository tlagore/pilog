from setuptools import setup, find_packages


with open('README.md') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='pymsg',
    version='0.1.0',
    description='A generic tcp message sending package',
    long_description=readme,
    author='Tyrone Lagore',
    author_email='tyronelagore@gmail.com',
    url='https://github.com/kennethreitz/samplemod',
    license=license,
    packages=find_packages(exclude=('tests', 'docs'))
)