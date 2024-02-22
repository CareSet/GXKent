from setuptools import setup, find_packages

setup(
    name='GXKent',
    version='0.1.0',
    author='Fred Trotter',
    author_email='fred.trotter@careset.com',
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    url='http://pypi.python.org/pypi/GXKent/',
    license='LICENSE.txt',
    description='An awesome package for GXKent.',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    install_requires=[
        'great-expectations>=0.18.0'
    ],
)
