#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
    'requests',
    'sciencebasepy',
    'validators',
    'pydash',
    'beautifulsoup4',
    'jsonbender',
    'Unidecode'
]

setup_requirements = [ ]

test_requirements = [ ]

setup(
    author="R. Sky Bristol",
    author_email='skybristol@gmail.com',
    python_requires='>=3.5',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: Unlicense',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    description="Python for Linked Corporate Master Data provides tools for making corporate master data more linked and more open",
    entry_points={
        'console_scripts': [
            'pylinkedcmd=pylinkedcmd.cli:main',
        ],
    },
    install_requires=requirements,
    license="Unlicense",
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='pylinkedcmd',
    name='pylinkedcmd',
    packages=find_packages(include=['pylinkedcmd', 'pylinkedcmd.*']),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/skybristol/pylinkedcmd',
    version='0.2.3',
    zip_safe=False,
)
