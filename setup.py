from distutils.core import setup

setup(
    name='JsonSchemaGenerator',
    version='1.0.0',
    author='Adam Faulconbridge',
    author_email='afaulconbridge@googlemail.com',
    packages=['json_schema_generator'],
    description='JSON schema from JSON lines.',
    long_description=open('README.md').read(),
    install_requires=[
        "dask[bag,distributed]"
    ],
    extras_require={
        'visualization':  [
            "dask[dot]"
        ],
        'dev': [
            'pytest-cov',
            'flake8',
            'pylint',
            'pip-tools',
            'pipdeptree',
            'pre-commit'
        ]
    },
    entry_points={
        'console_scripts': [
            'json_schema_generator=json_schema_generator:main'
        ]
    },
)
