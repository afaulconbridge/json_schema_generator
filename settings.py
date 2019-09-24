from distutils.core import setup

setup(
    name='JsonSchemaGenerator',
    version='1.0.0',
    author='Adam Faulconbridge',
    author_email='afaulconbridge@googlemail.com',
    packages=['json_schema_generator',],
    description='JSON schema from JSON lines.',
    long_description=open('README.md').read(),
    install_requires=[
        "dask[bag,distributed]"
    ],
    extras_require={
        'visualization':  ["dask[dot]"]
    },
    entry_points={
        'console_scripts': ['ballgame=json_schema_generator:main'],
    },
)
