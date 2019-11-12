from setuptools import setup

setup(
    name="pysparkcli",
    version='0.0.1',
    py_modules=['pysparkcli'],
    install_requires=[
        'pyspark',
    ],
    entry_points='''
        [console_scripts]
        pysparkcli=pysparkcli.bin.start:start
    ''',
    )