from setuptools import setup

setup(
    name="pysparkcli",
    version='0.0.1',
    description='PySpark Project Buiding Tool',
    url='http://code.qburst.com/jinoj/pysparkcli',
    author='Jino J',
    author_email='jinoj@qburst.com',
    license='MIT',
    py_modules=['pysparkcli'],
    install_requires=[
        'pyspark',
    ],
    entry_points='''
        [console_scripts]
        pysparkcli=pysparkcli.bin.start:start
    ''',
    zip_safe=False
)
