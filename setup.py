import os
import setuptools


with open(os.path.join(os.path.dirname(__file__), 'README.md')) as readme:
    README = readme.read()

setuptools.setup(  
    name="pysparkcli",
    version='0.0.2',
    description='PySpark Project Buiding Tool',
    url='https://github.com/qburst/PySparkCLI',
    author='Jino Jossy',
    author_email='jinojossy93@gmail.com',
    long_description=README,
    long_description_content_type="text/markdown",
    license='MIT',
    packages=setuptools.find_packages(),
    install_requires=[
        'pyspark',
        'click',
        'jinja2',
        'pathlib'
    ],
    entry_points='''
        [console_scripts]
        pysparkcli=pysparkcli.bin.start:start
    ''',
    zip_safe=False
)
