import os


from setuptools import setup, find_namespace_packages

with open(os.path.join(os.path.dirname(__file__), 'README.md')) as readme:
    README = readme.read()

setup(
    name="pysparkcli",
    packages=find_namespace_packages(include=['core.*', 'bin.*']),
    version='0.0.1',
    description='PySpark Project Buiding Tool',
    url='https://github.com/qburst/PySparkCLI',
    author='Jino Jossy',
    author_email='jinojossy93@gmail.com',
    long_description=README,
    long_description_content_type="text/markdown",
    license='MIT',
    py_modules=['pysparkcli', 'core', 'bin'],
    install_requires=[
        'pyspark',
        'click',
        'jinja2',
    ],
    entry_points='''
        [console_scripts]
        pysparkcli=pysparkcli.bin.start:start
    ''',
    zip_safe=False
)
