<<<<<<< HEAD
import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
      name='pysparkcli',
      version="0.0.1",
      description='PySpark Project Buiding Tool',
      url='http://code.qburst.com/jinoj/pysparkcli',
      author='Jino J',
      author_email='jinoj@qburst.com',
      license='MIT',
      packages=['pysparkcli'],
      zip_safe=False)
=======
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
>>>>>>> fbb2fe40821568fe4b8c089e16bf9c9c158add10
