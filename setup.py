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
