from distutils.core import setup

setup(
    name='mbutil',
    version='0.4.0',
    author='Thomas Rasch',
    author_email='thomas.rasch@alpstein.com',
    packages=['mbutil'],
    scripts=['mb-util'],
    url='https://github.com/Alpstein/mbutil',
    license='LICENSE.md',
    description='An importer and exporter for MBTiles',
    long_description=open('README.md').read(),
)
