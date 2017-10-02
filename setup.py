from setuptools import setup, find_packages
# To use a consistent encoding
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))


setup(
    name='metadata',

    version='3.0',

    description='Metrolinux Metadata',
    long_description="Metrolinx Project",


    url='http://metrolinx.com',


    author='Sri',
    author_email='sri@metrolinx.com',


    license='Copyright',


    classifiers=[
        'Development Status :: 1 - Alpha',

        # Indicate who your project is intended for
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',



        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],


    packages=find_packages(exclude=['contrib', 'docs', 'tests']),


    extras_require={
        'dev': ['check-manifest'],
        'test': ['coverage'],
    },

    package_data={
        'sample': ['package_data.dat'],
    },

    data_files=[('my_data', ['data/data_file'])],


    py_modules=["__main__"],

    entry_points={
        'console_scripts': [
            'sample=sample:main',
        ],
    },
)
