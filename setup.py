from setuptools import setup, Extension
import platform

version = '0.4.0-SNAPSHOT'

setup(
    name='spookystuff',
    zip_safe=True,
    version=version,
    description='General purpose data collection framework that scale.',
    long_description='.',
    url='https://github.com/tribbloid/spookystuff',
    author='tribbloids.com',
    install_requires=[
        'dronekit>=2.8'
    ],
    author_email='pc175@uow.edu.au',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Environment :: Console',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: Apache Software License 2.0',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2.7',
        'Topic :: Scientific/Engineering',
    ],
    license='apache',
    packages=[
        'drone.python', 'drone.python.test'
    ],
    ext_modules=[]
)
