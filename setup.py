import glob
import io
import os
import shutil
import sys

from distutils.log import WARN, ERROR

from setuptools import find_packages, setup, Command

import oedtools


SETUP_DIR = os.path.abspath(os.path.dirname(__file__))


def get_readme():
    with io.open(os.path.join(SETUP_DIR, 'README.md'), encoding='utf-8') as readme:
        return readme.read()


def get_install_requirements():
    with io.open(os.path.join(SETUP_DIR, 'requirements-package.in'), encoding='utf-8') as requirements:
        return requirements.readlines()

version = oedtools.__version__
requirements = get_install_requirements()
readme = get_readme()


try:
    from wheel.bdist_wheel import bdist_wheel

    class BdistWheel(bdist_wheel):
        command_name = 'bdist_wheel'
        user_options = bdist_wheel.user_options

        def finalize_options(self):
            bdist_wheel.finalize_options(self)
            self.root_is_pure = False

        def get_tag(self):
            python, abi, platform = bdist_wheel.get_tag(self)
            python, abi = 'py2.py3', 'none'
            return python, abi, platform

except ImportError:
    BdistWheel = None

# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))


class Publish(Command):
    command_name = 'publish'
    user_options = [
        ('wheel', None, 'Publish the wheel'),
        ('sdist', None, 'Publish the sdist tar'),
        ('no-clean', None, 'Don\'t clean the build artifacts'),
        ('sign', None, 'Sign the artifacts using GPG')
    ]
    boolean_options = ['wheel', 'sdist']

    def initialize_options(self):
        self.wheel = False
        self.sdist = False
        self.no_clean = False
        self.sign = False

    def finalize_options(self):
        if not (self.wheel or self.sdist):
            self.announce('Either --wheel and/or --sdist must be provided', ERROR)
            sys.exit(-1)

    def run(self):
        if os.system('pip freeze | grep twine'):
            self.announce('twine not installed.\nUse `pip install twine`.\nExiting.', WARN)
            sys.exit(-1)

        if self.sdist:
            os.system('python setup.py sdist')

        if self.wheel:
            os.system('python setup.py bdist_wheel')

        if self.sign:
            for p in glob.glob('dist/*'):
                os.system('gpg --detach-sign -a {}'.format(p))

        os.system('twine upload dist/*')
        print('Tag the version:')
        print('  git tag -a {v} -m \'version {v}\''.format(v=version))
        print('  git push --tags')

        if not self.no_clean:
            shutil.rmtree('dist')
            shutil.rmtree('build')
            shutil.rmtree('oedtools.egg-info')


setup(
    name='oedtools',
    version=version,
    packages=find_packages(exclude=('tests', 'tests.*', 'tests.*.*')),
    include_package_data=True,
    package_data={
        '': [
            'requirements-package.in',
            'LICENSE',
        ],
        'oedtools/schema/': ['*']
    },
    exclude_package_data={
        '': ['__pycache__', '*.py[co]'],
    },
    scripts=['oed'],
    license='BSD 3-Clause',
    description='Command-line OED file validation and query toolkit for the Simplitium OED (re)insurance exposure data format',
    long_description=readme,
    long_description_content_type='text/markdown',
    url='https://github.com/sr-murthy/oedtools',
    author='Sandeep R Murthy',
    author_email='smurthy@protonmail.ch',
    maintainer='Sandeep R Murthy',
    maintainer_email='smurthy@protonmail.ch',
    keywords='Simplitium, Open Exposure Data (OED), exposure, insurance, reinsurance, catastrophe modelling',
    python_requires='>=3.6',
    install_requires=requirements,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Intended Audience :: Education',
        'Intended Audience :: End Users/Desktop',
        'Intended Audience :: Financial and Insurance Industry',
        'Intended Audience :: Science/Research',
        'License :: Public Domain',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.6',
        'Topic :: Scientific/Engineering'
    ]
)
