from setuptools import setup, find_packages
import sys, os

version = '0.1'

setup(name='cozydb',
      version=version,
      description="A cozy MySQL-python wrapper",
      long_description="""\
""",
      classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
      keywords='',
      author='Xupeng Yun',
      author_email='recordus@gmail.com',
      url='http://blog.xupeng.me',
      license='BSD',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      zip_safe=False,
      install_requires=[
          'MySQL-python',
      ],
      entry_points="""
      # -*- Entry points: -*-
      """,
      tests_require=['nose'],
      test_suite='nose.collector',
)
