from setuptools import setup, find_packages
import sys, os

version = '0.1.5'

setup(name='cozydb',
      version=version,
      description="cozydb is a cozy MySQL-python wrapper",
      long_description="""\
""",
      classifiers=[
          'Environment :: Other Environment',
          'License :: OSI Approved :: BSD License',
          'Operating System :: MacOS :: MacOS X',
          'Operating System :: Microsoft :: Windows :: Windows NT/2000',
          'Operating System :: OS Independent',
          'Operating System :: POSIX',
          'Operating System :: POSIX :: Linux',
          'Operating System :: Unix',
          'Programming Language :: Python',
          'Topic :: Database',
          'Topic :: Database :: Database Engines/Servers',
      ],
      keywords='',
      author='Xupeng Yun',
      author_email='recordus@gmail.com',
      url='https://github.com/xupeng/cozydb',
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
