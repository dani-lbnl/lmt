import ez_setup
ez_setup.use_setuptools()

from ConfigParser import SafeConfigParser

config = SafeConfigParser()
config.read(['site.cfg'])
settings = dict(config.items('settings'))

lmtrc = open('pyLMT/_defaultrc.py', 'w')
lmtrc.write("""
import os
DEFAULT_LMTRC=os.path.expanduser('%s')
""" % settings['lmtrc'])
lmtrc.close()

from setuptools import setup, find_packages
setup(
    name = 'pyLMT',
    version = '0.2.0',
    packages = find_packages(),
    install_requires = ['MySQL_python >= 1.2.3',
                        'numpy >= 1.6.0',
                        'matplotlib >= 1.0.1'],
    author = 'Andrew Uselton',
    author_email = 'acuselton@lbl.gov',
    description = ('Access to and analysis of the database of info '
                   'collected by the Lustre Monitoring tool '
                   '(LMT - https://github.com/chaos/lmt/wiki)'),
    license = 'GPL',
    keywords = 'LMT performance monitoring',
    url = 'https://github.com/chaos/lmt/wiki',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Topic :: Utilities',
        'License :: OSI Approved :: GNU General Public License (GPL)',
        'Operating System :: POSIX',
        'Programming Language :: Python',
        'Topic :: Scientific/Engineering :: Information Analysis',
        'Topic :: System :: Filesystems',
    ],
)
