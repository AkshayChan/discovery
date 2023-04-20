from setuptools import setup

setup(
    name='glue_python_shell_helper_libraries',
    version='0.1',
    install_requires=open("requirements.txt").read().strip().split("\n"),
    packages=['.elo_utils'],
) 