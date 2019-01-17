import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="openva_pipeline",
    version="0.0.0.9008",
    author="Jason Thomas, Samuel J. Clark, & Martin Bratschi",
    author_email="jarthomas@gmail.com",
    description="Automates the processing of verbal autopsy data.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/verbal-autopsy-software/openva_pipeline",
    license="GPLv3",
    packages=setuptools.find_packages(),
    include_package_data=True,
    install_requires=[
        'pandas',
        'pysqlcipher3',
        'requests',
        ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.5",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: POSIX :: Linux",
    ],
)

