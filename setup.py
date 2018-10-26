import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="openva_pipeline",
    version="0.0.0.9000",
    author="Jason Thomas",
    author_email="jarthomas@gmail.com",
    description="Automates the processing of verbal autopsy data.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/verbal-autopsy-software/openva_pipeline",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GPLv3 License",
        "Operating System :: Linux",
    ],
)

