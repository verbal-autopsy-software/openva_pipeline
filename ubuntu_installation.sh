#!/bin/bash

# Ubuntu 20.04 or 22.04: install openVA Pipeline in a virtual environment in current directory

# Install dependencies (R, Java, sqlcipher, etc.)
sudo apt update
sudo apt-get install -y python3-pip python3-venv openjdk-11-jdk r-base sqlite3 libsqlite3-dev sqlcipher libsqlcipher-dev curl libcurl4-openssl-dev

# Install openva_pipeline
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install --upgrade setuptools
pip install openva-pipeline
deactivate

# Write Rscript to install packages, then run the R script
sudo R CMD javareconf
echo "install.packages(c('openVA', 'lubridate')); q('no')" > packages.r
sudo Rscript packages.r
rm packages.r
