#!/bin/bash

# Add CRAN repository to software sources to get latest version of R
sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E298A3A825C0D65DFD57CBB651716619E084DAB9
sudo bash -c "echo 'deb https://cloud.r-project.org/bin/linux/ubuntu focal-cran40/' >> /etc/apt/sources.list"

#Install necessary software
sudo apt update
sudo apt-get install -y python3-pip openjdk-11-jdk r-base sqlite3 libsqlite3-dev sqlcipher libsqlcipher-dev curl libcurl4-openssl-dev git make

#Install python packages
git clone https://github.com/verbal-autopsy-software/pyCrossVA -b pipeline GitHub/pycrossva
git clone https://github.com/verbal-autopsy-software/openva_pipeline -b devel GitHub/openva_pipeline

pip install -e GitHub/pycrossva
pip install -e GitHub/openva_pipeline

#Write Rscript to install packages, then run the R script
sudo R CMD javareconf
echo "install.packages('openVA'); q('no')" > packages.r
sudo Rscript packages.r
rm packages.r