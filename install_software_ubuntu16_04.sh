#!/bin/bash

#--------------------------------------------------------------------------------------------------#
# This script installs the openVA Pipeline and its dependencies.  Users should:
#
# (1) note that this script will install files for sqlitebrowser in the users's home directory
#
#      $HOME/.local/share/sqlitebrowser
#
#     (no problem changing this directory)
#
# (2) note that the script will create a folder for R packages in the user's home directory
#
#      $HOME/R/x86_64-pc-linux-gnu-library/3.5/
#
#      (feel free to change this 
#
# (3) enter in the appropriate path for the variable PIPELINE_WORKING_DIR (directly below)
#
#--------------------------------------------------------------------------------------------------#

# Replace /pipeline/working/dir with the path to the openVA Pipeline's working directory
PIPELINE_WORKING_DIR = $HOME/ovaPipeline
SQLITEBROWSER_INSTALL_DIR = $HOME/.local/share
R_LIBRARY_DIR = $HOME/R/x86_64-pc-linux-gnu-library/3.5

mkdir --parents $PIPELINE_WORKING_DIR
mkdir --parents $SQLITEBROWSER_INSTALL_DIR
mkdir --parents $R_LIBRARY_DIR

# Install necessary software
sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 0x51716619e084dab9
sudo bash -c "echo 'deb https://cloud.r-project.org/bin/linux/ubuntu xenial-cran35/' >> /etc/apt/sources.list"

sudo apt update
sudo apt-get install -y python3-pip python3-dev openjdk-8-jdk r-base sqlite3 libsqlite3-dev sqlcipher libsqlcipher-dev git

# Update the Python package installer: pip

## the following command produces a warning message which can be safely ignored:
pip3 install --upgrade pip --user
## warning message:
## "You are using pip version 8.1.1, however version 10.0.1 is available.
## You should consider upgrading via the 'pip install --upgrade pip' command."
hash -d pip3
## However, running the above command and then "pip3 --version" shows that version 10.0.1 is indeed installed.
pip3 install --upgrade setuptools --user

# Install python packages with Pipenv (recommended)
cd $PIPELINE_WORKING_DIR
pip3 install pipenv --user
pipenv --python 3 
pipenv install openva-pipeline
## If you do not want to use a virtual environment, then comment out the previous 3 lines and uncomment the following line
#pip3 install openva-pipeline --user

# Configure Java with R, write an R script to install packages, then run and remove script
sudo R CMD javareconf JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/bin/jar
echo "install.packages(c('openVA', 'CrossVA'), lib = $R_LIBRARY_DIR, repos='https://cloud.r-project.org/')
q('no')" > packages.r

Rscript packages.r
rm packages.r

# optional (but recommended) step for installing DB Browser for SQLite (useful for configuring pipeline)
sudo apt install -y build-essential cmake qt5-default qttools5-dev-tools
cd $SQLITEBROWSER_INSTALL_DIR
git clone https://github.com/sqlitebrowser/sqlitebrowser

cd sqlitebrowser
mkdir build
cd build
cmake -Dsqlcipher=1 -Wno-dev ..
make
sudo make install
