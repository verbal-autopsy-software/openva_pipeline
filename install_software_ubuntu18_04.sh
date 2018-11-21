#!/bin/bash

# Add CRAN repository to software sources to get latest version of R
sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 0x51716619e084dab9
sudo bash -c "echo 'deb https://cloud.r-project.org/bin/linux/ubuntu bionic-cran35/' >> /etc/apt/sources.list"

#Install necessary software
sudo apt update
sudo apt-get install -y python3-pip python-dev python3-dev openjdk-11-jdk r-base sqlite3 libsqlite3-dev sqlcipher libsqlcipher-dev git

#Install python packages 
python3 -m pip install --upgrade pip setuptools --user
python3 -m pip install pbr testresources requests pysqlcipher3 pandas --user

#Write Rscript to install packages, then run the R script
sudo R CMD javareconf
echo "install.packages(c('openVA', 'CrossVA'), dependencies=TRUE, repos='https://cloud.r-project.org')
q('no')" > packages.r

sudo Rscript packages.r
rm packages.r

# Install Sqlitebrowser
sudo apt install -y build-essential git cmake libsqlite3-dev qt5-default qttools5-dev-tools qttools5-dev libqscintilla2-qt5-13 libqscintilla2-qt5-l10n

git clone https://github.com/sqlitebrowser/sqlitebrowser

cd sqlitebrowser
mkdir build
cd build
cmake -Dsqlcipher=1 -Wno-dev ..
make
sudo make install
