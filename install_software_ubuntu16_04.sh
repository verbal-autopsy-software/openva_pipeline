#!/bin/bash

# Install necessary software
sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 0x51716619e084dab9
sudo bash -c "echo 'deb https://cloud.r-project.org/bin/linux/ubuntu xenial-cran35/' >> /etc/apt/sources.list"

sudo apt update
sudo apt-get install -y python3-pip python-dev python3-dev openjdk-8-jdk r-base sqlite3 libsqlite3-dev sqlcipher libsqlcipher-dev git

# Install python packages
## the following command produces a warning message which can be safely ignored:
pip3 install --upgrade pip --user
## warning message:
## "You are using pip version 8.1.1, however version 10.0.1 is available.
## You should consider upgrading via the 'pip install --upgrade pip' command."
hash -d pip3
## However, running the above command and then "pip3 --version" shows that version 10.0.1 is indeed installed.
pip3 install --upgrade setuptools --user
pip3 install requests pysqlcipher3 pandas --user

# Configure Java with R, write an R script to install packages, then run and remove script
sudo R CMD javareconf JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/bin/jar
echo "install.packages(c('openVA', 'CrossVA'), repos='https://cloud.r-project.org/')
q('no')" > packages.r

Rscript packages.r
rm packages.r

# optional (but recommended) step for installing DB Browser for SQLite (useful for configuring pipeline)
sudo apt install -y build-essential cmake libsqlite3-dev qt5-default qttools5-dev-tools
git clone https://github.com/sqlitebrowser/sqlitebrowser

cd sqlitebrowser
mkdir build
cd build
cmake -Dsqlcipher=1 -Wno-dev ..
make
sudo make install
