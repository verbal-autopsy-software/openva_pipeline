Installation Guide 
==================

The following instructions guide the installation of the OpenVA Pipeline on Ubuntu 16.04 operating system.

  .. note:: 
     To make the installation process easier, all of the required software can be installed by downloading and running the bash script `install_software.sh <https://raw.githubusercontent.com/D4H-CRVS/OpenVA_Pipeline/master/install_software.sh>`_ located in the main folder of the OpenVA_Pipeline repository.

#. Install Python, OpenJDK, **R**, SQLite3, SQLCipher, and Git by typing the following commands at a terminal prompt (indicated by $)

   .. code:: bash

      $ sudo apt update
      $ sudo apt install python3-pip python3-dev openjdk-8-jre r-base r-cran-rjava sqlite3 libsqlite3-dev sqlcipher libsqlcipher-dev git -y

#. Download the `ODK-Briefcase-v1.10.1.jar <https://github.com/opendatakit/briefcase/releases>`_ file to the same folder where *pipeline.py*
   is located.

#. In a terminal, start **R** by simply typing ``R`` at the prompt, or use ``sudo R`` for system-wide installation of
   the packages.  The necessary packages can be installed (with an internet connection) using the following command:

   .. code:: r

     > install.packages(c("openVA", "CrossVA"), dependencies=TRUE)

   Note that ``>`` in the previous command indicates the **R** prompt (not part of the actual command).  This command will
   prompt the user to select a CRAN mirror (choose a mirror close to your geographic location).  After the installation
   of the packages has been completed, you can exit **R** with the following command:

   .. code:: r

     > q('no')

#. Python is pre-installed with Ubuntu 16.04, but additional packages and modules are needed, which can be installed
   with the following commands at a terminal:

   .. code:: bash

     $ pip3 install --upgrade pip --user
     $ hash -d pip3
     $ pip3 install --upgrade setuptools --user
     $ pip3 install requests pysqlcipher3 --user
     
   Note: the first command: ``pip3 install --upgrade pip --user`` will produce a warning message: ::

        You are using pip version 8.1.1, however version 10.0.1 is available.
        You should consider upgrading via the 'pip install --upgrade pip' command.
        
   However, after running the command ``hash -d pip3``, the command ``pip3 --version`` shows that version 10.0.1 is 
   indeed installed.

#. Install DB Browser for SQLite with the commands

   .. code:: bash

      $ sudo apt install build-essential git-core cmake libsqlite3-dev qt5-default qttools5-dev-tools libsqlcipher-dev -y
      $ git clone https://github.com/sqlitebrowser/sqlitebrowser
      $ cd sqlitebrowser
      $ mkdir build
      $ cd build
      $ cmake -Dsqlcipher=1 -Wno-dev ..
      $ make
      $ sudo make install

Instructions for installing JDK 8 on Ubuntu 16.04 can be found `here <http://www.javahelps.com/2015/03/install-oracle-jdk-in-ubuntu.html>`_.
After installing JDK 8, run the following command at the terminal to properly configure **R**

.. code:: r

   $ sudo R CMD javareconf

and then install the **R** packages (as described above).
