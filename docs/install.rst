Installation Guide 
==================

The following instructions guide the installation of the openVA Pipeline on Ubuntu 16.04 operating system.

  .. note:: 
     To make the installation process easier, all of the required software can be installed by downloading and running the bash script
     `install_software_ubuntu16_04.sh <https://https://github.com/verbal-autopsy-software/openva_pipeline/blob/master/install_software_ubuntu16_04.sh>`_
     located in the main folder of the openVA_Pipeline repository.  There is a corresponding script for installing the pipeline (and
     dependencies) on Ubuntu 18.04: 
     `install_software_ubuntu18_04.sh <https://https://github.com/verbal-autopsy-software/openva_pipeline/blob/master/install_software_ubuntu18_04.sh>`_

#. Install Python, OpenJDK, **R**, SQLite3, SQLCipher, and Git by typing the following commands at a terminal prompt (indicated by $)

   .. code:: bash

      $ sudo apt update
      $ sudo apt install python3-pip python3-dev openjdk-8-jre r-base sqlite3 libsqlite3-dev sqlcipher libsqlcipher-dev git -y

#. (*optional*) Download the `ODK-Briefcase-v1.12.2.jar <https://github.com/opendatakit/briefcase/releases>`_ file to a folder that will
   serve as the openVA_Pipeline working directory.

#. Configure OpenJDK with R by running the following command 

   .. code:: bash

      $ sudo R CMD javareconf JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/bin/jar

#. In a terminal, start **R** by simply typing ``R`` at the prompt, or use ``sudo R`` for system-wide installation of
   the packages.  The necessary packages can be installed (with an internet connection) using the following command:

   .. code:: r

     > install.packages(c("openVA", "CrossVA"))

   Note that ``>`` in the previous command indicates the **R** prompt (not part of the actual command).  This command will
   prompt the user to select a CRAN mirror (choose a mirror close to your geographic location), as well as a location for the
   library where the R packages will be installed (this does not happen if R is run as sudo).  After the installation
   of the packages has been completed, you can exit **R** with the following command:

   .. code:: r

     > q('no')

#. Python version 3.5 was installed during the initial ``sudo apt install`` command above, but we still need to update the
   package installer, setup a virtual environment with `Pipenv <https://pipenv.readthedocs.io/en/latest>`_, and install
   the `openva-pipeline <https://pypi.org/project/openva-pipeline/>`_ package.  These tasks can be accomplished at the terminal
   as follows:

   .. code:: bash

     $ pip3 install --upgrade pip --user
     $ hash -d pip3
     $ pip3 install --upgrade setuptools --user
     $ pip3 install pipenv --user
     $ pipenv install openva-pipeline

   We can enter the virtual environment and import the openva-pipeline package with the following terminal commands:

   .. code:: bash

     $ pipenv shell
     $ python


     >>> import openva_pipeline as ovaPL   # import package
     >>> help(ovaPL)                       # access primary help file (hit q to exit)
     >>> help(ovaPL.runPipeline)           # access help file for a particular function
     >>> quit()                            # return to virtualenv terminal shell

     $ exit
     

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



Alternative Installation Options
--------------------------------

Using Java JDK (instead of OpenJDK)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Instructions for installing JDK 8 on Ubuntu 16.04 can be found `here <http://www.javahelps.com/2015/03/install-oracle-jdk-in-ubuntu.html>`_.
After installing JDK 8, run the following command at the terminal to properly configure **R**

.. code:: r

   $ sudo R CMD javareconf

and then install the **R** packages (as described above).


Installing the Pipeline package with using a virtual environment (and pipenv)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Simply use ``pip3`` to install the openva-pipeline package as follows

.. code:: bash

   $ pip3 install openva-pipeline --user

