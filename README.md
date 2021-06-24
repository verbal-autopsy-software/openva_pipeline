OpenVA_Pipeline
===============
[![image](https://img.shields.io/pypi/pyversions/openva_pipeline)](https://pypi.org/project/openva_pipeline/)
[![image](http://readthedocs.org/projects/openva-pipeline/badge/)](http://openva-pipeline.readthedocs.io/)
[![Build status](https://ci.appveyor.com/api/projects/status/gsxtxr40r21s0q79?svg=true)](https://ci.appveyor.com/project/jarathomas/openva-pipeline)
[![Coverage](https://codecov.io/gh/verbal-autopsy-software/openva_pipeline/branch/master/graph/badge.svg?token=307LFXDUJ2)](https://codecov.io/gh/verbal-autopsy-software/openva_pipeline)


Automates the processing of verbal autopsy (VA) data from an ODK Aggregate
server, through the openVA Pipeline middleware, and ending with a DHIS2 server
(with the [VA module](https://github.com/SwissTPH/dhis2_va_draft)). VA data are
analyzed using the [openVA
package](https://github.com/verbal-autopsy-software/openVA) in **R**, which is
demonstrated with code for an RShiny app in the [openVA_App](https://github.com/verbal-autopsy-software/openVA_App) and described in
the video tutorials: [Installation Guide (Windows)](https://youtu.be/C2EPOpTzJTk) and [Analysis Guide](https://youtu.be/K1wkSbTwxkg).


The documentation for the openVA Pipeline can be found at:
[http://openva-pipeline.readthedocs.io/](http://openva-pipeline.readthedocs.io/)

## Getting Started

The OpenVA Pipeline has been designed for (and tested on) Ubuntu LTS 20.04.
The steps to install and configure the pipeline can be found in
`docs/setup.md`. To make the installation easier, there is also a bash shell
script `install_software_ubuntu20_04.sh` that installs all of the necessary software (the
script requires the `sudo` password).

<!-- The documentation can also be found on [Read the Docs](https://openva-pipeline.readthedocs.io/en/latest/): -->

<!-- - [**Software Requirements**](https://openva-pipeline.readthedocs.io/en/latest/software.html)  -->
<!-- - [**Installation Guide**](https://openva-pipeline.readthedocs.io/en/latest/install.html) -->
<!-- - [**Pipeline Configuration**](https://openva-pipeline.readthedocs.io/en/latest/config.html) -->

## License
GNU General Public License v3.0

----

## Licenses for Dependencies (ODK Briefcase and SmartVA-Analyze)

The openva-pipeline package makes use of two external programs:
`ODK-Briefcase-v1.18.0.jar` and `smartva`.  [ODK Briefcase](https://github.com/opendatakit/briefcase) 
is a tool in the Open Data Kit (ODK) Software Suite, and is open-source software available 
under the Apache 2.0 license (see 
[ODK_Briefcase_LICENSE](https://github.com/opendatakit/briefcase/blob/master/LICENSE.md).
`smartva` is an open-source command line interface application for linux (see the 
[SmartVA-Analyze MIT License](https://github.com/ihmeuw/SmartVA-Analyze/blob/master/LICENSE))
that is available for download from the
[ihmeuw/SmartVA-Analyze repository](https://github.com/ihmeuw/SmartVA-Analyze/releases).
**These applications are NOT included under the same license as the OpenVA Pipeline (GPL-3)**.
