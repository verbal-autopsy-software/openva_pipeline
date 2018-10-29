Documentation for classes, functions, and methods
=================================================

.. module:: runPipeline
.. module:: pipeline
.. module:: transferDB
.. module:: odk
.. module:: openVA
.. module:: dhis
.. module:: exceptions

Main Interface
--------------

The OpenVA Pipeline is run using the following function

.. autofunction:: runPipeline.runPipeline


API for Transfer Database
-------------------------
.. autoclass:: transferDB.TransferDB
   :inherited-members:

API for ODK Briefcase
---------------------
.. autoclass:: odk.ODK
   :inherited-members:

API for OpenVA
--------------
.. autoclass:: openVA.OpenVA
   :inherited-members:

API for DHIS2
-------------
.. autoclass:: dhis.DHIS
   :inherited-members:
.. autoclass:: dhis.API
   :inherited-members:
.. autoclass:: dhis.VerbalAutopsyEvent
   :inherited-members:
.. autofunction:: dhis.create_db
.. autofunction:: dhis.getCODCode
.. autofunction:: dhis.findKeyValue

Excetptions
-----------
.. autoexception:: exceptions.PipelineError
.. autoexception:: exceptions.DatabaseConnectionError
.. autoexception:: exceptions.PipelineConfigurationError
.. autoexception:: exceptions.ODKConfigurationError
.. autoexception:: exceptions.OpenVAConfigurationError
.. autoexception:: exceptions.DHISConfigurationError
.. autoexception:: exceptions.ODKError
.. autoexception:: exceptions.OpenVAError
.. autoexception:: exceptions.SmartVAError
.. autoexception:: exceptions.DHISConfigurationError
