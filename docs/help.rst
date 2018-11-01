Documentation for classes, functions, and methods
=================================================

.. module:: openva_pipeline

Main Interface
--------------

The OpenVA Pipeline is run using the following function

.. autofunction:: openva_pipeline.runPipeline.runPipeline


API for Transfer Database
-------------------------
.. autoclass:: openva_pipeline.transferDB.TransferDB
   :inherited-members:

API for ODK Briefcase
---------------------
.. autoclass:: openva_pipeline.odk.ODK
   :inherited-members:

API for OpenVA
--------------
.. autoclass:: openva_pipeline.openVA.OpenVA
   :inherited-members:

API for DHIS2
-------------
.. autoclass:: openva_pipeline.dhis.DHIS
   :inherited-members:
.. autoclass:: openva_pipeline.dhis.API
   :inherited-members:
.. autoclass:: openva_pipeline.dhis.VerbalAutopsyEvent
   :inherited-members:
.. autofunction:: openva_pipeline.dhis.create_db
.. autofunction:: openva_pipeline.dhis.getCODCode
.. autofunction:: openva_pipeline.dhis.findKeyValue

Exceptions
-----------
.. autoexception:: openva_pipeline.exceptions.PipelineError
.. autoexception:: openva_pipeline.exceptions.DatabaseConnectionError
.. autoexception:: openva_pipeline.exceptions.PipelineConfigurationError
.. autoexception:: openva_pipeline.exceptions.ODKConfigurationError
.. autoexception:: openva_pipeline.exceptions.OpenVAConfigurationError
.. autoexception:: openva_pipeline.exceptions.DHISConfigurationError
.. autoexception:: openva_pipeline.exceptions.ODKError
.. autoexception:: openva_pipeline.exceptions.OpenVAError
.. autoexception:: openva_pipeline.exceptions.SmartVAError
.. autoexception:: openva_pipeline.exceptions.DHISConfigurationError
