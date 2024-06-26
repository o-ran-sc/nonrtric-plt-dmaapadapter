.. This work is licensed under a Creative Commons Attribution 4.0 International License.
.. SPDX-License-Identifier: CC-BY-4.0
.. Copyright (C) 2021-2033 Nordix Foundation. All rights reserved
.. Copyright (C) 2024 OpenInfra Foundation Europe. All rights reserved

Developer Guide
===============

This document provides a quickstart for developers of the Non-RT RIC DMaaP Adapter.

Additional developer guides are available on the `O-RAN SC NONRTRIC Developer wiki <https://wiki.o-ran-sc.org/display/RICNR/Release+J>`_.

DMaaP Adapter Service
---------------------

This Java implementation is run in the same way as the Information Coordinator Service.

The following properties in the application.yaml file have to be modified:
* server.ssl.key-store=./config/keystore.jks
* app.webclient.trust-store=./config/truststore.jks
* app.configuration-filepath=./src/test/resources/test_application_configuration.json

Kubernetes deployment
=====================

Non-RT RIC can be also deployed in a Kubernetes cluster, `it/dep repository <https://gerrit.o-ran-sc.org/r/admin/repos/it/dep>`_.
hosts deployment and integration artifacts. Instructions and helm charts to deploy the Non-RT-RIC functions in the
OSC NONRTRIC integrated test environment can be found in the *./nonrtric* directory.

For more information on installation of NonRT-RIC in Kubernetes, see `Deploy NONRTRIC in Kubernetes <https://wiki.o-ran-sc.org/display/RICNR/Release+J+-+Run+in+Kubernetes>`_.

For more information see `Integration and Testing documentation on the O-RAN-SC wiki <https://docs.o-ran-sc.org/projects/o-ran-sc-it-dep/en/latest/index.html>`_.

