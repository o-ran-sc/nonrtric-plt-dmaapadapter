.. This work is licensed under a Creative Commons Attribution 4.0 International License.
.. SPDX-License-Identifier: CC-BY-4.0
.. Copyright (C) 2021 Nordix


DMaaP Adapter
~~~~~~~~~~~~~

Configurable mediator to take information from DMaaP and Kafka and present it as a coordinated Information Producer.

This adapter is a generic information producer, which register itself as an information producer of defined information types in Information Coordination Service (ICS).
The information types are defined in a configuration file.
Information jobs defined using ICS then allow information consumers to retrieve data from DMaaP MR or Kafka topics (accessing the ICS API).

The adapter is implemented in Java Spring (DMaaP Adapter Service).
