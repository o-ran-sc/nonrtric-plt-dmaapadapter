.. This work is licensed under a Creative Commons Attribution 4.0 International License.
.. SPDX-License-Identifier: CC-BY-4.0
.. Copyright (C) 2022 Nordix


DMaaP Adapter
~~~~~~~~~~~~~

************
Introduction
************

This is a generic information producer using the Information Coordination Service (ICS) Data Producer API. It can get information from DMaaP (ONAP) or directly from Kafka topics.
The information can be filtered, transformed, aggregated and then delivered to data consumers using REST calls (POST) or via Kafka.

The DMaaP Adapter registers itself as an information producer along with its information types in Information Coordination Service (ICS).
The information types are defined in a configuration file.

A data consumer can create an information job (data subscription) using the ICS consumer API (for R-Apps) or the A1-EI (Enrichment Information) API (for NearRT-RICs) based on the registered information types.
This service will get data from DMaaP MR or Kafka topics and deliver it to the data consumers based on their created subscription jobs.

So, a data consumer may be decoupled from DMaaP and/or Kafka this way.

The service is implemented in Java Spring Boot (DMaaP Adapter Service).

.. image:: ./Architecture.png
   :width: 500pt

*************
Data Delivery
*************
When a data consumer creates a an Information Job, either a URL for REST callbacks, or a Kafka Topic can be given as output for the job. 
After filtering, aggregation and data transnformation the data will be delivered to the output. Several data consumers can receive data from one 
Kafka Topic.

.. image:: ./DataDelivery.png
   :width: 500pt

******************
Configuration File
******************

The configuration file defines which DMaaP and Kafka topics that should be listened to and registered as subscribeable information types.
There is an example configuration file in config/application_configuration.json

Each entry will be registered as a subscribe information type in ICS. The following attributes can be used in each entry:

* id, the information type identifier.

* dmaapTopicUrl, a URL to use to retrieve information from DMaaP. Defaults to not listen to any topic.

* kafkaInputTopic, a Kafka topic to listen to. Defaults to not listen to any topic.

* useHttpProxy, indicates if a HTTP proxy shall be used for data delivery (if configured). Defaults to false.
  This parameter is only relevant if a HTTPproxy is configured in the application.yaml file.

* dataType, this can be set to "pmData" which gives a possibility to perform a special filtering of PM data.

* isJson, this indicates that the received is Json objects (from Kafka a stream of objects and from DMaaP an array of quoted json objects).
  Default value is false.
  If the received data is Json objects, the data sent to the consumer does not need to be quoted.
  When buffering is used the output will be an array of json objects '[{},{}]' as opposed to an array of strings '["string1", "string2"]'.
  When buffering is not used, the output content-type will be 'application/json' as opposed to 'text/plain'. When buffering is used, the
  output content-type will 'application/json' regardless of this parameter.

These parameters will be used to choose which parameter schemas that defines which parameters that can be used when creating an information job/data subscription.

Below follows an example of a configuration file.

.. code-block:: javascript

    {
       "types": [
          {
             "id": "DmaapInformationType",
             "dmaapTopicUrl": "/dmaap-topic-1",
             "useHttpProxy": true,
             "isJson" : true
          },
          {
             "id": "KafkaInformationType",
             "kafkaInputTopic": "TutorialTopic",
          },
          {
             "id": "PmInformationType",
             "dmaapTopicUrl": "/dmaap-topic-2",
             "dataType": "PmData",
             "isJson" : true
          }
       ]
    }

**************************
Information Job Parameters
**************************

When an information consumer creates an information job,it can provide type specific parameters. The allowed parameters are defined by a Json Schema.
The following schemas can be used by the component (are located in dmaapadapter/src/main/resources):

===============
typeSchema.json
===============
This schema will by default be registerred for the type. The following properties are defined:

* kafkaOutputTopic, optional parameter which enables that the Information Job will output the data to a Kafka topic instead of a direct call to one data consumer. The output of a job can be directed to HTTP or to Kafka regardless if the input is retrieved from DMaaP or from Kafka.

* filterType, selects the type of filtering that will be done. This can be one of: "regexp", "json-path", "jslt".

  * regexp is for standard regexp matching of text. Objects that contains a match of the expression will be pushed to the consumer.
  * json-path can be used for extracting relevant data from json.
  * jslt, which is an open source language for JSON processing. It can be used both for selecting matching json objects and for extracting or even transforming of json data. This is very powerful.

* filter, the value of the filter expression.
* bufferTimeout can be used to buffer several json objects received from Kafka when kafkaInputTopic is defined into one json array. If bufferTimeout is used, the delivered data will be a Json array of the objects received. If not, each received object will be delivered in a separate call. This contains:

  * maxSize, the maximum number of objects to collect before delivery to the consumer
  * maxTimeMiliseconds, the maximum time to delay delivery (to buffer).

* maxConcurrency, defines max how many paralell REST calls the consumer wishes to receive. 1, which is default, means sequential. A higher values may increase throughput.


Below follows examples of a filters.

.. code-block:: javascript

    {
      "filterType":"regexp",
      "filter": ".*"
    }


.. code-block:: javascript

    {
      "filterType":"jslt",
      "filter": "if(.event.commonEventHeader.sourceName == \"O-DU-1122\") .event.perf3gppFields.measDataCollection.measInfoList[0].measValuesList[0].measResults[0].sValue"
    }


.. code-block:: javascript

    {
      "filterType":"json-path",
      "filter": "$.event.perf3gppFields.measDataCollection.measInfoList[0].measTypes.sMeasTypesList[0]"
    }

Below follows an example of using bufferTimeout and maxConcurrency.

.. code-block:: javascript

    {
       "bufferTimeout":{
          "maxSize":123,
          "maxTimeMiliseconds":456
       },
       "maxConcurrency":1
    }



=====================
typeSchemaPmData.json
=====================
This schema will be registered when the configured dataType is "pmData".
This will extend the filtering capabilities so that a special filter for PM data can be used. Here it is possible to
define which meas-types (counters) to get from which resources.

The filterType parameter is extended to allow value "pmdata" which can be used for PM data filtering.

* sourceNames an array of source names for wanted PM reports.
* measObjInstIds an array of meas object instances for wanted PM reports. If a the given filter value is contained in the filter definition, it will match (partial matching).
  For instance a value like "NRCellCU" will match "ManagedElement=seliitdus00487,GNBCUCPFunction=1,NRCellCU=32".
* measTypes selects the meas types to get
* measuredEntityDns partial match of meas entity DNs.

All PM filter properties are optional and a non given will result in "match all".
The result of the filtering is still following the structure of a 3GPP PM report.

Below follows an example on a PM filter.

.. code-block:: javascript

    {
      "filterType":"pmdata"
      "filter": {
        "sourceNames":[
           "O-DU-1122"
        ],
        "measObjInstIds":[
           "UtranCell=dGbg-997"
        ],
        "measTypes":[
           "succImmediateAssignProcs"
        ],eparate call.
        "measuredEntityDns":[
           "ManagedElement=RNC-Gbg-1"
        ]
      }
    }
