# Live Streaming System

## Overview

This project was created for the course Analysis and Design of Information Systems at NTUA ECE (2021-2022). The aim of this project
was to design a live streaming system that acts as a prototype of an actual IoT system.

## Team members

* [Astrinakis Nikolaos](https://github.com/nickastrin)
* [Kaparou Alexandra](https://github.com/alexandrakapa)
* [Tampakakis Christos](https://github.com/tampakc)

## Prerequisites

In order to use this project, the installation of Java, Apache ActiveMQ, Apache Flink, Cassandra and Apache Grafana is required.

## Details

For more details on this project, please check out the pdf file located in the *Documentation* folder of this repository. 
There are three folders in this repo:
- *MessageGenerator* contains the source code needed to generate random data using ActiveMQ
- *FlinkJob* contains the source code that is used to categorize data by day, detect late events and compute the aggregations. 
- *Documentation* contains a paper and a powerpoint presentation on the project. The code used in Cassandra and Apache Grafana is written in the aforementioned paper.
