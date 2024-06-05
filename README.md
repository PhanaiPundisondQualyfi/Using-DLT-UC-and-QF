# Using Delta Live Tables, Unity Catalog, and Query Federation

This project demonstrates how to use Delta Live Tables, Unity Catalog, and Query Federation in Databricks to create a star schema. The sample data from AdventureWorks was used.
Delta Live Tables (DLT) provide an automated, reliable framework for building data pipelines.
Unity Catalog offers a unified governance solution for managing data and AI assets in Databricks.
Query Federation allows you to access and query data across different data sources without the need to move or replicate data.
The key components and functionalities of this project includes:
- [Planning Data Model](#Planning-Data-Model)
- [Databricks Setup](#Databricks-Setup)
- [Query Federation & Modelling](#Query-Federation-&-Modelling)
- [Visualisation via Microsoft PowerBi](#Visualisation-via-Microsoft-PowerBi)

## Planning Data Model
Create physical data model displaying entities, relationships, named attributes, keys, data types, and null values. Can be viewed in the PNG file.

## Databricks Setup
Configurations:
- Enable Unity Catalog and assign correct permissions
- Create connection, e.g. connecting to Azure SQL DB then use Azure SQL Server details (host, user, password)
- Create catalogs; standard for storing bronze, silver, gold layers and foreign for storing external data
- Create schema in standard catalog as the target schema for Delta Live Tables

## Query Federation & Modelling
Use Databricks notebook to create python programs for query federation and modelling the star schema. Can be viewed in the pipeline folder.

For Delta Live Tables to work, use DLT decorators.

Create Delta Live Tables using above notebook as Source Code and the standard-catalog.target-schema as Destination.

The Product Edition, Compute and Advanced settings may require adjustments based on your capacity and needs.

Validate Delta Live Tables to locate errors, if any. Then Start Delta Live Tables when successfully validated.

## Visualisation via Microsoft PowerBi
Connect Databricks to PowerBi using Databricks built-in fuction, partner connect.

Create insights, examples below:




