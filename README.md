# Databricks squema compare

Compares tables, views and user defined function definitions between databricks schemas in hive metastore and/or code repository DDL definitions.

## Purpose

The purpose of this tool is to compare definitions of objects (currently only tables, views and user defined functions) between databricks squemas on hive metatore. 
Therefore, if for example, one schema is reserved for integration and another one is reserved for production the differences between both can be discovered.
This tool is suitable for projects that have all environments and databricks schemas on the same databricks instance. It cannot compare at this moment schemas that live on different instances (but this could change in future).

Additionally, the tool can compare definitions for tables, views and user defined functions stored as DDL statements in your code repository against a databricks schema. 
This way you can check for example, if definitions are correctly deployed according to definitions in code repository.

The tool, does not create or execute any script to syncronize the found differences, it is only a reporting tool that shows you the differences. It is not doing any changes on databricks, it is a read-only tool.

## Installation

The tool runs locally on PC from command line. You need to have python installed on your machine. No anaconda or other python big bundle is necessary for the installation, just bare bones python as downloaded from https://www.python.org/downloads. Current version is tested using python 3.12.1.

Once python is installed, you need to install databricks python sql connector as described here https://docs.databricks.com/en/dev-tools/python-sql-connector.html:

```
pip install databricks-sql-connector
```

## Connection to databricks instance

In order to connect to databricks the tool is depending on the setup of the following environment variables:

|Environment variable      |Content                |
|--------------------------|-----------------------|
|DATABRICKS_SERVER_HOSTNAME|Databricks instance    |
|DATABRICKS_HTTP_PATH      |Databricks http path   |
|DATABRICKS_TOKEN          |Databricks access token|

## Configuration file

If a file named \"dbsc-config.json\" exists on the current path it will be read automatically by the tool in order to setup additional options and default configurations.

This file is a JSON file that contains a dictionary as root node, the entries allowed in the root node are the following:

|Configuration item      |Data type       |Description|
|------------------------|----------------|-----------|
|schema_groups           |(dictionary)    |Defines groups of schemas like a shortcut to write less on the console (if an environment consists of three schemas, bronze, siver and gold, we can have an abbreviation to reffer to these three)|
|schema_name_replacements|(dictionary)    |Defines string replacements to be performed in schema names in order to be comparable. In order to be able to compare two schemas named, for example, \"dev_bronze\" and \"int_bronze\", we must replace \"dev_\" and \"int_\" by the same string, so that between schemas object names can be related to each other.|
|ignored_objects_in_repo |(list of string)|List of strings containing the list of objects that might be present in schemas but never in repository, therefore we do not send comparison differences for these when comparing against repository (i.e.: temporary tables/views)|


This is an example of a configuration file:

```
{
  "schema_groups":{
    "@dev" :"dev_bronze+dev_silver+dev_gold",
    "@int" :"int_bronze+int_silver+int_gold",
    "@prod":"prod_bronze+prod_silver+prod_gold"
  },
  "schema_name_replacements":[
    {"substring":"dev_","replacement":"_${env}_"},
    {"substring":"int_","replacement":"_${env}_"},
    {"substring":"prod_","replacement":"_${env}_"}
  ],
  "ignored_objects_in_repo":[
    "tabl:${env}_silver.tmp_*"
  ]
}

```

## Running the tool

The tool has two running modes, one for doing comparisons and another one downloading to JSON schema definitions.

For doing schema comparison the tool is to be called like this:

python dbsc.py \<source\> \<target\> \[--filter:\<pattern\>\] \[--sep\] \[--raw\] \[--np\]

For downloading schema definition to JSON the tool is to be called like this:

python dbsc.py --dump:\<source\> \[--filter:\<pattern\>\] \[--np\]

On both cases the meaning of the parameters on command line is the following:

\<source\>           : Databricks source schema names (one or several separated by +), schema group (specified in configuration file) or project folder

\<target\>           : Databricks target schema names (one or several separated by +), schema group (specified in configuration file) or project folder

--dump:\<source\>    : No comparison, just dump schema definition as json to console

--filter:\<pattern\> : Filter objects to compare using pattern

--sep              : Print separation line between objects in results

--raw              : Report results as raw list

--np              : No progress indicator

## Examples

Example 1: Dump definition of schema "prod_gold" into JSON file
```
python dbsc.py --dump:prod_gold > definition.json
```

Example 2: Do a full comparison between integration and production (according to schemas defined in configuration file)
```
python dbsc.py @int @prod
```

Example 3: Do a comparison on specific schemas between integration and production
```
python dbsc.py int_bronze+int_silver prod_bronze+prod_silver
```

Example 4: Do a comparison for a single between integration and production
```
python dbsc.py int_gold prod_gold
```

## Limitations

Not everything that exists on the hive metatore for a specific schema is be compared, this tool is focused only on tables, views and user defined functions.

Also, it is important to note that not all possible options that you can have in the definition for these three objects are compared the tool is covering the most important topics:

|Item                         |Features compared                                     |
|-----------------------------|------------------------------------------------------|
|Tables                       |Name,comment,table columns                            |
|Table columns                |Name,data type,null/not null,comment                  |
|Views                        |Name,comment,sql definition                           |
|Scalar user defined functions|Name,comment,parameter list,return type,sql definition|
|Table user defined functions |Name,comment,parameter list,return type,sql definition|

When comparing to DDL statements in code repository (project folder) only python files (.py extension are read). DDL statements are read from the cells that start with magic command %sql.

The current version has been tested on databricks runtime version 13.3 LTS without unity catalog enabled.