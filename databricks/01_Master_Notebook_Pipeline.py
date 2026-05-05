# Databricks notebook source
dbutils.notebook.run("/Workspace/Repos/prathameshbpatil8512@gmail.com/dev-sales/databricks/02_Config_Connection", 60)

dbutils.notebook.run("/Workspace/Repos/prathameshbpatil8512@gmail.com/dev-sales/databricks/03_Bronze_Ingestion", 60)

dbutils.notebook.run("/Workspace/Repos/prathameshbpatil8512@gmail.com/dev-sales/databricks/04_Silver_Transformation", 60)

dbutils.notebook.run("/Workspace/Repos/prathameshbpatil8512@gmail.com/dev-sales/databricks/05_Gold_Transformation", 60)

dbutils.notebook.run("/Workspace/Repos/prathameshbpatil8512@gmail.com/dev-sales/databricks/06_PowerBI_Consumption", 60)
