# Databricks notebook source
dbutils.notebook.run("/Workspace/Repos/prathameshbpatil8512@gmail.com/dev-sales/databricks/notebooks/02_Config_Connection", 60)

dbutils.notebook.run("/Workspace/Repos/prathameshbpatil8512@gmail.com/dev-sales/databricks/notebooks/03_Bronze_Ingestion", 60)

dbutils.notebook.run("/Workspace/Repos/prathameshbpatil8512@gmail.com/dev-sales/databricks/notebooks/04_Silver_Transformation", 60)

dbutils.notebook.run("/Workspace/Repos/prathameshbpatil8512@gmail.com/dev-sales/databricks/notebooks/05_Gold_Transformation", 60)

dbutils.notebook.run("/Workspace/Repos/prathameshbpatil8512@gmail.com/dev-sales/databricks/notebooks/06_PowerBI_Consumption", 60)
