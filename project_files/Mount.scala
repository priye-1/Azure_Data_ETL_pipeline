// Databricks notebook source
val containerName = "landing"
val storageAccountName = "storageAccountName"
val sas = "SasStringGeneratedFRomAzureStorageAccount"
val url = "wasbs://" + containerName + "@" + storageAccountName + ".blob.core.windows.net/"
var config = "fs.azure.sas." + containerName + "." + storageAccountName + ".blob.core.windows.net"

// COMMAND ----------

// COMMAND ----------

dbutils.fs.mount(
    source = url,
    mountPoint = "/mnt/landing",
    extraConfigs = Map(config -> sas)
)

// COMMAND ----------

val containerName = "archive"
val storageAccountName = "storageAccountName"
val sas = "SasStringGeneratedFRomAzureStorageAccount"
val url = "wasbs://" + containerName + "@" + storageAccountName + ".blob.core.windows.net/"
var config = "fs.azure.sas." + containerName + "." + storageAccountName + ".blob.core.windows.net"

// COMMAND ----------

// COMMAND ----------

dbutils.fs.mount(
    source = url,
    mountPoint = "/mnt/archive",
    extraConfigs = Map(config -> sas)
)

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE DATABASE IF NOT EXISTS DATAWAREHOUSEDB;

// COMMAND ----------


