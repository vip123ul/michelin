# Databricks notebook source
dbutils.fs.mount(
  source = "wasbs://raw@shivamdatabricks.blob.core.windows.net",
  mount_point = "/mnt/shivamdatabricks/raw",
  extra_configs = {"fs.azure.account.key.shivamdatabricks.blob.core.windows.net":"Key"})
