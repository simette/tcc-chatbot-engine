# Databricks notebook source
# MAGIC %sql
# MAGIC create catalog if not exists tcc_workspace_catalog;
# MAGIC
# MAGIC create schema if not exists tcc_workspace_catalog.raw_inss_crawled_data;
# MAGIC create table if not exists tcc_workspace_catalog.raw_inss_crawled_data.direitos_aposentadoria (
# MAGIC     retirement_type string,
# MAGIC     page_url string,
# MAGIC     html_page_content string,
# MAGIC     scraped_at timestamp,
# MAGIC     last_update_on_inss_site timestamp
# MAGIC )
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC create schema if not exists tcc_workspace_catalog.inss_crawled_data;
# MAGIC create table if not exists tcc_workspace_catalog.inss_crawled_data.direitos_aposentadoria (
# MAGIC     retirement_type string,
# MAGIC     page_url string,
# MAGIC     html_page_content string,
# MAGIC     scraped_at timestamp,
# MAGIC     last_update_on_inss_site timestamp
# MAGIC )
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
