# Databricks notebook source

env = dbutils.widgets.get("env")
schema_bronze = dbutils.widgets.get("schema_bronze")
schema_silver = dbutils.widgets.get("schema_silver")
schema_gold = dbutils.widgets.get("schema_gold")
schema_quarantine = dbutils.widgets.get("schema_quarantine")

def create_catalog(env):
    spark.sql(f"create catalog if not exists sdp_catalog_{env}")

create_catalog(env)

def create_schema(env, schema_name):
    spark.sql(f"use catalog sdp_catalog_{env}")
    spark.sql(f"create schema if not exists {schema_name}")

schema_names = [schema_bronze, schema_silver, schema_gold, schema_quarantine]
for schema in schema_names:
    create_schema(env, schema)