# Databricks notebook source
from random import randint

rows = [{'id': 0, 'name': 'fixed'}, {'id': randint(1, 10), 'name': 'random'}]
print("Created `rows`")