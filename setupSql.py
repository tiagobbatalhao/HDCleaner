#!/usr/bin/env python

import sqlite3
import os.path
import csv
import re,sys
from dateutil import parser as dateutilparser

_folder = os.path.abspath(os.path.dirname(__file__))
database_filename = _folder + '/FilesDatabase.sqlite'

def createTableFromDict(database,table_name,columns):
	"""
	Create a table from a dictionary containing column names and data types
	"""
	command = u'CREATE TABLE ' + str(table_name) + u' ( '
	command += u' , '.join([str(column) + u' ' + str(typ) for column,typ in columns.items()])
	command += u' ) ; '
	try:
		database.execute(command)
		database.commit()
	except sqlite3.OperationalError as exception:
		print(exception)

def updateFromDict(database,table_name,dictionary,condition_dictionary):
	dic = {}
	dic.update({u'set_'+x : y for x,y in dictionary.items() if y is not None})
	dic.update({u'con_'+x : y for x,y in condition_dictionary.items() if y is not None})
	setClause = u' , '.join([str(x[4:]) + u' = :' + str(x) for x,y in dic.items() if x[:3]=='set'])
	conClause = u' AND '.join([str(x[4:]) + u' = :' + str(x) for x,y in dic.items() if x[:3]=='con'])
	command = u'UPDATE ' + str(table_name) + u' SET ' + str(setClause) + u' WHERE ' + str(conClause)
	try:
		database.execute(command,dic)
		database.commit()
	except sqlite3.OperationalError as exception:
		print(exception)

def insertFromDict(database,table_name,dictionary):
	dic = {}
	dic.update({u'set_'+str(x) : y for x,y in dictionary.items() if y is not None})
	columns = u' , '.join([x[4:] for x in dic])
	values = u' , '.join([u':'+x for x in dic])
	command = u'INSERT INTO ' + str(table_name) + u' ( ' + str(columns) + u' ) VALUES ( ' + str(values) + u' );'
	try:
		database.execute(command,dic)
		database.commit()
	except sqlite3.OperationalError as exception:
		print(exception)

def _createTableFiles(database,table_name = u'papers'):
	columns = {}
	columns[u'fileName'] = u'TEXT PRIMARY KEY'
	columns[u'fileSize'] = u'INTEGER'
	columns[u'hashMD5'] = u'VARCHAR(32)'
	createTableFromDict(database,table_name,columns)

def setupSql(filename = None):
	"""
	Sets up a database, should be called only once
	"""
	if filename is None:
		filename = database_filename
	if os.path.isfile(filename):
		os.remove(filename)
	database = sqlite3.connect(filename)
	_createTableFiles(database,'master')
	_createTableFiles(database,'slave')
	return database

def openDatabase(filename = None):
	"""
	Return the standard database object
	"""
	if filename is None:
		filename = database_filename
	database = sqlite3.Connection(filename)
	return database
