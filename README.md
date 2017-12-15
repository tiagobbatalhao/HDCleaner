# HDCleaner
Looks for duplicate files in a hard drive / SSD.

Usage example
```
>>> master = ['~/Documents','~/Desktop']
>>> slave = ['~/Downloads']
>>> database = setupDatabase()
>>> files = findFiles(master)
>>> insertFilesToDatabase(database,'master',files)
>>> files = findFiles(slave)
>>> insertFilesToDatabase(database,'slave',files)
>>> getFileInformation(database)
>>> issues = findIssues_MasterSlave(database)
```
