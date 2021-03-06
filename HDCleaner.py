import os,os.path # File-manipulation functions
import glob # File-manipulation functions
import sys # File-manipulation functions
import shutil # File-manipulation functions
import filecmp # File-manipulation functions
import hashlib # Evaluate hash-functions

import setupSql as setupSql

def _findFiles(folder, method = 'walk'):
    """
    List files in one directory,
    including its subfolders
    """
    if folder == None or folder == '':
        folder = os.getcwd()
    assert type(folder) == type(''), "'folder' must be a string."
    # Remove a trailing slash, if one is present
    folder = folder.rstrip('/')
    if len(folder) > 1 and folder[0] == '~':
        home = os.path.expanduser('~')
        folder = home + folder[1:]
    list_files = []
    if method == 'glob':
        list_glob = glob.glob(folder + '/*')
        for item in list_glob:
            if os.path.isfile(item):
                list_files.append(item)
            elif os.path.isdir(item):
                list_files += _findFiles(item)
    elif method == 'walk':
        for root,dirs,files in os.walk(folder):
            for fl in files:
                path = root + '/' + fl
                if os.path.isfile(path):
                    list_files.append(path)
    return list_files

def findFiles(folders, method = 'walk'):
    """
    List files in several directories,
    including its subfolders
    """
    list_files = []
    if type(folders) == type(''):
        list_files += _findFiles(folders,method)
    elif hasattr(folders,'__iter__'):
        for folder in folders:
            list_files += findFiles(folder)
    return list_files

def hashFile(filename, method = 'md5', blockSize = 65536):
    """
    Hash a file
    """
    hashFunction = eval('hashlib.'+method+'()')
    fileObject = open(filename,'rb')
    buff = fileObject.read(blockSize)
    while len(buff):
        hashFunction.update(buff)
        buff = fileObject.read(blockSize)
    fileObject.close()
    return hashFunction.hexdigest()

def insertFilesToDatabase(database,table,list_files):
    """
    Save a list of files to a sqlite database
    """
    for filename in list_files:
        # filename = unicode(filename,'utf-8')
        setupSql.insertFromDict(database,table,{'fileName':filename})

def getFileInformation(database, onlyNull = True, hashing = True):
    """
    Populate the database with file size and hash
    """
    for table in ['master','slave']:
        command = u""" SELECT fileName FROM {} """.format(table)
        if onlyNull:
            command += u""" WHERE fileSize IS NULL """
        command += u""" ; """
        execution = database.execute(command)
        while True:
            fl = execution.fetchone()
            if fl is None:
                break
            try:
                size = os.stat(fl[0]).st_size
                if hashing:
                    md5 = hashFile(fl[0])
                    update = {'fileSize':size,'hashMD5':md5}
                else:
                    update = {'fileSize':size}
                condition = {'fileName':fl[0]}
                setupSql.updateFromDict(database,table,update,condition)
            except FileNotFoundError:
                pass

def openDatabase(filename = None):
    """
    Return the current database
    """
    return setupSql.openDatabase(filename)


def setupDatabase(filename = None):
    """
    Set up a clean database
    """
    return setupSql.setupSql(filename)

def findIssues_Master(database):
    """
    Find sets of repeated files in the master table
    """
    return _findIssues_Same(database,'master')
def findIssues_Slave(database):
    """
    Find sets of repeated files in the slave table
    """
    return _findIssues_Same(database,'slave')


def _findIssues_Same(database, table='master'):
    # query = u"""
    # 		SELECT DISTINCT t1.fileName, t2.filename, t1.fileSize
    # 		FROM master t1 INNER JOIN master t2
    # 		ON t1.hashMD5 = t2.hashMD5
    # 		AND t1.fileName < t2.fileName
    # 		;"""
    query = u"""
            SELECT hashMD5,COUNT(hashMD5),fileSize
            FROM {}
            GROUP BY hashMD5
            HAVING( COUNT(hashMD5) > 1)
            ORDER BY fileSize DESC
            ;""".format(table)
    execute = database.execute(query)
    results = execute.fetchall()
    issues = []
    try:
        for result in results:
            query = u"""
            		SELECT fileName,fileSize
                    FROM {}
                    WHERE hashMD5 = :hashMD5
                    ;""".format(table)
            res = database.execute(query,{'hashMD5':result[0]})
            issues.append(res.fetchall())
    except KeyboardInterrupt:
        pass
    return issues

def findIssues_MasterSlave(database):
    """
    Find files in the slave table that are
    repeated in the master table
    """
    query = u"""
    		SELECT t2.fileName, t1.filename, t2.fileSize
    		FROM slave t2 INNER JOIN master t1
    		ON t1.hashMD5 = t2.hashMD5
            AND t1.filename <> t2.fileName
            GROUP BY t2.filename
            ORDER BY t2.fileSize DESC
    		;"""
    results = database.execute(query).fetchall()
    return results

def delete(filename):
    """
    Try to delete a file.
    """
    try:
        os.remove(filename)
        return True
    except FileNotFoundError:
        return False

def main(master,slave):
	if False:
		database = setupDatabase()
		fls = findFiles(master)
		print('\nFound {} files on master'.format(len(fls)))
		insertFilesToDatabase(database,'master',fls)
		print('\nInserted to database')
		fls = findFiles(slave)
		print('\nFound {} files on slave'.format(len(fls)))
		insertFilesToDatabase(database,'slave',fls)
		print('\nInserted to database')
	else:
		database = openDatabase()
	getFileInformation(database)
	print('\nHashed files.')
	issues = findIssues_MasterSlave(database)
	print('\nFound {} issues'.format(len(issues)))
	savedSpace = 0
	try:
		for issue in issues:
			if delete(issue[0]):
				savedSpace += issue[2]
				print('Deleted: {}'.format(issue[0]))
	except KeyboardInterrupt:
		pass
	return savedSpace


def removeEmptyFolders(path):
    if not os.path.isdir(path):
        return
    # remove empty subfolders
    try:
        files = os.listdir(path)
        if len(files):
            for f in files:
                fullpath = os.path.join(path, f)
                if os.path.isdir(fullpath):
                    removeEmptyFolders(fullpath)
        # if folder empty, delete it
        files = os.listdir(path)
        if len(files) == 0:
            print('Removing empty folder: {}'.format(path))
            os.rmdir(path)
    except OSError:
        pass








def handleIssueForHD(issues):
	savedSpace = 0
	for issue in issues:
		toKeep = [x for x in issue if '/Consolidados/' in x[0]]
		toDelete = [x for x in issue if '/Consolidados/' not in x[0]]
		toDelete.sort(key = lambda x: x[0])
		if len(toKeep)==0:
			toKeep.append(toDelete.pop(0))
		for name,size in toDelete:
			if delete(name):
				savedSpace += size
				print('Deleted: {}'.format(name))

