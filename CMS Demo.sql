

select * from Environment
select * from Purpose
select PurposeName
		,EnvironmentName
		,ServerName
		,BlockedProcessEvents
		,DeadlockEvents
		,ErrorEvents
		,BackupFolder
		,IsActive 
	from vwServers

exec spLoad

--Environment data
--select * from vwClusterNodes
select * from vwServers
select * from vwDatabases
select * from vwDatabaseFiles

--Schema
select * from vwDatabaseObjects
select * from vwDatabaseObjectColumns
select * from [dbo].[vwSequenses]

--Performance
select * from vwMissingIndexes
select * from vwIndexUsage order by size_mbs desc
select * from vwTopSql
select * from vwTopWait

--Audit
select * from vwLogins
select * from vwServices
select * from vwVolumes

--Jobs
select * from vwJobs
select * from vwJobSteps
select * from vwJobErrors

--Backups
select * from vwBackups
select * from vwBackupReport

--Extended Events
select * from vwErrors
select * from vwDeadlocks
select * from vwLongSql




/*
Utilities:
•	spDailyChecks
Runs dozens of routine checks, worth to check it every morning.

•	spHourlyChecks
Alerts which require prompt attention

•	spSearch 'FactProductInventory'
This may be used to find where tables, routines and columns exist.

•	spStopJob
Finds all jobs that start with a given name, stops and disables them. Useful to manage replication or other processes.

•	spJobStart 
Re enables jobs, but does not start them automatically.

•	spAddColumn
One of the most common needs is to add columns to a table, this utility finds all databases and servers where a given table exists and adds the column. 
If the column is already there it will skip that database. We recommend using params @Debug=1, @Exec=0 to get a printed output, so you can run commands arbitrarily.
With some creativity this proc can also be used to change data types, add constraints, etc.

exec spAddColumn @SchemaName='Person', @TableName='PersonPhone', @ColumnName='ChangedBy',@Type='varchar(100)',@Debug=1,@Exec=0


•	spUpdatePublisher / spUpdateSubscriper 
Allows executing commands on all publisher / subscriber databases at once, however this does not support long scripts separated by GO statements.

exec spUpdatePublisher @code='create or alter view vPersonPhone as select * from Person.PersonPhone', @Debug=1, @Exec=0

•	spDbCompare
Compares 2 databases and finds many sorts of differences. And runs a lot faster than Redgate Sql Compare.

exec spDbCompare @sourceserver='sqlvm1', @sourcedb='AdventureWorks2019Prod', @targetserver = 'sqlvm2', @targetdb='AdventureWorks2014QA'

•	Diagnostics 
exec spSearchServer 'sqlvm1'
exec spSearchDatabase 'AdventureWorks2019'

•	spRunSql
Runs a command on servers matching some criteria
--get all server admin logins
exec [dbo].[spRunSql] @cmd='select * from master..syslogins where sysadmin=1', @where='servername like ''%vm%'''

•	spRunSqlDb
Runs a command on databases matching some criteria
--where are the gabriels?
exec [spRunSqlDb] @cmd='select [BusinessEntityID], [PersonType], [FirstName], [LastName] from [#database#].person.person where firstname=''gabriel''', @where='Databasename like ''%adventureworks201_%'''


•	Masking
--Which columnns are masked
SELECT * FROM vwDatabaseObjectColumns
where masking_function is not null

--Which columns are missing
select * from [vwMissingMask]
where PurposeName <> 'prod'

•	Compression candidates
select * from vwIndexUSage
where size_mbs > 10
and reads < 10
and isnull(data_compression_desc,'none') = 'none'

*/

	