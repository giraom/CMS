
--setup is very simple
select * from Environment
select * from Purpose
select PurposeName
		,EnvironmentName
		,ServerName
		,BlockedProcessEvents
		,DeadlockEvents
		,ErrorEvents
		,LongQueryEvents
		,BackupFolder
		,IsActive 
	from vwServers

exec spLoad
exec spLoadIndexFragmentation

--Environment data
select * from vwClusterNodes
select * from [dbo].[AvailabilityGroups]

select * from vwServers
select * from vwVolumes
select * from vwLogins
select * from vwServices

--Databases
select * from vwDatabases
select * from vwDatabaseFiles

--Schema
select * from vwDatabaseObjects
select * from vwDatabaseObjectColumns
--select * from [dbo].[vwSequenses]

--Performance
select * from vwMissingIndexes
select * from vwIndexUsage order by size_mbs desc
select * from vwIndexUsage order by reads desc
select * from vwIndexUsage order by writes desc
select * from vwTopSql order by servername, TotalWorkerTime desc
select * from vwTopWait order by Servername, percent_total_waits desc
select * from vwQueryStoreLongestAvgTimes order by avg_duration desc
select * from vwQueryStoreTopExecCounts order by total_execution_count desc

--Jobs
select * from vwJobs
select * from vwJobSteps
select * from vwJobHistory

--Backups
select * from vwBackups
select * from vwBackupReport

--Extended Events
select * from vwErrors
select * from vwDeadlocks
select * from vwLongSql

select * from RplImportLog

/*
Utilities:
--Index Maintenance
	select Maintenance, ServerName
		, DatabaseName
		, TableName
		, IndexName
		, index_type_desc
		, avg_fragmentation_in_percent
		, fragment_count
		, page_count
		, SchemaName
		, RowCount_S
		, index_type
		, is_unique
		, size_mbs
		, writes
		, reads
		, fill_factor
		, cols
		, included
		, filter_definition
		, object_type
	from [vwIndexFragmentation]
	where size_mbs>10 
	and avg_fragmentation_in_percent > 0.1
	order by avg_fragmentation_in_percent desc

--History Tracking
	select * from [dbo].[ServersHist]  order by servername, date desc
	select * from [dbo].[VolumesHist]  order by servername, volume_mount_point date desc
	select * from [dbo].[DatabasesHist]  order by servername, databasename, date desc
	select * from [dbo].[DatabaseFilesHist] order by servername, databasename, filename, date desc

•	spDailyChecks
Runs dozens of routine checks, worth to check it every morning.

•	spHourlyChecks
Alerts which require prompt attention

•	spSearch 'FactProductInventory'
This may be used to find where tables, routines and columns exist.

•	[dbo].[spJobStop]
Finds all jobs that start with a given name, stops and disables them. Useful to manage replication or other processes.

•	 [dbo].[spJobStart]
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
exec spSearchDatabase 'AdventureWorksDW2019'

•	spRunSql
Runs a command on servers matching some criteria
--get all server admin logins
exec [dbo].[spRunSql] @cmd='select * from master..syslogins where sysadmin=1', @where='servername like ''%vm%'''

•	spRunSqlDb
Runs a command on databases matching some criteria
--where are the gabriels?
exec [spRunSqlDb] @cmd='select [BusinessEntityID], [PersonType], [FirstName], [LastName] from [#database#].person.person where firstname=''gabriel''', @where='Databasename like ''%adventureworks201_%'''
exec [spRunSqlDb] @cmd='select * from [#database#].dbo.DimCustomer where firstname=''gabriel''', @where='Databasename like ''%adventureworksDW201_%'''

--Review existing indexes
select * from vwIndexUsage where table_name='Person' order by servername, databasename, index_name

--	Missing Indexes
	exec spLoadMissingIndexes
	select * from vwMissingIndexes
--farm level analysis
	select distinct SchemaOnly, TableOnly, equality_columns
		, sum(unique_compiles) unique_compiles
		, sum(user_seeks) user_seeks
		, count(distinct servername) servers
		, count(distinct servername+'.'+databasename) databases
	from vwMissingIndexes
	group by SchemaOnly, TableOnly, equality_columns

•	Masking
--Which columnns are masked, switch to mSync demo
SELECT * FROM vwDatabaseObjectColumns
where masking_function is not null

--Which columns are missing
select * from [vwMissingMask]
where PurposeName <> 'prod'

•	Compression candidates
select * from vwIndexUSage
where size_mbs > 1
and reads < 100
and isnull(data_compression_desc,'none') = 'none'

*/

	