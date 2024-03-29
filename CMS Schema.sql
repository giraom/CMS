/*
WARNING!
THIS SCRIPT DROPS ALL TABLES AND ROUTINES FROM THE CURRENT DATABASE!
*/

USE CMS
GO

if object_id('dbo.spPrintLongSql') is not null
drop proc dbo.spPrintLongSql
go
CREATE PROCEDURE [dbo].[spPrintLongSql]( @string nvarchar(max) )
AS
SET NOCOUNT ON
 
set @string = rtrim( @string )
 
declare @cr char(1), @lf char(1)
set @cr = char(13)
set @lf = char(10)
 
declare @len int, @cr_index int, @lf_index int, @crlf_index int, @has_cr_and_lf bit, @left nvarchar(4000), @reverse nvarchar(4000)
set @len = 4000
 
while ( len( @string ) > @len )
begin
   set @left = left( @string, @len )
   set @reverse = reverse( @left )
   set @cr_index = @len - charindex( @cr, @reverse ) + 1
   set @lf_index = @len - charindex( @lf, @reverse ) + 1
   set @crlf_index = case when @cr_index < @lf_index then @cr_index else @lf_index end
   set @has_cr_and_lf = case when @cr_index < @len and @lf_index < @len then 1 else 0 end
   print left( @string, @crlf_index - 1 )
   set @string = right( @string, len( @string ) - @crlf_index - @has_cr_and_lf )
end
 
print @string
GO

if object_id('dbo.spDropAllObjects') is not null
drop proc dbo.spDropAllObjects
go
CREATE PROC dbo.spDropAllObjects 
as

/*xtypes
| AF    |  Aggregate function (CLR)             |
| C     |  CHECK constraint                     |
| D     |  Default or DEFAULT constraint        |
| F     |  FOREIGN KEY constraint               |
| L     |  Log                                  |
| FN    |  Scalar function                      |
| FS    |  Assembly (CLR) scalar-function       |
| FT    |  Assembly (CLR) table-valued function |
| IF    |  In-lined table-function              |
| IT    |  Internal table                       |
| P     |  Stored procedure                     |
| PC    |  Assembly (CLR) stored-procedure      |
| PK    |  PRIMARY KEY constraint (type is K)   |
| RF    |  Replication filter stored procedure  |
| S     |  System table                         |
| SN    |  Synonym                              |
| SQ    |  Service queue                        |
| TA    |  Assembly (CLR) DML trigger           |
| TF    |  Table function                       |
| TR    |  SQL DML Trigger                      |
| TT    |  Table type                           |
| U     |  User table                           |
| UQ    |  UNIQUE constraint (type is K)        |
| V     |  View                                 |
| X     |  Extended stored procedure			|
*/

declare @sql varchar(max)=''

--Foreign keys
select @sql = @sql + 'IF OBJECT_id ('''+SCHEMA_NAME(O.schema_id)+'.['+O.name+']'') IS NOT NULL alter table '+p.name +' drop constraint '+f.name +char(13)
	--SELECT SCHEMA_NAME(O.schema_id),  o.name, p.name, r.name, f.*
from [sys].[foreign_keys] f
join sys.objects o on f.object_id =  o.object_id
join sys.objects p on f.parent_object_id =  p.object_id
join sys.objects r on f.referenced_object_id =  r.object_id
where f.type='f'
--exec (@sql)
exec spPrintLongSql @sql

--Routines
set @sql=''
select @sql = @sql + 'IF OBJECT_id ('''+SCHEMA_NAME(O.uid)+'.['+O.name+']'') IS NOT NULL drop '+
	case	when xtype='v' then 'view'
			when xtype = 'SN' then 'synonym'
			when xtype = 'P' then 'procedure'
			when xtype in ('FN','FS','FT','IF','TF') then 'function'
	end
+' dbo.['+name +']'+char(13)
--select *
from sysobjects o 
where xtype in ('FN','FS','FT','IF','TF','v','SN','P')
order by xtype, uid, name
--exec (@sql)
exec spPrintLongSql @sql

--Tables
set @sql=''
select @sql = @sql + 'IF OBJECT_id ('''+SCHEMA_NAME(O.uid)+'.['+O.name+']'') IS NOT NULL drop table dbo.['+name +']'+char(13)
--select *
from sysobjects o 
where name not in ('sysdiagrams') 
and xtype in ('u')
order by xtype, uid, name
--exec (@sql)
exec spPrintLongSql @sql

go

--exec dbo.spDropAllObjects

go
IF OBJECT_id ('dbo.[fk_Jobs_Servers]') IS NOT NULL alter table Jobs drop constraint fk_Jobs_ServersIF OBJECT_id ('dbo.[fk_Logins_Servers]') IS NOT NULL alter table Logins drop constraint fk_Logins_ServersIF OBJECT_id ('dbo.[fk_LongSql_Servers]') IS NOT NULL alter table LongSql drop constraint fk_LongSql_ServersIF OBJECT_id ('dbo.[fk_LongSqlFiles_Servers]') IS NOT NULL alter table LongSqlFiles drop constraint fk_LongSqlFiles_ServersIF OBJECT_id ('dbo.[fk_MissingIndexes_Servers]') IS NOT NULL alter table MissingIndexes drop constraint fk_MissingIndexes_ServersIF OBJECT_id ('dbo.[fk_Msdb_Backups_Servers]') IS NOT NULL alter table Msdb_Backups drop constraint fk_Msdb_Backups_ServersIF OBJECT_id ('dbo.[fk_QueryStoreLongestAvgTimes_Servers]') IS NOT NULL alter table QueryStoreLongestAvgTimes drop constraint fk_QueryStoreLongestAvgTimes_ServersIF OBJECT_id ('dbo.[fk_QueryStoreTopExecCounts_Servers]') IS NOT NULL alter table QueryStoreTopExecCounts drop constraint fk_QueryStoreTopExecCounts_ServersIF OBJECT_id ('dbo.[fk_RplDates_Servers]') IS NOT NULL alter table RplDates drop constraint fk_RplDates_ServersIF OBJECT_id ('dbo.[fk_RplImportLog_Servers]') IS NOT NULL alter table RplImportLog drop constraint fk_RplImportLog_ServersIF OBJECT_id ('dbo.[fk_RplImportLogDetail_Servers]') IS NOT NULL alter table RplImportLogDetail drop constraint fk_RplImportLogDetail_ServersIF OBJECT_id ('dbo.[fk_RplPublicationTable_Servers]') IS NOT NULL alter table RplPublicationTable drop constraint fk_RplPublicationTable_ServersIF OBJECT_id ('dbo.[fk_RplSubscription_Servers]') IS NOT NULL alter table RplSubscription drop constraint fk_RplSubscription_ServersIF OBJECT_id ('dbo.[fk_RplSubscriptionRoutine_Servers]') IS NOT NULL alter table RplSubscriptionRoutine drop constraint fk_RplSubscriptionRoutine_ServersIF OBJECT_id ('dbo.[fk_RplSubscriptionTable_Servers]') IS NOT NULL alter table RplSubscriptionTable drop constraint fk_RplSubscriptionTable_ServersIF OBJECT_id ('dbo.[fk_Sequences_Servers]') IS NOT NULL alter table Sequences drop constraint fk_Sequences_ServersIF OBJECT_id ('dbo.[fk_Services_Servers]') IS NOT NULL alter table Services drop constraint fk_Services_ServersIF OBJECT_id ('dbo.[fk_SQLAzure_Servers]') IS NOT NULL alter table SQLAzure drop constraint fk_SQLAzure_ServersIF OBJECT_id ('dbo.[fk_SubscriptionPendingCommands_Servers]') IS NOT NULL alter table SubscriptionPendingCommands drop constraint fk_SubscriptionPendingCommands_ServersIF OBJECT_id ('dbo.[fk_Subscriptions_Servers]') IS NOT NULL alter table Subscriptions drop constraint fk_Subscriptions_ServersIF OBJECT_id ('dbo.[fk_TopSql_Servers]') IS NOT NULL alter table TopSql drop constraint fk_TopSql_ServersIF OBJECT_id ('dbo.[fk_TopWait_Servers]') IS NOT NULL alter table TopWait drop constraint fk_TopWait_ServersIF OBJECT_id ('dbo.[fk_Volumes_Servers]') IS NOT NULL alter table Volumes drop constraint fk_Volumes_ServersIF OBJECT_id ('dbo.[fk_Admin_DatabasesNotToBackup_Servers]') IS NOT NULL alter table Admin_DatabasesNotToBackup drop constraint fk_Admin_DatabasesNotToBackup_ServersIF OBJECT_id ('dbo.[fk_ApplicationServers_Server]') IS NOT NULL alter table ApplicationServers drop constraint fk_ApplicationServers_ServerIF OBJECT_id ('dbo.[fk_Articles_Servers]') IS NOT NULL alter table Articles drop constraint fk_Articles_ServersIF OBJECT_id ('dbo.[fk_AvailabilityGroups_Servers]') IS NOT NULL alter table AvailabilityGroups drop constraint fk_AvailabilityGroups_ServersIF OBJECT_id ('dbo.[fk_ClusterNodes_Servers]') IS NOT NULL alter table ClusterNodes drop constraint fk_ClusterNodes_ServersIF OBJECT_id ('dbo.[fk_DatabaseFiles_Servers]') IS NOT NULL alter table DatabaseFiles drop constraint fk_DatabaseFiles_ServersIF OBJECT_id ('dbo.[fk_DatabaseObjectColumns_Servers]') IS NOT NULL alter table DatabaseObjectColumns drop constraint fk_DatabaseObjectColumns_Servers
IF OBJECT_id ('dbo.[fk_DatabaseObjectPerms_Servers]') IS NOT NULL alter table DatabaseObjectPerms drop constraint fk_DatabaseObjectPerms_ServersIF OBJECT_id ('dbo.[fk_DatabaseObjects_Servers]') IS NOT NULL alter table DatabaseObjects drop constraint fk_DatabaseObjects_ServersIF OBJECT_id ('dbo.[fk_DatabasePerms_Servers]') IS NOT NULL alter table DatabasePerms drop constraint fk_DatabasePerms_ServersIF OBJECT_id ('dbo.[fk_Databases_Servers]') IS NOT NULL alter table Databases drop constraint fk_Databases_ServersIF OBJECT_id ('dbo.[fk_DeadLockFiles_Servers]') IS NOT NULL alter table DeadLockFiles drop constraint fk_DeadLockFiles_ServersIF OBJECT_id ('dbo.[fk_Deadlocks_Servers]') IS NOT NULL alter table Deadlocks drop constraint fk_Deadlocks_ServersIF OBJECT_id ('dbo.[fk_ErrorFiles_Servers]') IS NOT NULL alter table ErrorFiles drop constraint fk_ErrorFiles_ServersIF OBJECT_id ('dbo.[fk_Errors_Servers]') IS NOT NULL alter table Errors drop constraint fk_Errors_ServersIF OBJECT_id ('dbo.[fk_IndexFragmentation_Servers]') IS NOT NULL alter table IndexFragmentation drop constraint fk_IndexFragmentation_ServersIF OBJECT_id ('dbo.[fk_IndexUsage_Servers]') IS NOT NULL alter table IndexUsage drop constraint fk_IndexUsage_ServersIF OBJECT_id ('dbo.[FK_Servers_Environment]') IS NOT NULL alter table Servers drop constraint FK_Servers_EnvironmentIF OBJECT_id ('dbo.[fk_Servers_EnvironmentId]') IS NOT NULL alter table Servers drop constraint fk_Servers_EnvironmentIdIF OBJECT_id ('dbo.[fk_SecurityGroups_Purpose]') IS NOT NULL alter table SecurityGroups drop constraint fk_SecurityGroups_PurposeIF OBJECT_id ('dbo.[fk_servers_purpose]') IS NOT NULL alter table Servers drop constraint fk_servers_purposeIF OBJECT_id ('dbo.[fk_ApplicationServer_Purpose]') IS NOT NULL alter table ApplicationServers drop constraint fk_ApplicationServer_PurposeIF OBJECT_id ('dbo.[fk_JobSteps_JobId]') IS NOT NULL alter table JobSteps drop constraint fk_JobSteps_JobIdIF OBJECT_id ('dbo.[FK_JobSteps_Jobs]') IS NOT NULL alter table JobSteps drop constraint FK_JobSteps_JobsIF OBJECT_id ('dbo.[FK_JobErrors_Jobs]') IS NOT NULL alter table JobHistory drop constraint FK_JobErrors_JobsIF OBJECT_id ('dbo.[fk_Subscriptions_ArticleId]') IS NOT NULL alter table Subscriptions drop constraint fk_Subscriptions_ArticleIdIF OBJECT_id ('dbo.[fk_DatabaseObjectColumns_DatabaseObjects]') IS NOT NULL alter table DatabaseObjectColumns drop constraint fk_DatabaseObjectColumns_DatabaseObjectsIF OBJECT_id ('dbo.[fk_IndexUsage_DatabaseObjects]') IS NOT NULL alter table IndexUsage drop constraint fk_IndexUsage_DatabaseObjectsIF OBJECT_id ('dbo.[fk_MissingIndexes_Databases]') IS NOT NULL alter table MissingIndexes drop constraint fk_MissingIndexes_DatabasesIF OBJECT_id ('dbo.[fk_QueryStoreLongestAvgTimes_Databases]') IS NOT NULL alter table QueryStoreLongestAvgTimes drop constraint fk_QueryStoreLongestAvgTimes_DatabasesIF OBJECT_id ('dbo.[fk_QueryStoreTopExecCounts_Databases]') IS NOT NULL alter table QueryStoreTopExecCounts drop constraint fk_QueryStoreTopExecCounts_DatabasesIF OBJECT_id ('dbo.[fk_RplDates_Databases]') IS NOT NULL alter table RplDates drop constraint fk_RplDates_DatabasesIF OBJECT_id ('dbo.[fk_RplImportLog_Databases]') IS NOT NULL alter table RplImportLog drop constraint fk_RplImportLog_DatabasesIF OBJECT_id ('dbo.[fk_RplImportLogDetail_Databases]') IS NOT NULL alter table RplImportLogDetail drop constraint fk_RplImportLogDetail_DatabasesIF OBJECT_id ('dbo.[fk_RplPublicationTable_Databases]') IS NOT NULL alter table RplPublicationTable drop constraint fk_RplPublicationTable_DatabasesIF OBJECT_id ('dbo.[fk_RplSubscription_Databases]') IS NOT NULL alter table RplSubscription drop constraint fk_RplSubscription_DatabasesIF OBJECT_id ('dbo.[fk_RplSubscriptionRoutine_Databases]') IS NOT NULL alter table RplSubscriptionRoutine drop constraint fk_RplSubscriptionRoutine_Databases
IF OBJECT_id ('dbo.[fk_RplSubscriptionTable_Databases]') IS NOT NULL alter table RplSubscriptionTable drop constraint fk_RplSubscriptionTable_DatabasesIF OBJECT_id ('dbo.[fk_Sequences_Databases]') IS NOT NULL alter table Sequences drop constraint fk_Sequences_DatabasesIF OBJECT_id ('dbo.[fk_SQLAzure_Databases]') IS NOT NULL alter table SQLAzure drop constraint fk_SQLAzure_DatabasesIF OBJECT_id ('dbo.[fk_TopSql_Databases]') IS NOT NULL alter table TopSql drop constraint fk_TopSql_DatabasesIF OBJECT_id ('dbo.[fk_DatabaseFiles_Databases]') IS NOT NULL alter table DatabaseFiles drop constraint fk_DatabaseFiles_DatabasesIF OBJECT_id ('dbo.[fk_DatabaseObjectColumns_Databases]') IS NOT NULL alter table DatabaseObjectColumns drop constraint fk_DatabaseObjectColumns_DatabasesIF OBJECT_id ('dbo.[fk_DatabaseObjectPerms_Databases]') IS NOT NULL alter table DatabaseObjectPerms drop constraint fk_DatabaseObjectPerms_DatabasesIF OBJECT_id ('dbo.[fk_DatabaseObjects_Databases]') IS NOT NULL alter table DatabaseObjects drop constraint fk_DatabaseObjects_DatabasesIF OBJECT_id ('dbo.[fk_DatabasePerms_Databases]') IS NOT NULL alter table DatabasePerms drop constraint fk_DatabasePerms_DatabasesIF OBJECT_id ('dbo.[fk_IndexFragmentation_Databases]') IS NOT NULL alter table IndexFragmentation drop constraint fk_IndexFragmentation_DatabasesIF OBJECT_id ('dbo.[fk_IndexUsage_Databases]') IS NOT NULL alter table IndexUsage drop constraint fk_IndexUsage_DatabasesIF OBJECT_id ('dbo.[fk_SecurityGroups_Application]') IS NOT NULL alter table SecurityGroups drop constraint fk_SecurityGroups_ApplicationIF OBJECT_id ('dbo.[fk_ApplicationServers_Application]') IS NOT NULL alter table ApplicationServers drop constraint fk_ApplicationServers_ApplicationIF OBJECT_id ('dbo.[fk_DatabaseObjectPerms_LoginId]') IS NOT NULL alter table DatabaseObjectPerms drop constraint fk_DatabaseObjectPerms_LoginIdIF OBJECT_id ('dbo.[FK_DatabaseObjectPerms_Logins]') IS NOT NULL alter table DatabaseObjectPerms drop constraint FK_DatabaseObjectPerms_LoginsIF OBJECT_id ('dbo.[fk_SecurityGroups_SecurityType]') IS NOT NULL alter table SecurityGroups drop constraint fk_SecurityGroups_SecurityType
IF OBJECT_id ('dbo.[fnNullVal]') IS NOT NULL drop function dbo.[fnNullVal]IF OBJECT_id ('dbo.[InStringCount]') IS NOT NULL drop function dbo.[InStringCount]IF OBJECT_id ('dbo.[RegexMatch]') IS NOT NULL drop function dbo.[RegexMatch]IF OBJECT_id ('dbo.[udf_schedule_description]') IS NOT NULL drop function dbo.[udf_schedule_description]IF OBJECT_id ('dbo.[fJobHist]') IS NOT NULL drop function dbo.[fJobHist]IF OBJECT_id ('dbo.[fnPendingServers]') IS NOT NULL drop function dbo.[fnPendingServers]IF OBJECT_id ('dbo.[spAddColumn]') IS NOT NULL drop procedure dbo.[spAddColumn]IF OBJECT_id ('dbo.[spBackup]') IS NOT NULL drop procedure dbo.[spBackup]IF OBJECT_id ('dbo.[spCleanLogFiles]') IS NOT NULL drop procedure dbo.[spCleanLogFiles]IF OBJECT_id ('dbo.[spCleanOldData]') IS NOT NULL drop procedure dbo.[spCleanOldData]IF OBJECT_id ('dbo.[spCleanOldPerfMon]') IS NOT NULL drop procedure dbo.[spCleanOldPerfMon]IF OBJECT_id ('dbo.[spCleanup]') IS NOT NULL drop procedure dbo.[spCleanup]IF OBJECT_id ('dbo.[spCleanupServer]') IS NOT NULL drop procedure dbo.[spCleanupServer]IF OBJECT_id ('dbo.[spCompositeCacheSwap]') IS NOT NULL drop procedure dbo.[spCompositeCacheSwap]IF OBJECT_id ('dbo.[spCompositeCacheSwapPrep]') IS NOT NULL drop procedure dbo.[spCompositeCacheSwapPrep]IF OBJECT_id ('dbo.[spDailyChecks]') IS NOT NULL drop procedure dbo.[spDailyChecks]IF OBJECT_id ('dbo.[spDbCompare]') IS NOT NULL drop procedure dbo.[spDbCompare]IF OBJECT_id ('dbo.[spDisableConstraints]') IS NOT NULL drop procedure dbo.[spDisableConstraints]IF OBJECT_id ('dbo.[spDropAllObjects]') IS NOT NULL drop procedure dbo.[spDropAllObjects]IF OBJECT_id ('dbo.[spDS_PublisherDatabase]') IS NOT NULL drop procedure dbo.[spDS_PublisherDatabase]IF OBJECT_id ('dbo.[spDS_PublisherServer]') IS NOT NULL drop procedure dbo.[spDS_PublisherServer]IF OBJECT_id ('dbo.[spEnableConstraints]') IS NOT NULL drop procedure dbo.[spEnableConstraints]IF OBJECT_id ('dbo.[spExec]') IS NOT NULL drop procedure dbo.[spExec]IF OBJECT_id ('dbo.[spExecCommand]') IS NOT NULL drop procedure dbo.[spExecCommand]IF OBJECT_id ('dbo.[spExportDeadlocks]') IS NOT NULL drop procedure dbo.[spExportDeadlocks]IF OBJECT_id ('dbo.[spExportLongSql]') IS NOT NULL drop procedure dbo.[spExportLongSql]IF OBJECT_id ('dbo.[spHourlyChecks]') IS NOT NULL drop procedure dbo.[spHourlyChecks]IF OBJECT_id ('dbo.[spInventory]') IS NOT NULL drop procedure dbo.[spInventory]IF OBJECT_id ('dbo.[spj_MonitorDeadlocks]') IS NOT NULL drop procedure dbo.[spj_MonitorDeadlocks]IF OBJECT_id ('dbo.[spJob_Failure_Notification]') IS NOT NULL drop procedure dbo.[spJob_Failure_Notification]IF OBJECT_id ('dbo.[spJobStart]') IS NOT NULL drop procedure dbo.[spJobStart]IF OBJECT_id ('dbo.[spJobStop]') IS NOT NULL drop procedure dbo.[spJobStop]IF OBJECT_id ('dbo.[spLoad]') IS NOT NULL drop procedure dbo.[spLoad]IF OBJECT_id ('dbo.[spLoadADGroupMembers]') IS NOT NULL drop procedure dbo.[spLoadADGroupMembers]IF OBJECT_id ('dbo.[spLoadArticles]') IS NOT NULL drop procedure dbo.[spLoadArticles]IF OBJECT_id ('dbo.[spLoadAvailabilityGroups]') IS NOT NULL drop procedure dbo.[spLoadAvailabilityGroups]IF OBJECT_id ('dbo.[spLoadBackups]') IS NOT NULL drop procedure dbo.[spLoadBackups]IF OBJECT_id ('dbo.[spLoadClusterNodes]') IS NOT NULL drop procedure dbo.[spLoadClusterNodes]IF OBJECT_id ('dbo.[spLoadDataBaseFiles]') IS NOT NULL drop procedure dbo.[spLoadDataBaseFiles]IF OBJECT_id ('dbo.[spLoadDataBaseObjectColums]') IS NOT NULL drop procedure dbo.[spLoadDataBaseObjectColums]IF OBJECT_id ('dbo.[spLoadDataBaseObjectPerms]') IS NOT NULL drop procedure dbo.[spLoadDataBaseObjectPerms]IF OBJECT_id ('dbo.[spLoadDataBaseObjects]') IS NOT NULL drop procedure dbo.[spLoadDataBaseObjects]IF OBJECT_id ('dbo.[spLoadDataBasePerms]') IS NOT NULL drop procedure dbo.[spLoadDataBasePerms]IF OBJECT_id ('dbo.[spLoadDatabases]') IS NOT NULL drop procedure dbo.[spLoadDatabases]
IF OBJECT_id ('dbo.[spLoadDeadLockFiles]') IS NOT NULL drop procedure dbo.[spLoadDeadLockFiles]IF OBJECT_id ('dbo.[spLoadDeadlocks]') IS NOT NULL drop procedure dbo.[spLoadDeadlocks]IF OBJECT_id ('dbo.[spLoadErrorFiles]') IS NOT NULL drop procedure dbo.[spLoadErrorFiles]IF OBJECT_id ('dbo.[spLoadErrors]') IS NOT NULL drop procedure dbo.[spLoadErrors]IF OBJECT_id ('dbo.[spLoadHourly]') IS NOT NULL drop procedure dbo.[spLoadHourly]IF OBJECT_id ('dbo.[spLoadIndexFragmentation]') IS NOT NULL drop procedure dbo.[spLoadIndexFragmentation]IF OBJECT_id ('dbo.[spLoadIndexUsage]') IS NOT NULL drop procedure dbo.[spLoadIndexUsage]IF OBJECT_id ('dbo.[spLoadJobHistory]') IS NOT NULL drop procedure dbo.[spLoadJobHistory]IF OBJECT_id ('dbo.[spLoadJobs]') IS NOT NULL drop procedure dbo.[spLoadJobs]IF OBJECT_id ('dbo.[spLoadJobsRunning]') IS NOT NULL drop procedure dbo.[spLoadJobsRunning]IF OBJECT_id ('dbo.[spLoadJobSteps]') IS NOT NULL drop procedure dbo.[spLoadJobSteps]IF OBJECT_id ('dbo.[spLoadLinkedServers]') IS NOT NULL drop procedure dbo.[spLoadLinkedServers]IF OBJECT_id ('dbo.[spLoadLogins]') IS NOT NULL drop procedure dbo.[spLoadLogins]IF OBJECT_id ('dbo.[spLoadLongSql]') IS NOT NULL drop procedure dbo.[spLoadLongSql]IF OBJECT_id ('dbo.[spLoadLongSqlFiles]') IS NOT NULL drop procedure dbo.[spLoadLongSqlFiles]IF OBJECT_id ('dbo.[spLoadMissingIndexes]') IS NOT NULL drop procedure dbo.[spLoadMissingIndexes]IF OBJECT_id ('dbo.[spLoadMsdb_Backups]') IS NOT NULL drop procedure dbo.[spLoadMsdb_Backups]IF OBJECT_id ('dbo.[spLoadPerMon]') IS NOT NULL drop procedure dbo.[spLoadPerMon]IF OBJECT_id ('dbo.[spLoadPerMonApp]') IS NOT NULL drop procedure dbo.[spLoadPerMonApp]IF OBJECT_id ('dbo.[spLoadPublications]') IS NOT NULL drop procedure dbo.[spLoadPublications]IF OBJECT_id ('dbo.[spLoadPublishers]') IS NOT NULL drop procedure dbo.[spLoadPublishers]IF OBJECT_id ('dbo.[spLoadQueryStoreLongestAvgTimes]') IS NOT NULL drop procedure dbo.[spLoadQueryStoreLongestAvgTimes]IF OBJECT_id ('dbo.[spLoadQueryStoreTopExecCounts]') IS NOT NULL drop procedure dbo.[spLoadQueryStoreTopExecCounts]IF OBJECT_id ('dbo.[spLoadRplDates]') IS NOT NULL drop procedure dbo.[spLoadRplDates]IF OBJECT_id ('dbo.[spLoadRplImportLog]') IS NOT NULL drop procedure dbo.[spLoadRplImportLog]IF OBJECT_id ('dbo.[spLoadRplImportLogDetail]') IS NOT NULL drop procedure dbo.[spLoadRplImportLogDetail]IF OBJECT_id ('dbo.[spLoadRplPublicationTable]') IS NOT NULL drop procedure dbo.[spLoadRplPublicationTable]IF OBJECT_id ('dbo.[spLoadRplSubscription]') IS NOT NULL drop procedure dbo.[spLoadRplSubscription]IF OBJECT_id ('dbo.[spLoadRplSubscriptionRoutine]') IS NOT NULL drop procedure dbo.[spLoadRplSubscriptionRoutine]IF OBJECT_id ('dbo.[spLoadRplSubscriptionTable]') IS NOT NULL drop procedure dbo.[spLoadRplSubscriptionTable]IF OBJECT_id ('dbo.[spLoadSequences]') IS NOT NULL drop procedure dbo.[spLoadSequences]IF OBJECT_id ('dbo.[spLoadServerperms]') IS NOT NULL drop procedure dbo.[spLoadServerperms]IF OBJECT_id ('dbo.[spLoadServers]') IS NOT NULL drop procedure dbo.[spLoadServers]IF OBJECT_id ('dbo.[spLoadServices]') IS NOT NULL drop procedure dbo.[spLoadServices]IF OBJECT_id ('dbo.[spLoadSQLAzure]') IS NOT NULL drop procedure dbo.[spLoadSQLAzure]IF OBJECT_id ('dbo.[spLoadSubscriptions]') IS NOT NULL drop procedure dbo.[spLoadSubscriptions]IF OBJECT_id ('dbo.[spLoadTempConnections]') IS NOT NULL drop procedure dbo.[spLoadTempConnections]IF OBJECT_id ('dbo.[spLoadTopSql]') IS NOT NULL drop procedure dbo.[spLoadTopSql]IF OBJECT_id ('dbo.[spLoadTopWait]') IS NOT NULL drop procedure dbo.[spLoadTopWait]IF OBJECT_id ('dbo.[spLoadVolumes]') IS NOT NULL drop procedure dbo.[spLoadVolumes]IF OBJECT_id ('dbo.[spLogmanCreate]') IS NOT NULL drop procedure dbo.[spLogmanCreate]IF OBJECT_id ('dbo.[spPrintLongSql]') IS NOT NULL drop procedure dbo.[spPrintLongSql]IF OBJECT_id ('dbo.[spPurgeAudit]') IS NOT NULL drop procedure dbo.[spPurgeAudit]
IF OBJECT_id ('dbo.[spPurgeDates]') IS NOT NULL drop procedure dbo.[spPurgeDates]IF OBJECT_id ('dbo.[spPurgeDeadLocks]') IS NOT NULL drop procedure dbo.[spPurgeDeadLocks]IF OBJECT_id ('dbo.[spPurgeErrors]') IS NOT NULL drop procedure dbo.[spPurgeErrors]IF OBJECT_id ('dbo.[spPurgeLongSql]') IS NOT NULL drop procedure dbo.[spPurgeLongSql]IF OBJECT_id ('dbo.[spPurgeRplDeleteTables]') IS NOT NULL drop procedure dbo.[spPurgeRplDeleteTables]IF OBJECT_id ('dbo.[spPurgeRplImportLog]') IS NOT NULL drop procedure dbo.[spPurgeRplImportLog]IF OBJECT_id ('dbo.[spReplicationCheck]') IS NOT NULL drop procedure dbo.[spReplicationCheck]IF OBJECT_id ('dbo.[spReplicationCheckDetail]') IS NOT NULL drop procedure dbo.[spReplicationCheckDetail]IF OBJECT_id ('dbo.[spReportDeadLock]') IS NOT NULL drop procedure dbo.[spReportDeadLock]IF OBJECT_id ('dbo.[spReportDeadLockDbSummary]') IS NOT NULL drop procedure dbo.[spReportDeadLockDbSummary]IF OBJECT_id ('dbo.[spReportDeadLockSummary]') IS NOT NULL drop procedure dbo.[spReportDeadLockSummary]IF OBJECT_id ('dbo.[spReportDisableServer]') IS NOT NULL drop procedure dbo.[spReportDisableServer]IF OBJECT_id ('dbo.[spReportPerfMon]') IS NOT NULL drop procedure dbo.[spReportPerfMon]IF OBJECT_id ('dbo.[spReportPerfMonApp]') IS NOT NULL drop procedure dbo.[spReportPerfMonApp]IF OBJECT_id ('dbo.[spReportPerfMonServerSummary]') IS NOT NULL drop procedure dbo.[spReportPerfMonServerSummary]IF OBJECT_id ('dbo.[spReportReplicationVolume]') IS NOT NULL drop procedure dbo.[spReportReplicationVolume]IF OBJECT_id ('dbo.[spRunSql]') IS NOT NULL drop procedure dbo.[spRunSql]IF OBJECT_id ('dbo.[spRunSqlDb]') IS NOT NULL drop procedure dbo.[spRunSqlDb]IF OBJECT_id ('dbo.[spSaveHistTables]') IS NOT NULL drop procedure dbo.[spSaveHistTables]IF OBJECT_id ('dbo.[spSearch]') IS NOT NULL drop procedure dbo.[spSearch]IF OBJECT_id ('dbo.[spSearchADGroup]') IS NOT NULL drop procedure dbo.[spSearchADGroup]IF OBJECT_id ('dbo.[spSearchDatabase]') IS NOT NULL drop procedure dbo.[spSearchDatabase]IF OBJECT_id ('dbo.[spSearchLogin]') IS NOT NULL drop procedure dbo.[spSearchLogin]IF OBJECT_id ('dbo.[spSearchServer]') IS NOT NULL drop procedure dbo.[spSearchServer]IF OBJECT_id ('dbo.[spSearchTable]') IS NOT NULL drop procedure dbo.[spSearchTable]IF OBJECT_id ('dbo.[spServerResponseChecks]') IS NOT NULL drop procedure dbo.[spServerResponseChecks]IF OBJECT_id ('dbo.[spUpdatePublisher]') IS NOT NULL drop procedure dbo.[spUpdatePublisher]IF OBJECT_id ('dbo.[spUpdateSubscriber]') IS NOT NULL drop procedure dbo.[spUpdateSubscriber]IF OBJECT_id ('dbo.[spd]') IS NOT NULL drop synonym dbo.[spd]IF OBJECT_id ('dbo.[sph]') IS NOT NULL drop synonym dbo.[sph]IF OBJECT_id ('dbo.[spr]') IS NOT NULL drop synonym dbo.[spr]IF OBJECT_id ('dbo.[sps]') IS NOT NULL drop synonym dbo.[sps]IF OBJECT_id ('dbo.[fnLastBackups]') IS NOT NULL drop function dbo.[fnLastBackups]IF OBJECT_id ('dbo.[tfServerFullyLoaded]') IS NOT NULL drop function dbo.[tfServerFullyLoaded]IF OBJECT_id ('dbo.[vblocks]') IS NOT NULL drop view dbo.[vblocks]IF OBJECT_id ('dbo.[vBlockTree]') IS NOT NULL drop view dbo.[vBlockTree]IF OBJECT_id ('dbo.[VIV_TotalRows]') IS NOT NULL drop view dbo.[VIV_TotalRows]IF OBJECT_id ('dbo.[vwADGroupMembers]') IS NOT NULL drop view dbo.[vwADGroupMembers]IF OBJECT_id ('dbo.[vwAvailabilityGroups]') IS NOT NULL drop view dbo.[vwAvailabilityGroups]IF OBJECT_id ('dbo.[vwBackupReport]') IS NOT NULL drop view dbo.[vwBackupReport]IF OBJECT_id ('dbo.[vwBackups]') IS NOT NULL drop view dbo.[vwBackups]IF OBJECT_id ('dbo.[vwBadIndexes]') IS NOT NULL drop view dbo.[vwBadIndexes]IF OBJECT_id ('dbo.[vwClusterNodes]') IS NOT NULL drop view dbo.[vwClusterNodes]IF OBJECT_id ('dbo.[vwDatabaseFiles]') IS NOT NULL drop view dbo.[vwDatabaseFiles]IF OBJECT_id ('dbo.[vwDatabaseObjectColumns]') IS NOT NULL drop view dbo.[vwDatabaseObjectColumns]IF OBJECT_id ('dbo.[vwDatabaseObjectPerms]') IS NOT NULL drop view dbo.[vwDatabaseObjectPerms]
IF OBJECT_id ('dbo.[vwDatabaseObjects]') IS NOT NULL drop view dbo.[vwDatabaseObjects]IF OBJECT_id ('dbo.[vwDatabasePerms]') IS NOT NULL drop view dbo.[vwDatabasePerms]IF OBJECT_id ('dbo.[vwDatabases]') IS NOT NULL drop view dbo.[vwDatabases]IF OBJECT_id ('dbo.[vwDatabasesNotOwnedBySA]') IS NOT NULL drop view dbo.[vwDatabasesNotOwnedBySA]IF OBJECT_id ('dbo.[vwDatabasesOnLine]') IS NOT NULL drop view dbo.[vwDatabasesOnLine]IF OBJECT_id ('dbo.[vwDatabasesOnLineNonSystem]') IS NOT NULL drop view dbo.[vwDatabasesOnLineNonSystem]IF OBJECT_id ('dbo.[vwDeadLocks]') IS NOT NULL drop view dbo.[vwDeadLocks]IF OBJECT_id ('dbo.[vwErrors]') IS NOT NULL drop view dbo.[vwErrors]IF OBJECT_id ('dbo.[vwIndexFragmentation]') IS NOT NULL drop view dbo.[vwIndexFragmentation]IF OBJECT_id ('dbo.[vwIndexUsage]') IS NOT NULL drop view dbo.[vwIndexUsage]IF OBJECT_id ('dbo.[vwJobHistory]') IS NOT NULL drop view dbo.[vwJobHistory]IF OBJECT_id ('dbo.[vwJobs]') IS NOT NULL drop view dbo.[vwJobs]IF OBJECT_id ('dbo.[vwJobsRunning]') IS NOT NULL drop view dbo.[vwJobsRunning]IF OBJECT_id ('dbo.[vwJobSteps]') IS NOT NULL drop view dbo.[vwJobSteps]IF OBJECT_id ('dbo.[vwLogins]') IS NOT NULL drop view dbo.[vwLogins]IF OBJECT_id ('dbo.[vwLongSql]') IS NOT NULL drop view dbo.[vwLongSql]IF OBJECT_id ('dbo.[vwMissingIndexes]') IS NOT NULL drop view dbo.[vwMissingIndexes]IF OBJECT_id ('dbo.[vwMissingMask]') IS NOT NULL drop view dbo.[vwMissingMask]IF OBJECT_id ('dbo.[vwMissingMsdb_Backups]') IS NOT NULL drop view dbo.[vwMissingMsdb_Backups]IF OBJECT_id ('dbo.[vwMsdb_Backups]') IS NOT NULL drop view dbo.[vwMsdb_Backups]IF OBJECT_id ('dbo.[vwPerMon]') IS NOT NULL drop view dbo.[vwPerMon]IF OBJECT_id ('dbo.[vwQueryStoreLongestAvgTimes]') IS NOT NULL drop view dbo.[vwQueryStoreLongestAvgTimes]IF OBJECT_id ('dbo.[vwQueryStoreTopExecCounts]') IS NOT NULL drop view dbo.[vwQueryStoreTopExecCounts]IF OBJECT_id ('dbo.[vwReplication]') IS NOT NULL drop view dbo.[vwReplication]IF OBJECT_id ('dbo.[vwRplDates]') IS NOT NULL drop view dbo.[vwRplDates]IF OBJECT_id ('dbo.[vwRplImportLog]') IS NOT NULL drop view dbo.[vwRplImportLog]IF OBJECT_id ('dbo.[vwRplImportLogDetail]') IS NOT NULL drop view dbo.[vwRplImportLogDetail]IF OBJECT_id ('dbo.[vwRplPublicationTable]') IS NOT NULL drop view dbo.[vwRplPublicationTable]IF OBJECT_id ('dbo.[vwRplSubscription]') IS NOT NULL drop view dbo.[vwRplSubscription]IF OBJECT_id ('dbo.[vwRplSubscriptionRoutine]') IS NOT NULL drop view dbo.[vwRplSubscriptionRoutine]IF OBJECT_id ('dbo.[vwRplSubscriptionTable]') IS NOT NULL drop view dbo.[vwRplSubscriptionTable]IF OBJECT_id ('dbo.[vwSequenses]') IS NOT NULL drop view dbo.[vwSequenses]IF OBJECT_id ('dbo.[vwServerLatency]') IS NOT NULL drop view dbo.[vwServerLatency]IF OBJECT_id ('dbo.[vwServerPerms]') IS NOT NULL drop view dbo.[vwServerPerms]IF OBJECT_id ('dbo.[vwServers]') IS NOT NULL drop view dbo.[vwServers]IF OBJECT_id ('dbo.[vwServersNonAzureSql]') IS NOT NULL drop view dbo.[vwServersNonAzureSql]IF OBJECT_id ('dbo.[vwServersToPatch]') IS NOT NULL drop view dbo.[vwServersToPatch]IF OBJECT_id ('dbo.[vwServersVms]') IS NOT NULL drop view dbo.[vwServersVms]IF OBJECT_id ('dbo.[vwServices]') IS NOT NULL drop view dbo.[vwServices]IF OBJECT_id ('dbo.[vwSqlBuilds]') IS NOT NULL drop view dbo.[vwSqlBuilds]IF OBJECT_id ('dbo.[vwSqlMgtJobHistory]') IS NOT NULL drop view dbo.[vwSqlMgtJobHistory]IF OBJECT_id ('dbo.[vwSqlMgtJobs]') IS NOT NULL drop view dbo.[vwSqlMgtJobs]IF OBJECT_id ('dbo.[vwSqlMgtJobsFiltered]') IS NOT NULL drop view dbo.[vwSqlMgtJobsFiltered]IF OBJECT_id ('dbo.[vwSqlMgtJobSteps]') IS NOT NULL drop view dbo.[vwSqlMgtJobSteps]IF OBJECT_id ('dbo.[vwSqlMgtJobStepsFiltered]') IS NOT NULL drop view dbo.[vwSqlMgtJobStepsFiltered]IF OBJECT_id ('dbo.[vwSuspectSysadmins]') IS NOT NULL drop view dbo.[vwSuspectSysadmins]IF OBJECT_id ('dbo.[vwTempConnections]') IS NOT NULL drop view dbo.[vwTempConnections]
IF OBJECT_id ('dbo.[vwTopSql]') IS NOT NULL drop view dbo.[vwTopSql]IF OBJECT_id ('dbo.[vwTopWait]') IS NOT NULL drop view dbo.[vwTopWait]IF OBJECT_id ('dbo.[vwVolumes]') IS NOT NULL drop view dbo.[vwVolumes]
IF OBJECT_id ('dbo.[ADGroupMembers]') IS NOT NULL drop table dbo.[ADGroupMembers]IF OBJECT_id ('dbo.[Admin_DatabasesNotToBackup]') IS NOT NULL drop table dbo.[Admin_DatabasesNotToBackup]IF OBJECT_id ('dbo.[Applications]') IS NOT NULL drop table dbo.[Applications]IF OBJECT_id ('dbo.[ApplicationServers]') IS NOT NULL drop table dbo.[ApplicationServers]IF OBJECT_id ('dbo.[Articles]') IS NOT NULL drop table dbo.[Articles]IF OBJECT_id ('dbo.[AvailabilityGroups]') IS NOT NULL drop table dbo.[AvailabilityGroups]IF OBJECT_id ('dbo.[BackupFiles]') IS NOT NULL drop table dbo.[BackupFiles]IF OBJECT_id ('dbo.[BackupFolders]') IS NOT NULL drop table dbo.[BackupFolders]IF OBJECT_id ('dbo.[blocks]') IS NOT NULL drop table dbo.[blocks]IF OBJECT_id ('dbo.[BrokenRoutines]') IS NOT NULL drop table dbo.[BrokenRoutines]IF OBJECT_id ('dbo.[ClusterNodes]') IS NOT NULL drop table dbo.[ClusterNodes]IF OBJECT_id ('dbo.[Command]') IS NOT NULL drop table dbo.[Command]IF OBJECT_id ('dbo.[CustomerMismatch]') IS NOT NULL drop table dbo.[CustomerMismatch]IF OBJECT_id ('dbo.[CustomerMismatchOrders]') IS NOT NULL drop table dbo.[CustomerMismatchOrders]IF OBJECT_id ('dbo.[DatabaseFiles]') IS NOT NULL drop table dbo.[DatabaseFiles]IF OBJECT_id ('dbo.[DatabaseFilesHist]') IS NOT NULL drop table dbo.[DatabaseFilesHist]IF OBJECT_id ('dbo.[DatabaseObjectColumns]') IS NOT NULL drop table dbo.[DatabaseObjectColumns]IF OBJECT_id ('dbo.[DatabaseObjectPerms]') IS NOT NULL drop table dbo.[DatabaseObjectPerms]IF OBJECT_id ('dbo.[DatabaseObjects]') IS NOT NULL drop table dbo.[DatabaseObjects]IF OBJECT_id ('dbo.[DatabasePerms]') IS NOT NULL drop table dbo.[DatabasePerms]IF OBJECT_id ('dbo.[Databases]') IS NOT NULL drop table dbo.[Databases]IF OBJECT_id ('dbo.[DatabasesHist]') IS NOT NULL drop table dbo.[DatabasesHist]IF OBJECT_id ('dbo.[DeadLockFiles]') IS NOT NULL drop table dbo.[DeadLockFiles]IF OBJECT_id ('dbo.[Deadlocks]') IS NOT NULL drop table dbo.[Deadlocks]IF OBJECT_id ('dbo.[Environment]') IS NOT NULL drop table dbo.[Environment]IF OBJECT_id ('dbo.[ErrorFiles]') IS NOT NULL drop table dbo.[ErrorFiles]IF OBJECT_id ('dbo.[Errors]') IS NOT NULL drop table dbo.[Errors]IF OBJECT_id ('dbo.[ExecErrors]') IS NOT NULL drop table dbo.[ExecErrors]IF OBJECT_id ('dbo.[ImportError]') IS NOT NULL drop table dbo.[ImportError]IF OBJECT_id ('dbo.[ImportFile]') IS NOT NULL drop table dbo.[ImportFile]IF OBJECT_id ('dbo.[ImportLog]') IS NOT NULL drop table dbo.[ImportLog]IF OBJECT_id ('dbo.[IndexFragmentation]') IS NOT NULL drop table dbo.[IndexFragmentation]IF OBJECT_id ('dbo.[IndexUsage]') IS NOT NULL drop table dbo.[IndexUsage]IF OBJECT_id ('dbo.[IndexUsageHist]') IS NOT NULL drop table dbo.[IndexUsageHist]IF OBJECT_id ('dbo.[JobHistory]') IS NOT NULL drop table dbo.[JobHistory]IF OBJECT_id ('dbo.[Jobs]') IS NOT NULL drop table dbo.[Jobs]IF OBJECT_id ('dbo.[JobsRunning]') IS NOT NULL drop table dbo.[JobsRunning]IF OBJECT_id ('dbo.[JobSteps]') IS NOT NULL drop table dbo.[JobSteps]IF OBJECT_id ('dbo.[LinkedServers]') IS NOT NULL drop table dbo.[LinkedServers]IF OBJECT_id ('dbo.[Logins]') IS NOT NULL drop table dbo.[Logins]IF OBJECT_id ('dbo.[LongSql]') IS NOT NULL drop table dbo.[LongSql]IF OBJECT_id ('dbo.[LongSqlFiles]') IS NOT NULL drop table dbo.[LongSqlFiles]IF OBJECT_id ('dbo.[MissingIndexes]') IS NOT NULL drop table dbo.[MissingIndexes]IF OBJECT_id ('dbo.[Msdb_Backups]') IS NOT NULL drop table dbo.[Msdb_Backups]IF OBJECT_id ('dbo.[ObjectTypes]') IS NOT NULL drop table dbo.[ObjectTypes]IF OBJECT_id ('dbo.[PerfMon]') IS NOT NULL drop table dbo.[PerfMon]IF OBJECT_id ('dbo.[PerfMonApp]') IS NOT NULL drop table dbo.[PerfMonApp]IF OBJECT_id ('dbo.[PerfMonAppStg]') IS NOT NULL drop table dbo.[PerfMonAppStg]IF OBJECT_id ('dbo.[PerfMonStg]') IS NOT NULL drop table dbo.[PerfMonStg]IF OBJECT_id ('dbo.[PublicationPendingCommands]') IS NOT NULL drop table dbo.[PublicationPendingCommands]
IF OBJECT_id ('dbo.[Publications]') IS NOT NULL drop table dbo.[Publications]IF OBJECT_id ('dbo.[Publishers]') IS NOT NULL drop table dbo.[Publishers]IF OBJECT_id ('dbo.[Purpose]') IS NOT NULL drop table dbo.[Purpose]IF OBJECT_id ('dbo.[QueryStoreLongestAvgTimes]') IS NOT NULL drop table dbo.[QueryStoreLongestAvgTimes]IF OBJECT_id ('dbo.[QueryStoreTopExecCounts]') IS NOT NULL drop table dbo.[QueryStoreTopExecCounts]IF OBJECT_id ('dbo.[ReplicationCheck]') IS NOT NULL drop table dbo.[ReplicationCheck]IF OBJECT_id ('dbo.[RplDates]') IS NOT NULL drop table dbo.[RplDates]IF OBJECT_id ('dbo.[RplImportLog]') IS NOT NULL drop table dbo.[RplImportLog]IF OBJECT_id ('dbo.[RplImportLogDetail]') IS NOT NULL drop table dbo.[RplImportLogDetail]IF OBJECT_id ('dbo.[RplPublicationTable]') IS NOT NULL drop table dbo.[RplPublicationTable]IF OBJECT_id ('dbo.[RplSubscription]') IS NOT NULL drop table dbo.[RplSubscription]IF OBJECT_id ('dbo.[RplSubscriptionRoutine]') IS NOT NULL drop table dbo.[RplSubscriptionRoutine]IF OBJECT_id ('dbo.[RplSubscriptionTable]') IS NOT NULL drop table dbo.[RplSubscriptionTable]IF OBJECT_id ('dbo.[SecurityGroups]') IS NOT NULL drop table dbo.[SecurityGroups]IF OBJECT_id ('dbo.[SecurityTypes]') IS NOT NULL drop table dbo.[SecurityTypes]IF OBJECT_id ('dbo.[Sequences]') IS NOT NULL drop table dbo.[Sequences]IF OBJECT_id ('dbo.[ServerAudit]') IS NOT NULL drop table dbo.[ServerAudit]IF OBJECT_id ('dbo.[ServerPerms]') IS NOT NULL drop table dbo.[ServerPerms]IF OBJECT_id ('dbo.[Servers]') IS NOT NULL drop table dbo.[Servers]IF OBJECT_id ('dbo.[ServersHist]') IS NOT NULL drop table dbo.[ServersHist]IF OBJECT_id ('dbo.[Services]') IS NOT NULL drop table dbo.[Services]IF OBJECT_id ('dbo.[SQLAzure]') IS NOT NULL drop table dbo.[SQLAzure]IF OBJECT_id ('dbo.[SubscriptionPendingCommands]') IS NOT NULL drop table dbo.[SubscriptionPendingCommands]IF OBJECT_id ('dbo.[Subscriptions]') IS NOT NULL drop table dbo.[Subscriptions]IF OBJECT_id ('dbo.[TempConnections]') IS NOT NULL drop table dbo.[TempConnections]IF OBJECT_id ('dbo.[TempFiles]') IS NOT NULL drop table dbo.[TempFiles]IF OBJECT_id ('dbo.[TempFolders]') IS NOT NULL drop table dbo.[TempFolders]IF OBJECT_id ('dbo.[TopSql]') IS NOT NULL drop table dbo.[TopSql]IF OBJECT_id ('dbo.[TopWait]') IS NOT NULL drop table dbo.[TopWait]IF OBJECT_id ('dbo.[Volumes]') IS NOT NULL drop table dbo.[Volumes]IF OBJECT_id ('dbo.[VolumesHist]') IS NOT NULL drop table dbo.[VolumesHist]

go
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
if object_id('dbo.spPrintLongSql') is not null
drop proc dbo.spPrintLongSql
go
CREATE PROCEDURE [dbo].[spPrintLongSql]( @string nvarchar(max) )
AS
SET NOCOUNT ON
 
set @string = rtrim( @string )
 
declare @cr char(1), @lf char(1)
set @cr = char(13)
set @lf = char(10)
 
declare @len int, @cr_index int, @lf_index int, @crlf_index int, @has_cr_and_lf bit, @left nvarchar(4000), @reverse nvarchar(4000)
set @len = 4000
 
while ( len( @string ) > @len )
begin
   set @left = left( @string, @len )
   set @reverse = reverse( @left )
   set @cr_index = @len - charindex( @cr, @reverse ) + 1
   set @lf_index = @len - charindex( @lf, @reverse ) + 1
   set @crlf_index = case when @cr_index < @lf_index then @cr_index else @lf_index end
   set @has_cr_and_lf = case when @cr_index < @len and @lf_index < @len then 1 else 0 end
   print left( @string, @crlf_index - 1 )
   set @string = right( @string, len( @string ) - @crlf_index - @has_cr_and_lf )
end
 
print @string
GO

if object_id('dbo.spExec') is not null
drop proc dbo.spExec
go
CREATE proc [dbo].[spExec] (@sql varchar(max), @debug bit = 0, @exec bit = 1, @raiserror bit=0)
as
begin
	begin try
		if @exec = 1
			exec (@sql)
		if @debug = 1
		begin
			if len(@sql) < 8000
				print @sql
			else 
				exec dbo.spPrintLongSql @sql
			--print 'GO'			
		end
	end try
	begin catch
		declare @error varchar(255), @severity int, @state int
		select @error = ERROR_MESSAGE()
			, @severity = ERROR_SEVERITY()
			, @state = ERROR_STATE()
		
		if len(@sql) < 8000
			print @sql
		else 
			exec dbo.spPrintLongSql @sql
		
		insert into ExecErrors (message, command) values (@error, @sql)

		if @raiserror = 1
			raiserror (@error, @severity, @state)
		else 
			print error_message()
	end catch
end


GO


GO
/****** Object:  UserDefinedFunction [dbo].[fnLastBackups]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create function [dbo].[fnLastBackups] ()
returns @b table (
	DatabaseName varchar(100),
	Type varchar(10),
	Size bigint,
	Name varchar(255),
	Date datetime
)
as 
begin

insert into @b
select db.DatabaseName
	,b.Type , b.size, b.name, b.Date
from databases db
outer apply (
	select top 1 * from backupfiles b
	where b.databasename = db.databasename
	and type ='Full'
	order by date desc
) b

insert into @b
select db.DatabaseName
	,b.Type, b.size , b.name, b.Date
from databases db
cross apply (
	select max(Date) last
	from @b b
	where b.DatabaseName = db.DatabaseName
) l
outer apply (
	select top 1 * from backupfiles b
	where b.databasename = db.databasename
	and type ='Diff'
	and b.Date > l.last
	order by date desc
) b

insert into @b
select db.DatabaseName
	,b.Type , b.size, b.name, b.Date
from databases db
cross apply (
	select max(Date) last
	from @b b
	where b.DatabaseName = db.DatabaseName
) l
join backupfiles b
	on b.databasename = db.databasename
	and type ='Log'
	and b.Date > l.last

return
end

GO
/****** Object:  UserDefinedFunction [dbo].[fnNullVal]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
create function [dbo].[fnNullVal] (@Type varchar(100) )
returns varchar(100)
as
begin 
	declare @NullVal varchar(100)

	select @NullVal = case  when @Type LIKE '%char%' then ''''''
							when @Type LIKE '%text%' then ''''''
							when @Type LIKE 'decimal%' then '0'
							when @Type LIKE 'numeric%' then '0'
							when @Type LIKE 'varbinary%' then '0x'
							when @Type in ('tinyint','smallint','float','money','int','bit','smallmoney','bigint') then '0'
							when @Type in ('uniqueidentifier') then '''00000000-0000-0000-0000-000000000000'''
							when @Type in ('datetime', 'date','smalldatetime') then '''01/01/1999'''
							else ''''''
							--TODO: image, datetime, varbinary
						end 
	return(@NullVal)
end		
GO
/****** Object:  UserDefinedFunction [dbo].[InStringCount]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE FUNCTION [dbo].[InStringCount](
    @searchString nvarchar(max),
    @searchTerm nvarchar(max)
)
RETURNS INT
AS
BEGIN
    return (LEN(@searchString)-LEN(REPLACE(@searchString,@searchTerm,'')))/LEN(@searchTerm)
END
GO
/****** Object:  UserDefinedFunction [dbo].[RegexMatch]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE FUNCTION [dbo].[RegexMatch]
    (
      @pattern VARCHAR(2000),
      @matchstring VARCHAR(MAX)--Varchar(8000) got SQL Server 2000
    )
RETURNS INT
AS BEGIN
    DECLARE @objRegexExp INT,
        @objErrorObject INT,
        @strErrorMessage VARCHAR(255),
        @hr INT,
        @match BIT
    
    
    SELECT  @strErrorMessage = 'creating a regex object'
    EXEC @hr= sp_OACreate 'VBScript.RegExp', @objRegexExp OUT
    IF @hr = 0
        EXEC @hr= sp_OASetProperty @objRegexExp, 'Pattern', @pattern
        --Specifying a case-insensitive match
    IF @hr = 0
        EXEC @hr= sp_OASetProperty @objRegexExp, 'IgnoreCase', 1
        --Doing a Test'
    IF @hr = 0
        EXEC @hr= sp_OAMethod @objRegexExp, 'Test', @match OUT, @matchstring

    IF @hr <> 0
        BEGIN
            RETURN NULL
        END
    EXEC sp_OADestroy @objRegexExp
    RETURN @match
   END
GO
/****** Object:  UserDefinedFunction [dbo].[tfServerFullyLoaded]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



CREATE function [dbo].[tfServerFullyLoaded] ()
returns @servers table (serverid int)
as
begin
	--got columms
	insert into @servers
	select distinct ServerId from DatabaseObjectColumns

	--has dbs, but none is available for reads
	insert into @servers
	select distinct ServerId 
	from Servers s
	where exists (select * from databases d where d.ServerId = s.serverid)
	and not exists (select * from vwdatabases d where d.serverid = s.serverid 
				and d.state_desc = 'online'
				and d.ServerName = coalesce(d.PrimaryReplicaServerName,ServerName)
				and databasename not in ('master','msdb','reportserver','reportservertempdb','model','tempdb','SSISDB','RedGate','ChangeLog','DBATOOLS')
				and databasename not like 'distribution%' and databasename not like '%test%' and databasename not like '%[0-9][0-9]'
				)
	and ServerId not in (select serverid from @servers)
	 
	 --got dbs, got objects, but no table
	insert into @servers
	select distinct s.ServerId 
	from Servers s
	join databaseObjects do on do.ServerId = s.serverid
	where 1=1 --not exists (select * from databaseObjects t where t.ServerId = s.ServerId and t.xtype in ('u') )
	and s.ServerId not in (select serverid from @servers)
	
	/*
	--got dbs and but all have no object
	insert into @servers
	select distinct ServerId from Databases d
	where d.ServerId not in @servers
	and not exists (select * from DatabaseObjects do where do.DatabaseId = d.databaseid)
	
	--got objects 
	

and (
	--there is no db
	not exists (select * from databases d where d.serverid=s.serverid)
	--there is a db that should have been loaded
	or exists (select * from vwdatabases d where d.serverid = s.serverid and d.state_desc = 'online'and d.ServerName = coalesce(d.PrimaryReplicaServerName,ServerName)
				and databasename not in ('master','msdb','reportserver','reportservertempdb','model','tempdb','SSISDB','RedGate','ChangeLog')
				and databasename not like 'distribution%' and databasename not like '%test%' and databasename not like '%[0-9][0-9]'
				)
	)
	*/
	return
end 
GO
/****** Object:  UserDefinedFunction [dbo].[udf_schedule_description]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[udf_schedule_description] (@freq_type INT ,
 @freq_interval INT ,
 @freq_subday_type INT ,
 @freq_subday_interval INT ,
 @freq_relative_interval INT ,
 @freq_recurrence_factor INT ,
 @active_start_date INT ,
 @active_end_date INT,
 @active_start_time INT ,
 @active_end_time INT ) 
RETURNS NVARCHAR(255) AS 
BEGIN
DECLARE @schedule_description NVARCHAR(255)
DECLARE @loop INT
DECLARE @idle_cpu_percent INT
DECLARE @idle_cpu_duration INT

IF (@freq_type = 0x1) -- OneTime
 BEGIN
 SELECT @schedule_description = N'Once on ' + CONVERT(NVARCHAR, @active_start_date) + N' at ' + CONVERT(NVARCHAR, cast((@active_start_time / 10000) as varchar(10)) + ':' + right('00' + cast((@active_start_time % 10000) / 100 as varchar(10)),2))
 RETURN @schedule_description
 END
IF (@freq_type = 0x4) -- Daily
 BEGIN
 SELECT @schedule_description = N'Every day '
 END
IF (@freq_type = 0x8) -- Weekly
 BEGIN
 SELECT @schedule_description = N'Every ' + CONVERT(NVARCHAR, @freq_recurrence_factor) + N' week(s) on '
 SELECT @loop = 1
 WHILE (@loop <= 7)
 BEGIN
 IF (@freq_interval & POWER(2, @loop - 1) = POWER(2, @loop - 1))
 SELECT @schedule_description = @schedule_description + DATENAME(dw, N'1996120' + CONVERT(NVARCHAR, @loop)) + N', '
 SELECT @loop = @loop + 1
 END
 IF (RIGHT(@schedule_description, 2) = N', ')
 SELECT @schedule_description = SUBSTRING(@schedule_description, 1, (DATALENGTH(@schedule_description) / 2) - 2) + N' '
 END
IF (@freq_type = 0x10) -- Monthly
 BEGIN
 SELECT @schedule_description = N'Every ' + CONVERT(NVARCHAR, @freq_recurrence_factor) + N' months(s) on day ' + CONVERT(NVARCHAR, @freq_interval) + N' of that month '
 END
IF (@freq_type = 0x20) -- Monthly Relative
 BEGIN
 SELECT @schedule_description = N'Every ' + CONVERT(NVARCHAR, @freq_recurrence_factor) + N' months(s) on the '
 SELECT @schedule_description = @schedule_description +
 CASE @freq_relative_interval
 WHEN 0x01 THEN N'first '
 WHEN 0x02 THEN N'second '
 WHEN 0x04 THEN N'third '
 WHEN 0x08 THEN N'fourth '
 WHEN 0x10 THEN N'last '
 END +
 CASE
 WHEN (@freq_interval > 00)
 AND (@freq_interval < 08) THEN DATENAME(dw, N'1996120' + CONVERT(NVARCHAR, @freq_interval))
 WHEN (@freq_interval = 08) THEN N'day'
 WHEN (@freq_interval = 09) THEN N'week day'
 WHEN (@freq_interval = 10) THEN N'weekend day'
 END + N' of that month '
 END
IF (@freq_type = 0x40) -- AutoStart
 BEGIN
 SELECT @schedule_description = FORMATMESSAGE(14579)
 RETURN @schedule_description
 END
IF (@freq_type = 0x80) -- OnIdle
 BEGIN
 EXECUTE master.dbo.xp_instance_regread N'HKEY_LOCAL_MACHINE',
 N'SOFTWARE\Microsoft\MSSQLServer\SQLServerAgent',
 N'IdleCPUPercent',
 @idle_cpu_percent OUTPUT,
 N'no_output'
 EXECUTE master.dbo.xp_instance_regread N'HKEY_LOCAL_MACHINE',
 N'SOFTWARE\Microsoft\MSSQLServer\SQLServerAgent',
 N'IdleCPUDuration',
 @idle_cpu_duration OUTPUT,
 N'no_output'
 SELECT @schedule_description = FORMATMESSAGE(14578, ISNULL(@idle_cpu_percent, 10), ISNULL(@idle_cpu_duration, 600))
 RETURN @schedule_description
 END
-- Subday stuff
 SELECT @schedule_description = @schedule_description +
 CASE @freq_subday_type
 WHEN 0x1 THEN N'at ' + CONVERT(NVARCHAR, cast(
 CASE WHEN LEN(cast((@active_start_time / 10000)as varchar(10)))=1
     THEN  '0'+cast((@active_start_time / 10000) as varchar(10))
     ELSE cast((@active_start_time / 10000) as varchar(10))
     END    
as varchar(10)) + ':' + right('00' + cast((@active_start_time % 10000) / 100 as varchar(10)),2))
 WHEN 0x2 THEN N'every ' + CONVERT(NVARCHAR, @freq_subday_interval) + N' second(s)'
 WHEN 0x4 THEN N'every ' + CONVERT(NVARCHAR, @freq_subday_interval) + N' minute(s)'
 WHEN 0x8 THEN N'every ' + CONVERT(NVARCHAR, @freq_subday_interval) + N' hour(s)'
 END
 IF (@freq_subday_type IN (0x2, 0x4, 0x8))
 SELECT @schedule_description = @schedule_description + N' between ' +
CONVERT(NVARCHAR, cast(
CASE WHEN LEN(cast((@active_start_time / 10000)as varchar(10)))=1
     THEN  '0'+cast((@active_start_time / 10000) as varchar(10))
     ELSE cast((@active_start_time / 10000) as varchar(10))
     END    
as varchar(10)) + ':' + right('00' + cast((@active_start_time % 10000) / 100 as varchar(10)),2) ) 
+ N' and ' +
CONVERT(NVARCHAR, cast(
 CASE WHEN LEN(cast((@active_end_time / 10000)as varchar(10)))=1
     THEN  '0'+cast((@active_end_time / 10000) as varchar(10))
     ELSE cast((@active_end_time / 10000) as varchar(10))
     END    
as varchar(10)) + ':' + right('00' + cast((@active_end_time % 10000) / 100 as varchar(10)),2) )


RETURN @schedule_description
END
GO
/****** Object:  Table [dbo].[Servers]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Servers](
	[ServerId] [int] IDENTITY(1,1) NOT NULL,
	[EnvironmentId] [int] NULL,
	[PurposeId] [int] NULL,
	[ServerName] [varchar](100) NULL,
	[ServerDescription] [varchar](255) NULL,
	[WindowsRelease] [varchar](20) NULL,
	[CreatedDate] [datetime] NULL,
	[Version] [varchar](255) NULL,
	[Edition] [varchar](255) NULL,
	[EngineEdition] [varchar](50) NULL,
	[Collation] [varchar](50) NULL,
	[LogicalCPUCount] [int] NULL,
	[HyperthreadRatio] [int] NULL,
	[PhysicalCPUCount] [int] NULL,
	[PhysicalMemoryMB] [int] NULL,
	[VMType] [varchar](50) NULL,
	[Hardware] [varchar](100) NULL,
	[ProcessorNameString] [varchar](100) NULL,
	[BlockedProcessEvents] [varchar](255) NULL,
	[DeadlockEvents] [varchar](255) NULL,
	[ErrorEvents] [varchar](255) NULL,
	[LongQueryEvents] [varchar](255) NULL,
	[PerfMonLogs] [varchar](255) NULL,
	[IsActive] [bit] NULL,
	[IP] [varchar](30) NULL,
	[Error] [varchar](255) NULL,
	[BackupFolder] [varchar](500) NULL,
	[DailyChecks] [bit] NULL,
	[Domain] [varchar](100) NULL,
	[BackupChecks] [smallint] NULL,
	[Build] [varchar](50) NULL,
	[ErrorDate] [datetime] NULL,
	[resource_governor_enabled_functions] [tinyint] NULL,
	[RemoteUser] [varchar](100) NULL,
 CONSTRAINT [PK_Servers] PRIMARY KEY CLUSTERED 
(
	[ServerId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Environment]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Environment](
	[EnvironmentId] [int] IDENTITY(1,1) NOT NULL,
	[EnvironmentName] [varchar](50) NULL,
 CONSTRAINT [PK_Environment] PRIMARY KEY CLUSTERED 
(
	[EnvironmentId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Purpose]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Purpose](
	[PurposeId] [int] IDENTITY(1,1) NOT NULL,
	[PurposeName] [varchar](50) NULL,
 CONSTRAINT [PK_Purpose] PRIMARY KEY CLUSTERED 
(
	[PurposeId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Jobs]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Jobs](
	[JobId] [int] IDENTITY(1,1) NOT NULL,
	[ServerId] [int] NULL,
	[Jobname] [varchar](255) NULL,
	[Description] [varchar](512) NULL,
	[IsEnabled] [bit] NULL,
	[ScheduleDscr] [nvarchar](255) NULL,
	[Operator] [varchar](100) NULL,
	[OperatorEnabled] [bit] NULL,
	[Operator_email_address] [nvarchar](100) NULL,
	[Owner] [varchar](100) NULL,
	[JobStartStepName] [varchar](255) NULL,
	[IsScheduled] [bit] NULL,
	[JobScheduleName] [varchar](255) NULL,
	[Frequency] [varchar](36) NULL,
	[Units] [varchar](21) NULL,
	[Active_start_date] [datetime] NULL,
	[Active_end_date] [datetime] NULL,
	[Run_Time] [varchar](8) NULL,
	[Created_Date] [varchar](24) NULL,
	[jobidentifier] [uniqueidentifier] NULL,
 CONSTRAINT [PK_Jobs] PRIMARY KEY CLUSTERED 
(
	[JobId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[JobSteps]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[JobSteps](
	[JobStepId] [int] IDENTITY(1,1) NOT NULL,
	[JobId] [int] NOT NULL,
	[job_name] [varchar](255) NULL,
	[ScheduleDscr] [varchar](255) NULL,
	[enabled] [bit] NULL,
	[step_id] [smallint] NULL,
	[step_name] [varchar](255) NULL,
	[database_name] [varchar](255) NULL,
	[command] [varchar](max) NULL,
	[proc_name] [varchar](255) NULL,
	[serverid] [int] NULL,
 CONSTRAINT [PK_JobSteps] PRIMARY KEY CLUSTERED 
(
	[JobStepId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  View [dbo].[vwJobSteps]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE view [dbo].[vwJobSteps]
as
select p.*, e.*
	, s.serverid, s.ServerName
	, s.ServerDescription
	, j.Jobname, j.ScheduleDscr
	, je.JobStepId
	, je.JobId
	, je.job_name
	, je.enabled
	, je.step_id
	, je.step_name
	, je.database_name
	, je.command
	, je.proc_name

--select *
from servers s
left join purpose p on p.purposeid = s.purposeid
left join environment e on e.environmentid = s.environmentid
left join Jobs j on s.serverid=j.serverid
left join JobSteps je on je.jobid = j.jobid
--where s.ServerId=171

GO
/****** Object:  Table [dbo].[IndexUsage]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[IndexUsage](
	[ServerId] [int] NULL,
	[DatabaseId] [int] NULL,
	[data_space] [varchar](200) NULL,
	[allocation_desc] [varchar](200) NULL,
	[table_schema] [varchar](200) NULL,
	[object_type] [varchar](200) NULL,
	[table_name] [varchar](200) NULL,
	[index_type] [varchar](200) NULL,
	[index_name] [varchar](200) NULL,
	[is_unique] [bit] NULL,
	[is_disabled] [bit] NULL,
	[database_file] [varchar](200) NULL,
	[size_mbs] [int] NULL,
	[used_size] [int] NULL,
	[data_size] [int] NULL,
	[writes] [bigint] NULL,
	[reads] [bigint] NULL,
	[index_id] [int] NULL,
	[fill_factor] [float] NULL,
	[avg_fragmentation_in_percent] [float] NULL,
	[cols] [varchar](1000) NULL,
	[included] [varchar](8000) NULL,
	[filter_definition] [varchar](1000) NULL,
	[drop_cmd] [varchar](8000) NULL,
	[disable_cmd] [varchar](8000) NULL,
	[create_cmd] [varchar](8000) NULL,
	[DatabaseObjectId] [int] NULL,
	[rowid] [int] IDENTITY(1,1) NOT NULL,
	[data_compression_desc] [nvarchar](120) NULL,
 CONSTRAINT [pk_IndexUsage] PRIMARY KEY CLUSTERED 
(
	[rowid] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[DatabaseObjectColumns]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[DatabaseObjectColumns](
	[DatabaseObjectColumnId] [int] IDENTITY(1,1) NOT NULL,
	[DatabaseObjectId] [int] NULL,
	[ServerId] [int] NULL,
	[DatabaseId] [int] NULL,
	[TABLE_CATALOG] [varchar](200) NULL,
	[TABLE_SCHEMA] [varchar](200) NULL,
	[TABLE_NAME] [varchar](200) NULL,
	[COLUMN_NAME] [varchar](200) NULL,
	[ORDINAL_POSITION] [int] NULL,
	[COLUMN_DEFAULT] [varchar](800) NULL,
	[IS_NULLABLE] [varchar](100) NULL,
	[DATA_TYPE] [varchar](100) NULL,
	[CHARACTER_MAXIMUM_LENGTH] [int] NULL,
	[COLLATION_NAME] [varchar](100) NULL,
	[is_computed] [bit] NULL,
	[is_identity] [bit] NULL,
	[MASKING_FUNCTION] [nvarchar](max) NULL,
 CONSTRAINT [PK_DatabaseObjectColumns] PRIMARY KEY CLUSTERED 
(
	[DatabaseObjectColumnId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Subscriptions]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Subscriptions](
	[SubscriptionId] [int] IDENTITY(1,1) NOT NULL,
	[ArticleId] [int] NULL,
	[PublicationId] [int] NULL,
	[PublisherId] [int] NULL,
	[ServerId] [int] NULL,
	[remote_publication_id] [int] NULL,
	[remote_article_id] [int] NULL,
	[subscriber_server] [varchar](100) NULL,
	[subscriber_db] [varchar](100) NULL,
	[subscription_type] [int] NOT NULL,
	[sync_type] [tinyint] NOT NULL,
	[status] [tinyint] NOT NULL,
	[snapshot_seqno_flag] [bit] NOT NULL,
	[independent_agent] [bit] NOT NULL,
	[subscription_time] [datetime] NOT NULL,
	[loopback_detection] [bit] NOT NULL,
	[agent_id] [int] NOT NULL,
	[update_mode] [tinyint] NOT NULL,
 CONSTRAINT [pk_Subscriptions] PRIMARY KEY CLUSTERED 
(
	[SubscriptionId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Articles]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Articles](
	[ArticleId] [int] IDENTITY(1,1) NOT NULL,
	[PublicationId] [int] NULL,
	[PublisherId] [int] NULL,
	[ServerId] [int] NULL,
	[article] [varchar](100) NOT NULL,
	[destination_object] [varchar](100) NULL,
	[source_owner] [varchar](100) NULL,
	[source_object] [varchar](100) NULL,
	[description] [nvarchar](255) NULL,
	[destination_owner] [varchar](100) NULL,
	[remote_publication_id] [int] NULL,
	[remote_article_id] [int] NULL,
PRIMARY KEY CLUSTERED 
(
	[ArticleId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[DatabaseObjects]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[DatabaseObjects](
	[DatabaseObjectId] [int] IDENTITY(1,1) NOT NULL,
	[ServerId] [int] NULL,
	[DatabaseId] [int] NULL,
	[ObjectName] [varchar](200) NULL,
	[SchemaName] [varchar](100) NULL,
	[Xtype] [varchar](2) NULL,
	[RowCount] [bigint] NULL,
	[ColCount] [int] NULL,
	[MinCr_Ct] [datetime] NULL,
	[RowLength] [int] NULL,
	[ReplColumns] [int] NULL,
	[HasCr_Dt] [bit] NULL,
	[SQL_DATA_ACCESS] [varchar](20) NULL,
	[ROUTINE_DEFINITION] [varchar](max) NULL,
	[is_mspublished] [bit] NULL,
	[is_rplpublished] [bit] NULL,
	[is_rplsubscribed] [bit] NULL,
	[is_disabled] [bit] NULL,
	[parent_object_id] [int] NULL,
	[start_value] [bigint] NULL,
	[current_value] [bigint] NULL,
	[ParentSchema] [varchar](100) NULL,
	[ParentTable] [varchar](100) NULL,
	[ParentColumn] [varchar](100) NULL,
	[crdate] [datetime] NULL,
 CONSTRAINT [PK_DatabaseObjects] PRIMARY KEY CLUSTERED 
(
	[DatabaseObjectId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Databases]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Databases](
	[DatabaseId] [int] IDENTITY(1,1) NOT NULL,
	[ServerId] [int] NULL,
	[DatabaseName] [varchar](100) NULL,
	[RecoveryModel] [varchar](100) NULL,
	[LogSizeKB] [bigint] NULL,
	[LogUsedKB] [bigint] NULL,
	[LogUsedPercentage] [varchar](50) NULL,
	[DBCompatibilityLevel] [varchar](50) NULL,
	[PageVerifyOption] [varchar](50) NULL,
	[is_auto_create_stats_on] [bit] NULL,
	[is_auto_update_stats_on] [bit] NULL,
	[is_auto_update_stats_async_on] [bit] NULL,
	[is_parameterization_forced] [bit] NULL,
	[snapshot_isolation_state_desc] [varchar](50) NULL,
	[is_read_committed_snapshot_on] [bit] NULL,
	[is_auto_close_on] [bit] NULL,
	[is_auto_shrink_on] [bit] NULL,
	[target_recovery_time_in_seconds] [int] NULL,
	[DataMB] [bigint] NULL,
	[LogMB] [bigint] NULL,
	[State_Desc] [varchar](100) NULL,
	[Create_Date] [datetime] NULL,
	[is_published] [bit] NULL,
	[is_subscribed] [bit] NULL,
	[Collation] [varchar](100) NULL,
	[CachedSizeMbs] [int] NULL,
	[CPUTime] [bigint] NULL,
	[Is_Read_Only] [bit] NULL,
	[delayed_durability_desc] [varchar](20) NULL,
	[containment_desc] [varchar](20) NULL,
	[is_cdc_enabled] [bit] NULL,
	[is_broker_enabled] [bit] NULL,
	[is_memory_optimized_elevate_to_snapshot_on] [bit] NULL,
	[AvailabilityGroup] [varchar](100) NULL,
	[PrimaryReplicaServerName] [varchar](100) NULL,
	[LocalReplicaRole] [tinyint] NULL,
	[SynchronizationState] [tinyint] NULL,
	[IsSuspended] [bit] NULL,
	[IsJoined] [bit] NULL,
	[SourceDatabaseName] [varchar](200) NULL,
	[owner] [varchar](100) NULL,
	[mirroring_state] [varchar](255) NULL,
	[mirroring_role] [varchar](255) NULL,
	[mirroring_safety_level] [varchar](255) NULL,
	[mirroring_partner] [varchar](255) NULL,
	[mirroring_partner_instance] [varchar](255) NULL,
	[mirroring_witness] [varchar](255) NULL,
	[mirroring_witness_state] [varchar](255) NULL,
	[mirroring_connection_timeout] [int] NULL,
	[mirroring_redo_queue] [int] NULL,
	[is_encrypted] [bit] NULL,
	[edition] [varchar](100) NULL,
	[service_objective] [varchar](100) NULL,
	[elastic_pool_name] [varchar](100) NULL,
	[is_query_store_on] [bit] NULL,
 CONSTRAINT [PK_Databases] PRIMARY KEY CLUSTERED 
(
	[DatabaseId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[RplImportLog]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[RplImportLog](
	[rowid] [int] IDENTITY(1,1) NOT NULL,
	[serverid] [int] NULL,
	[databaseid] [int] NULL,
	[ImportLogId] [int] NOT NULL,
	[SubscriptionId] [int] NULL,
	[RvFrom] [varbinary](8) NULL,
	[RvTo] [varbinary](8) NULL,
	[StartDate] [datetime] NULL,
	[EndDate] [datetime] NULL,
	[Success] [bit] NULL,
	[TotalRows] [bigint] NULL,
	[RvTotalRows] [bigint] NULL,
	[Threads] [tinyint] NULL,
	[UseStage] [bit] NULL,
	[message] [varchar](max) NULL,
	[RplSubscriptionRowId] [int] NULL,
	[TotalKbs] [bigint] NULL,
 CONSTRAINT [PK_RplImportLog] PRIMARY KEY CLUSTERED 
(
	[rowid] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[SQLAzure]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[SQLAzure](
	[DatabaseId] [int] NOT NULL,
	[ServerId] [int] NULL,
	[SLO_Name] [varchar](100) NULL,
	[Service_Tier] [varchar](100) NULL,
	[Hardware_Gen] [varchar](100) NULL,
	[Logical_CPUs] [numeric](5, 2) NULL,
	[Memory] [numeric](5, 2) NULL,
	[DTUs] [int] NULL,
	[CPUs] [int] NULL,
 CONSTRAINT [PK_SQLAzure] PRIMARY KEY CLUSTERED 
(
	[DatabaseId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[DatabaseFiles]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[DatabaseFiles](
	[DatabaseFileId] [int] IDENTITY(1,1) NOT NULL,
	[ServerId] [int] NULL,
	[DatabaseId] [int] NULL,
	[FileName] [varchar](200) NULL,
	[PhysicalName] [varchar](500) NULL,
	[TotalMbs] [int] NULL,
	[AvailableMbs] [int] NULL,
	[fileid] [int] NULL,
	[filegroupname] [varchar](100) NULL,
 CONSTRAINT [PK_DatabaseFiles] PRIMARY KEY CLUSTERED 
(
	[DatabaseFileId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[DatabasePerms]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[DatabasePerms](
	[DatabasePermId] [int] IDENTITY(1,1) NOT NULL,
	[ServerId] [int] NULL,
	[LoginId] [int] NULL,
	[DatabaseId] [int] NULL,
	[USERNAME] [varchar](100) NULL,
	[DB_OWNER] [bit] NULL,
	[DB_ACCESSADMIN] [bit] NULL,
	[DB_SECURITYADMIN] [bit] NULL,
	[DB_DDLADMIN] [bit] NULL,
	[DB_DATAREADER] [bit] NULL,
	[DB_DATAWRITER] [bit] NULL,
	[DB_DENYDATAREADER] [bit] NULL,
	[DB_DENYDATAWRITER] [bit] NULL,
	[CREATEDATE] [datetime] NULL,
	[UPDATEDATE] [datetime] NULL,
 CONSTRAINT [PK_DatabasePerms] PRIMARY KEY CLUSTERED 
(
	[DatabasePermId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[ImportLog]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[ImportLog](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[date] [datetime] NULL,
PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Publications]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Publications](
	[PublicationId] [int] IDENTITY(1,1) NOT NULL,
	[PublisherId] [int] NULL,
	[ServerId] [int] NULL,
	[publisher_db] [varchar](100) NULL,
	[publication] [varchar](100) NOT NULL,
	[publication_type] [int] NOT NULL,
	[thirdparty_flag] [bit] NOT NULL,
	[independent_agent] [bit] NOT NULL,
	[immediate_sync] [bit] NOT NULL,
	[allow_push] [bit] NOT NULL,
	[allow_pull] [bit] NOT NULL,
	[allow_anonymous] [bit] NOT NULL,
	[description] [nvarchar](255) NULL,
	[vendor_name] [nvarchar](100) NULL,
	[retention] [int] NULL,
	[sync_method] [int] NOT NULL,
	[allow_subscription_copy] [bit] NOT NULL,
	[thirdparty_options] [int] NULL,
	[allow_queued_tran] [bit] NOT NULL,
	[options] [int] NOT NULL,
	[retention_period_unit] [tinyint] NOT NULL,
	[allow_initialize_from_backup] [bit] NOT NULL,
	[remote_publication_id] [int] NULL,
 CONSTRAINT [pk_publications] PRIMARY KEY CLUSTERED 
(
	[PublicationId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  View [dbo].[vwDatabases]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE VIEW [dbo].[vwDatabases]
AS
SELECT p.PurposeName, e.EnvironmentName, p.PurposeId, e.EnvironmentId
	, s.ServerName, s.IsActive, s.DailyChecks, s.Version, s.EngineEdition
	,	m.datasource,	m.location
	--, s.ServerDescription
	, d.DatabaseId
	, d.ServerId
	, d.DatabaseName
	, df.DataMB, replace(format(df.DataMB,'N'),'.00','') DataMB_S 
	, df.LogMB
	, d.State_Desc
	, d.RecoveryModel
	, d.LogSizeKB
	, d.LogUsedKB
	, d.LogUsedPercentage
	, d.DBCompatibilityLevel
	, d.PageVerifyOption
	, d.is_query_store_on
	, d.is_auto_create_stats_on
	, d.is_auto_update_stats_on
	, d.is_auto_update_stats_async_on
	, d.is_parameterization_forced
	, d.snapshot_isolation_state_desc
	, d.is_read_committed_snapshot_on
	, d.is_auto_close_on
	, d.is_auto_shrink_on
	, d.target_recovery_time_in_seconds
	, d.Create_Date
	, d.is_published
	, d.is_subscribed
	, d.Collation
	, d.CachedSizeMbs
	, d.CPUTime
	, d.Is_Read_Only
	, d.delayed_durability_desc
	, d.containment_desc
	, d.is_cdc_enabled
	, d.is_broker_enabled
	, d.is_encrypted
	, d.is_memory_optimized_elevate_to_snapshot_on
	, d.AvailabilityGroup
	, d.PrimaryReplicaServerName
	, d.LocalReplicaRole
	, d.SynchronizationState
	, d.IsSuspended
	, d.IsJoined
	, d.SourceDatabaseName
	, d.owner
	, d.mirroring_state
	, d.mirroring_role
	, d.mirroring_safety_level
	, d.mirroring_partner
	, d.mirroring_partner_instance
	, d.mirroring_witness
	, d.mirroring_witness_state
	, d.mirroring_connection_timeout
	, d.mirroring_redo_queue
	, d.edition
	, d.service_objective
	, d.elastic_pool_name
	, az.SLO_Name, az.Service_Tier, az.Hardware_Gen, az.Logical_CPUs, az.Memory, az.DTUs, az.CPUs
	, fi.*
	, perm.*
	, o.*
	, il.GsyncReplication_LastSuccess
	, DATEDIFF(mi, il.GsyncReplication_LastSuccess, (SELECT MAX(date) FROM importlog) ) GsyncReplication_Lag
	, ra.MS_Published_Tables
	, rs.MS_Subscribed_Tables
	, i.*
	--select
FROM servers s
LEFT JOIN purpose p ON p.purposeid = s.purposeid
LEFT JOIN environment e ON e.environmentid = s.environmentid
JOIN databases d ON s.serverid=d.serverid
OUTER APPLY (
	SELECT SUM(CAST([rowCount] AS BIGINT)) Rows
		, SUM(CASE WHEN xtype='u' THEN 1 ELSE 0 END) Tables
		, SUM(CASE WHEN xtype='v' THEN 1 ELSE 0 END) Views
		, SUM(CASE WHEN xtype='p' THEN 1 ELSE 0 END) Procs
		, SUM(CASE WHEN xtype IN ('tf','fn') THEN 1 ELSE 0 END) Functions
		, SUM(CASE WHEN xtype='u' AND SchemaName = 'rpl' AND ObjectName LIKE 'del%' AND ObjectName NOT LIKE 'dates%' THEN 1 ELSE 0 END) Gsync_Published_Tables
		, SUM(CASE WHEN xtype='u' AND SchemaName = 'rpl' AND ObjectName LIKE 'stg%' AND ObjectName NOT LIKE 'dates%' THEN 1 ELSE 0 END) Gsync_Subscribed_Tables
		--select *
	FROM DatabaseObjects o
	WHERE o.DatabaseId = d.DatabaseId
) o
OUTER APPLY (
	SELECT SUM(1) MS_Published_Tables
	FROM [Articles] a
	JOIN publications p ON a.PublicationId= p.PublicationId
	WHERE p.ServerId = s.ServerId 
	AND p.publisher_db = d.DatabaseName
) RA
OUTER APPLY (
	SELECT SUM(1) MS_Subscribed_Tables
	FROM Subscriptions su
	WHERE su.subscriber_server = s.ServerName
	AND su.subscriber_db = d.DatabaseName
) RS
OUTER APPLY (
	SELECT SUM(1) Users
		, SUM( CASE WHEN DB_OWNER=1 THEN 1 ELSE 0 END) DBO_Users
	FROM [dbo].[DatabasePerms] perm
	WHERE perm.DatabaseId = d.DatabaseId 
) perm
OUTER APPLY (
	SELECT SUM(1) Files
		, COUNT(DISTINCT [filegroupname]) FileGroups
	FROM [dbo].[DatabaseFiles] fi
	WHERE fi.DatabaseId = d.DatabaseId 
) fi
OUTER APPLY (
	SELECT SUM(1) Indexes
		, SUM(i.data_size) Index_data_size
		, SUM(i.used_size) Index_used_size
	FROM IndexUsage i
	WHERE i.DatabaseId = d.DatabaseId
) i
OUTER APPLY (
	SELECT MAX(StartDate) GsyncReplication_LastSuccess FROM RplImportLog il
	WHERE il.databaseid = d.DatabaseId 
	AND il.Success = 1
	) il
OUTER APPLY (
	SELECT sum( case when filegroupname is not NULL then TotalMbs else 0 end) DataMb 
		, sum( case when filegroupname is NULL then TotalMbs else 0 end) LogMb
	FROM DatabaseFiles df
	WHERE df.databaseid = d.DatabaseId 
	) df

	left outer join master..sysservers m on m.srvname = s.ServerName
	left outer join SQLAzure az on az.databaseid = d.databaseid
	

GO
/****** Object:  View [dbo].[vwDatabaseObjects]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




CREATE view [dbo].[vwDatabaseObjects]
as
select d.ServerName, d.DatabaseName, do.SchemaName, do.ObjectName
	, do.Xtype
	, do.[RowCount], replace(format(do.[RowCount],'N'),'.00','') RowCount_S
	, do.ColCount, do.RowLength
	, do.ParentTable, do.ParentColumn
	, do.DatabaseObjectId, do.ServerId, do.DatabaseId
	, do.ReplColumns, do.HasCr_Dt, do.SQL_DATA_ACCESS, do.ROUTINE_DEFINITION, do.is_mspublished, do.is_rplpublished
	, do.is_rplsubscribed, do.is_disabled, do.parent_object_id, do.start_value, do.current_value, do.ParentSchema
	, case when r.article is null then 0 else 1 end as is_mssubscribed 
	, i.index_name PK, index_type PKType, i.size_mbs, i.writes, i.reads, i.cols PKCols
	, c.*
from databaseobjects do
join vwDatabases d on d.DatabaseId = do.DatabaseId
left outer join (
	 select a.source_owner, a.destination_object, s.subscriber_server, s.subscriber_db, a.article
	 from Articles a 
	 join subscriptions s on a.articleid = s.ArticleId
)r on r.destination_object = do.ObjectName and r.source_owner = do.SchemaName
	and r.subscriber_server = d.ServerName and r.subscriber_db = d.DatabaseName
outer apply (
	select top 1 * from IndexUsage i
	where i.DatabaseId = d.DatabaseId
	and i.table_schema = do.SchemaName
	and i.table_name = do.ObjectName
	and i.is_unique=1
	and do.Xtype in ('U','V')
	order by i.index_id
) i
outer apply (
	select count(*) ColumnsCount
		, sum(case when c.is_identity =1 then 1 else 0 end) HasIdentity
		, sum(case when c.is_computed =1 then 1 else 0 end) ComputerColumns
		, sum(case when CHARACTER_MAXIMUM_LENGTH=-1 then 1 else 0 end ) BlobColumns
	from DatabaseObjectColumns c
	where c.DatabaseObjectId = do.DatabaseObjectId
) c
where d.DatabaseName not in ('master','msdb')
	
GO
/****** Object:  View [dbo].[vwDatabaseObjectColumns]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO






CREATE view [dbo].[vwDatabaseObjectColumns]
as
select d.ServerName, d.DatabaseName, d.Xtype, d.[RowCount], d.RowCount_S, do.*
	, case when charindex(', '+do.COLUMN_NAME+',', ', '+pk.cols+',') > 0 then 1 else 0 end isKey
	, pk.index_name pkName, pk.cols pkCols
from databaseobjectColumns do
join vwDatabaseObjects d on d.DatabaseObjectId = do.DatabaseObjectId
outer apply (
	--get fisrt unique index and assume it the key
	select top 1 * from IndexUsage i
	where i.DatabaseId = do.DatabaseId
	and i.table_schema = do.TABLE_SCHEMA
	and i.table_name = do.TABLE_NAME
	and i.is_unique=1
	order by index_id 
) pk 

where d.DatabaseName not in ('master','msdb')



GO
/****** Object:  View [dbo].[vwMissingMask]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE view [dbo].[vwMissingMask]
as
	--which columns should be masked
	SELECT PurposeName, c.* 
	, 'exec ('''
		+ case when EngineEdition<>'SQL Azure' then 'use ['+DatabaseName+']' else '' end
		+'
		alter table  ['+table_schema + '].['+table_name+'] alter column ' + column_name +' add masked with (function = '''''
		+case when column_name like '%email%' then 'Email()'
			when column_name like '%email%' then 'Partial(4,"XXXX",3)'
			else 'Default()' end
		+''''')
		'') at ['+s.ServerName
	+ case when EngineEdition='SQL Azure' then '.'+DatabaseName else '' end
	+']' mask_cmd
	FROM vwDatabaseObjectColumns c
	join servers s on s.serverid = c.serverid 
	join Purpose p on p.PurposeId = s.PurposeId
	where masking_function is null
	and data_type like '%varchar%'
	and (column_name like '%email%' or column_name like '%phone%')
GO
/****** Object:  Table [dbo].[LongSql]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[LongSql](
	[LongSqlId] [int] IDENTITY(1,1) NOT NULL,
	[ServerId] [int] NULL,
	[event_timestamp] [datetime] NULL,
	[cpu_time] [bigint] NULL,
	[duration] [bigint] NULL,
	[physical_reads] [bigint] NULL,
	[logical_reads] [bigint] NULL,
	[writes] [bigint] NULL,
	[row_count] [bigint] NULL,
	[batch_text] [varchar](max) NULL,
	[client_app_name] [varchar](100) NULL,
	[client_hostname] [varchar](100) NULL,
	[database_name] [varchar](100) NULL,
	[nt_username] [varchar](100) NULL,
	[sql_text] [varchar](max) NULL,
	[isExported] [bit] NULL,
 CONSTRAINT [PK_LongSql] PRIMARY KEY CLUSTERED 
(
	[LongSqlId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  View [dbo].[vwLongSql]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE view [dbo].[vwLongSql]
as
select s.servername, s.purposeid, s.EnvironmentId
, d.LongSqlId
, d.ServerId
, d.event_timestamp
, d.cpu_time / 1000000 cpu_seconds
, d.duration / 1000000 seconds
, d.physical_reads physical_reads
, d.logical_reads logical_reads
, d.writes writes
, d.row_count rows
, d.batch_text
, d.client_app_name
, d.client_hostname
, d.database_name
, d.nt_username
, left(d.sql_text, 1000) sql_text
, 'http://servername/longsql_'+REPLICATE('0', 6-LEN(CAST(LongSqlId AS VARCHAR)) )+CAST(LongSqlId AS VARCHAR) +'.htm' Link
from LongSql d
join servers s on s.serverid = d.serverid



GO
/****** Object:  Table [dbo].[MissingIndexes]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[MissingIndexes](
	[MissingIndexId] [int] IDENTITY(1,1) NOT NULL,
	[ServerId] [int] NULL,
	[DatabaseId] [int] NULL,
	[index_advantage] [float] NULL,
	[last_user_seek] [datetime] NULL,
	[TableName] [varchar](500) NULL,
	[equality_columns] [varchar](4000) NULL,
	[inequality_columns] [varchar](4000) NULL,
	[included_columns] [varchar](4000) NULL,
	[unique_compiles] [bigint] NOT NULL,
	[user_seeks] [bigint] NOT NULL,
	[avg_total_user_cost] [float] NULL,
	[avg_user_impact] [float] NULL,
 CONSTRAINT [PK_MissingIndexes] PRIMARY KEY CLUSTERED 
(
	[MissingIndexId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  View [dbo].[vwMissingIndexes]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



CREATE view [dbo].[vwMissingIndexes]
as
with a as (
select s.servername, s.EngineEdition, d.databasename, t.* 
	, case when s.EngineEdition in ('Enterprise', 'SQL Azure', 'Managed Instance') then 'on' else 'off' end AllowOnline
	, replace(TableName, '['+DatabaseName+'].','') TableWithSchema
from MissingIndexes t 
join databases d on d.DatabaseId=t.DatabaseId
join servers s on s.ServerId = t.serverid
where equality_columns is not null
), b as (
	select *
		 , replace(replace(replace( TableWithSchema,'.','_'),']',''),'[','')  TableWithSchemaUnderscored
	from a
), c as (
select *
, substring(TableWithSchemaUnderscored, 1, charindex('_', TableWithSchemaUnderscored)-1) SchemaOnly
, substring(TableWithSchemaUnderscored, charindex('_', TableWithSchemaUnderscored)+1, len(TableWithSchemaUnderscored)) TableOnly
from b
)
select *
, 'exec ('''
+ case when EngineEdition<>'SQL Azure' then 'use ['+DatabaseName+']' else '' end
+'
create index idx_'+TableOnly
	+ '_'+replace(replace(replace(replace(equality_columns,']',''),'[',''),' ',''),',','_')  
	+' on '+TableWithSchema
	+ '('+equality_columns + isnull(','+inequality_columns,'')	+')'
	+ case when included_columns is not null then ' include ('+included_columns+')' else '' end
	+ ' WITH (FILLFACTOR=90, ONLINE='
	+ AllowOnline
	+', SORT_IN_TEMPDB=ON, PAD_INDEX = ON, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, DROP_EXISTING=OFF)
'') at ['+ServerName
+ case when EngineEdition='SQL Azure' then '.'+DatabaseName else '' end
+']' create_cmd
, 'select * from vwIndexUsage where servername='''+ServerName+''' and databasename='''+DatabaseName+''' and table_name='''+TableOnly+''' ' CheckExisting
from c
GO
/****** Object:  Table [dbo].[PerfMon]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[PerfMon](
	[PerfMonId] [int] IDENTITY(1,1) NOT NULL,
	[ServerId] [int] NULL,
	[ImportFileId] [int] NULL,
	[MetricDate] [date] NULL,
	[MetricTime] [time](7) NULL,
	[MemoryAvailableMBytes] [float] NULL,
	[PercentageProcessorTime] [float] NULL,
	[ForwardedRecordsPerSec] [float] NULL,
	[FullScansPerSec] [float] NULL,
	[IndexSearchesPerSec] [float] NULL,
	[PageLifeExpectancy] [float] NULL,
	[PageReadsPerSec] [float] NULL,
	[PageWritesPerSec] [float] NULL,
	[LazyWritesPerSec] [float] NULL,
	[C_AvgDiskBytesPerRead] [float] NULL,
	[C_AvgDiskBytesPerWrite] [float] NULL,
	[C_AvgDiskQueueLength] [float] NULL,
	[C_AvgDiskSecPerRead] [float] NULL,
	[C_AvgDiskSecPerWrite] [float] NULL,
	[D_AvgDiskBytesPerRead] [float] NULL,
	[D_AvgDiskBytesPerWrite] [float] NULL,
	[D_AvgDiskQueueLength] [float] NULL,
	[D_AvgDiskSecPerRead] [float] NULL,
	[D_AvgDiskSecPerWrite] [float] NULL,
 CONSTRAINT [PK_PerfMon] PRIMARY KEY CLUSTERED 
(
	[PerfMonId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  View [dbo].[vwPerMon]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
create view [dbo].[vwPerMon]
as
select s.servername, p.* 
from perfmon p
join servers s on s.serverid=p.serverid


GO
/****** Object:  Table [dbo].[Sequences]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Sequences](
	[ServerId] [int] NULL,
	[DatabaseId] [int] NULL,
	[SequenceName] [varchar](100) NULL,
	[Current_value] [bigint] NULL,
	[ParentTable] [varchar](100) NULL,
	[ParentColumn] [varchar](100) NULL,
	[maxExisting] [bigint] NULL,
	[NextInUse] [bigint] NULL,
	[IsMax] [bit] NULL,
	[Gap] [bigint] NULL
) ON [PRIMARY]
GO
/****** Object:  View [dbo].[vwSequenses]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE view [dbo].[vwSequenses]
as
select s.servername, d.databasename, q.* 
from Sequences q
join servers s on s.serverid=q.serverid
join Databases d on d.databaseid=q.databaseid

GO
/****** Object:  Table [dbo].[ServerPerms]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[ServerPerms](
	[ServerPermId] [int] IDENTITY(1,1) NOT NULL,
	[LoginId] [int] NULL,
	[Principal_Type] [varchar](50) NULL,
	[Security_Entity] [varchar](50) NULL,
	[Security_type] [varchar](50) NULL,
	[state_desc] [varchar](50) NULL,
	[ServerId] [int] NULL,
 CONSTRAINT [PK_ServerPerms] PRIMARY KEY CLUSTERED 
(
	[ServerPermId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Logins]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Logins](
	[LoginId] [int] IDENTITY(1,1) NOT NULL,
	[ServerId] [int] NULL,
	[LoginName] [varchar](100) NULL,
	[denylogin] [bit] NULL,
	[hasaccess] [bit] NULL,
	[isntname] [bit] NULL,
	[isntgroup] [bit] NULL,
	[isntuser] [bit] NULL,
	[sysadmin] [bit] NULL,
	[securityadmin] [bit] NULL,
	[serveradmin] [bit] NULL,
	[setupadmin] [bit] NULL,
	[processadmin] [bit] NULL,
	[diskadmin] [bit] NULL,
	[dbcreator] [bit] NULL,
	[bulkadmin] [bit] NULL,
	[SQLAgentOperatorRole] [bit] NULL,
	[SQLAgentReaderRole] [bit] NULL,
	[SQLAgentUserRole] [bit] NULL,
	[db_ssisadmin] [bit] NULL,
	[db_ssisltduser] [bit] NULL,
 CONSTRAINT [PK_Logins] PRIMARY KEY CLUSTERED 
(
	[LoginId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  View [dbo].[vwServerPerms]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE view [dbo].[vwServerPerms]
as
select p.*, e.*
	, s.ServerName
	, s.ServerDescription
	, l.LoginName
	, sp.*
from servers s
join purpose p on p.purposeid = s.purposeid
join environment e on e.environmentid = s.environmentid
join Logins l on l.serverid=s.serverid
join ServerPerms sp on sp.Loginid = l.loginid

GO
/****** Object:  Table [dbo].[Services]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Services](
	[ServiceId] [int] IDENTITY(1,1) NOT NULL,
	[ServerId] [int] NULL,
	[servicename] [varchar](512) NULL,
	[startup_type] [int] NULL,
	[startup_type_desc] [varchar](512) NULL,
	[status] [int] NULL,
	[status_desc] [varchar](512) NULL,
	[process_id] [int] NULL,
	[last_startup_time] [datetime] NULL,
	[service_account] [varchar](512) NULL,
	[filename] [varchar](512) NULL,
	[is_clustered] [varchar](5) NULL,
	[cluster_nodename] [varchar](512) NULL,
 CONSTRAINT [PK_Services] PRIMARY KEY CLUSTERED 
(
	[ServiceId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  View [dbo].[vwServices]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE view [dbo].[vwServices]
as
select p.*, e.*
	, s.ServerName
	, s.ServerDescription
	, j.*
from servers s
join purpose p on p.purposeid = s.purposeid
join environment e on e.environmentid = s.environmentid
left join Services j on s.serverid=j.serverid



GO
/****** Object:  Table [dbo].[JobHistory]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[JobHistory](
	[JobHistoryId] [int] IDENTITY(1,1) NOT NULL,
	[JobId] [int] NOT NULL,
	[job_name] [sysname] NOT NULL,
	[step_name] [sysname] NOT NULL,
	[message] [nvarchar](4000) NULL,
	[step_id] [int] NOT NULL,
	[subsystem] [nvarchar](40) NOT NULL,
	[command] [nvarchar](max) NULL,
	[output_file_name] [nvarchar](200) NULL,
	[RunDateTime] [datetime] NULL,
	[run_duration] [int] NOT NULL,
	[instance_id] [int] NOT NULL,
	[jobidentifier] [uniqueidentifier] NOT NULL,
	[run_status] [smallint] NULL,
	[Database_Name] [varchar](255) NULL,
	[database_user_name] [varchar](255) NULL,
	[ScheduleDscr] [varchar](255) NULL,
	[ServerID] [int] NULL,
 CONSTRAINT [PK_JobErrors] PRIMARY KEY CLUSTERED 
(
	[JobHistoryId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  View [dbo].[vwSqlMgtJobHistory]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



CREATE view [dbo].[vwSqlMgtJobHistory]
as
select  e.*
from JobHistory e 
where e.ServerId in (36,146,171)

GO
/****** Object:  Table [dbo].[QueryStoreLongestAvgTimes]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[QueryStoreLongestAvgTimes](
	[RowId] [int] IDENTITY(1,1) NOT NULL,
	[ServerId] [int] NULL,
	[DatabaseId] [int] NULL,
	[avg_duration] [int] NULL,
	[query_sql_text] [varchar](max) NULL,
	[query_id] [int] NULL,
	[query_text_id] [int] NULL,
	[plan_id] [int] NULL,
 CONSTRAINT [PK_QueryStoreLongestAvgTimes] PRIMARY KEY CLUSTERED 
(
	[RowId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  View [dbo].[vwQueryStoreLongestAvgTimes]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




create   view [dbo].[vwQueryStoreLongestAvgTimes]
as
select d.EnvironmentName, d.PurposeName, d.ServerName, d.IsActive, d.DailyChecks, d.DatabaseName, d.PurposeId, df.*
from QueryStoreLongestAvgTimes df
join vwDatabases d on d.DatabaseId = df.DatabaseId

GO
/****** Object:  Table [dbo].[TempConnections]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[TempConnections](
	[ServerId] [int] NULL,
	[App] [varchar](255) NULL,
	[Db] [varchar](255) NULL,
	[Host] [varchar](100) NULL,
	[Cnt] [int] NULL,
	[DateCreated] [datetime] NULL
) ON [PRIMARY]
GO
/****** Object:  View [dbo].[vwTempConnections]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create   view [dbo].[vwTempConnections]
as
select distinct 
	case 
		when App like 'SQL Agent%' then 'SQL Agent'
		when App like '%@%' then left (app, CHARINDEX('@',App)-1) 
		when App like '%:%' then left (app, CHARINDEX(':',App)-1) 
		when App like '%(%' then left (app, CHARINDEX('(',App)-1) 
		when App like '%.%' then left (app, CHARINDEX('.',App)-1) 
		when App like '%-%' then left (app, CHARINDEX('-',App)-1) 
		else app end App2
		, *
from TempConnections 
where app not in ('','sa')

GO
/****** Object:  Table [dbo].[TopSql]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[TopSql](
	[TopSqlId] [int] IDENTITY(1,1) NOT NULL,
	[ServerId] [int] NULL,
	[DatabaseId] [int] NULL,
	[SPName] [sysname] NOT NULL,
	[TotalWorkerTime] [bigint] NOT NULL,
	[AvgWorkerTime] [bigint] NULL,
	[execution_count] [bigint] NOT NULL,
	[CallsPerSecond] [bigint] NOT NULL,
	[total_elapsed_time] [bigint] NOT NULL,
	[avg_elapsed_time] [bigint] NULL,
	[cached_time] [datetime] NULL,
 CONSTRAINT [PK_TopSql] PRIMARY KEY CLUSTERED 
(
	[TopSqlId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  View [dbo].[vwTopSql]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create view [dbo].[vwTopSql]
as
select servername, databasename, t.* 
from topsql t 
join databases d on d.DatabaseId=t.DatabaseId
join servers s on s.ServerId = t.serverid

GO
/****** Object:  Table [dbo].[TopWait]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[TopWait](
	[TopWaitId] [int] IDENTITY(1,1) NOT NULL,
	[ServerId] [int] NULL,
	[wait_type] [nvarchar](60) NOT NULL,
	[wait_time_ms] [bigint] NOT NULL,
	[signal_wait_time_ms] [bigint] NOT NULL,
	[resource_wait_time_ms] [bigint] NULL,
	[percent_total_waits] [numeric](38, 15) NULL,
	[percent_total_signal_waits] [numeric](38, 15) NULL,
	[percent_total_resource_waits] [numeric](38, 15) NULL,
 CONSTRAINT [PK_TopWait] PRIMARY KEY CLUSTERED 
(
	[TopWaitId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  View [dbo].[vwTopWait]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


create view [dbo].[vwTopWait]
as
select servername, t.* 
from topWait t 
join servers s on s.ServerId = t.serverid



GO
/****** Object:  Table [dbo].[Volumes]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Volumes](
	[VolumeId] [int] IDENTITY(1,1) NOT NULL,
	[ServerId] [int] NULL,
	[volume_mount_point] [varchar](100) NULL,
	[TotalGB] [int] NULL,
	[AvailableGB] [int] NULL,
	[PercentageFree] [numeric](9, 2) NULL,
 CONSTRAINT [PK_Volumes] PRIMARY KEY CLUSTERED 
(
	[VolumeId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  View [dbo].[vwVolumes]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create view [dbo].[vwVolumes]
as
select p.*, e.*
	, s.ServerName
	, s.ServerDescription
	, v.*
from servers s
join purpose p on p.purposeid = s.purposeid
join environment e on e.environmentid = s.environmentid
join Volumes v on v.serverid=s.serverid


GO
/****** Object:  View [dbo].[vwLogins]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE view [dbo].[vwLogins]
as
select p.*, e.*
	, s.ServerName
	, s.ServerDescription
	, l.*
from servers s
left join purpose p on p.purposeid = s.purposeid
left join environment e on e.environmentid = s.environmentid
join Logins l on l.serverid = s.serverid



GO
/****** Object:  View [dbo].[vwSuspectSysadmins]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE view [dbo].[vwSuspectSysadmins]
as
select LoginName, ServerName
 from vwLogins
where sysadmin=1
and LoginName not in ('BUILTIN\Administrators', 'sa', 'NT AUTHORITY\NETWORK SERVICE')
--order by LoginName
GO
/****** Object:  Table [dbo].[Msdb_Backups]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Msdb_Backups](
	[RowId] [int] IDENTITY(1,1) NOT NULL,
	[ServerId] [int] NULL,
	[database_name] [varchar](255) NULL,
	[backup_start_date] [datetime] NULL,
	[backup_finish_date] [datetime] NULL,
	[expiration_date] [datetime] NULL,
	[backup_type] [varchar](20) NULL,
	[backup_size] [bigint] NULL,
	[logical_device_name] [varchar](255) NULL,
	[physical_device_name] [varchar](255) NULL,
	[backupset_name] [varchar](255) NULL,
	[description] [varchar](255) NULL,
	[databaseid] [int] NULL,
 CONSTRAINT [PK_Msdb_Backups] PRIMARY KEY CLUSTERED 
(
	[RowId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  View [dbo].[vwMsdb_Backups]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



CREATE   view [dbo].[vwMsdb_Backups]
as
select s.serverid, s.ServerName, s.BackupFolder, s.BackupChecks
	, d.DatabaseName, b.backup_start_date last_backup , b.backup_type
	, isnull(datediff(dd, backup_start_date, getdate()),30) DaysAgo 
from databases d 
join servers s on d.ServerId = s.ServerId
outer apply(select top 1 * from Msdb_Backups b
	where b.serverid = s.serverid
	and b.databaseid = d.databaseid
	AND b.backup_type IN ('Database','Database')
	order by b.backup_start_date desc) b
--where s.DailyChecks=1
--and d.state_desc = 'online'
--and s.ServerName = coalesce(d.PrimaryReplicaServerName,s.ServerName)
--and databasename not in ('master','msdb','reportserver','reportservertempdb','model','tempdb','SSISDB','RedGate','ChangeLog','DBATOOLS')
--and databasename not like 'distribution%' and databasename not like '%test%' and databasename not like '%[0-9][0-9]'
--and SourceDatabaseName is null
--and (b.RowId is null 	or 	backup_start_date < dateadd(dd, -3, getdate()))
	
GO
/****** Object:  View [dbo].[vwMissingMsdb_Backups]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
create   view [dbo].[vwMissingMsdb_Backups]
as
select  b.*
from databases d 
join servers s on d.ServerId = s.ServerId
outer apply(select top 1 * from vwMsdb_Backups b
	where b.serverid = s.serverid
	and b.databasename = d.DatabaseName
	order by b.DaysAgo desc) b
where s.DailyChecks=1
and d.state_desc = 'online'
and s.ServerName = coalesce(d.PrimaryReplicaServerName,s.ServerName)
and d.databasename not in ('master','msdb','reportserver','reportservertempdb','model','tempdb','SSISDB','RedGate','ChangeLog','DBATOOLS')
and d.databasename not like 'distribution%' and d.databasename not like '%test%' and d.databasename not like '%[0-9][0-9]'
and (backup_type is null or backup_type = 'Database')
and (b.serverid is null 
	or 	b.DaysAgo < s.BackupChecks)
	
GO
/****** Object:  View [dbo].[vwSqlMgtJobs]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE view [dbo].[vwSqlMgtJobs]
as
with j1 as (
	select s.serverid, s.ServerName
		, j.JobId
		, j.Jobname
		, j.ScheduleDscr
		, j.IsEnabled
		, replace(j.JobName,'<', '>') as JobName2 
	from jobs j
	join servers s on s.ServerId = j.ServerId
	where j.ServerId in (36,146,171)
) , j2 as (
	select  *
		 , CHARINDEX('>', JobName2) name_element2_start
		, CHARINDEX('>', JobName2, CHARINDEX('>',JobName2)+1) name_element3_start
	from j1 
), j3 as(
	select  *
		, case when name_element2_start < 3 then len(jobname2) else name_element2_start end name_element2_start2
		, case when name_element3_start < 3 then len(jobname2) else name_element3_start end name_element3_start2
	from j2
) , j4 as (
select 
	serverid
	, ServerName
	, JobId
	, Jobname
	, ScheduleDscr
	, IsEnabled 
	, substring (JobName2, 1, name_element2_start2-2) name_element1
	, ltrim(replace(substring (JobName2, name_element2_start2+1, name_element3_start2 - name_element2_start2),'>','')) name_element2
	, ltrim(replace(substring (JobName2, name_element3_start2+2, len(JobName2)),'>','')) name_element3 
 from j3 
)
select j4.*
	, ts.ServerName as TargetServer
	, ts.serverid TargetServerId
	, p.PurposeName
	, e.EnvironmentName
from j4
left join servers ts on ts.ServerName = name_element1
left join Purpose p on p.PurposeId = ts.PurposeId
left join Environment e on e.EnvironmentId = ts.EnvironmentId
GO
/****** Object:  View [dbo].[vwSqlMgtJobSteps]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE view [dbo].[vwSqlMgtJobSteps]
as
with js as (
select j.*
	, st.step_id
	, st.step_name
	, st.database_name
	, CHARINDEX('[', st.command) command_element1_start
	, CHARINDEX(']', st.command) command_element1_end
	, CHARINDEX('[', st.command, CHARINDEX('[',st.command)+1) command_element2_start
	, CHARINDEX(']', st.command, CHARINDEX(']',st.command)+1) command_element2_end
	, CHARINDEX('[', st.command, CHARINDEX('[',st.command, CHARINDEX('[',st.command)+1)+1) command_element3_start
	, CHARINDEX(']', st.command, CHARINDEX(']',st.command, CHARINDEX(']',st.command)+1)+1) command_element3_end
	, CHARINDEX('[', st.command, CHARINDEX('[', st.command, CHARINDEX('[',st.command, CHARINDEX('[',st.command)+1)+1)+1) command_element4_start
	, CHARINDEX(']', st.command, CHARINDEX(']', st.command, CHARINDEX(']',st.command, CHARINDEX(']',st.command)+1)+1)+1) command_element4_end
	, st.command
from [vwSqlMgtJobs] j
join JobSteps st on st.JobId = j.JobId
) 
select js.serverid	
	, js.ServerName	
	, js.JobId	
	, js.Jobname	
	, js.ScheduleDscr	
	, js.IsEnabled	
	, js.name_element1	
	, js.name_element2	
	, js.name_element3	
	, js.TargetServer	
	, js.TargetServerId	
	, js.PurposeName	
	, js.EnvironmentName	
	, js.step_id	
	, js.step_name	
	, js.database_name	
	, replace(substring (js.command, command_element1_start, command_element1_end - command_element1_start),'[','') command_element1
	, replace(substring (js.command, command_element2_start, command_element2_end - command_element2_start),'[','') command_element2
	, replace(substring (js.command, command_element3_start, command_element3_end - command_element3_start),'[','') command_element3
	, replace(substring (js.command, command_element4_start, command_element4_end - command_element4_start),'[','') command_element4
	, js.command
from js


GO
/****** Object:  View [dbo].[vwSqlMgtJobsFiltered]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
--select * from [vwSqlMgtJobs]
 
CREATE view [dbo].[vwSqlMgtJobsFiltered]
as
 select *
 from [vwSqlMgtJobs]
 where IsEnabled=1  
 and name_element2 not like '''BizTalk%'
 and name_element2 not like 'usp%'
 and name_element2 not like '''Backup%'
 and name_element2 not like 'Backup%'
 and name_element2 not in ('','bts_RebuildIndexes','Generate Script ','sp_cycle_errorlog',
 'NET USE \\DATADOMAIN','SiteCore Data Purge','Update MetaData'
 ,'SHRINK DATABASE','DEFRAG & STATS','ESBException Purge ')
GO
/****** Object:  View [dbo].[vwSqlMgtJobStepsFiltered]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE view [dbo].[vwSqlMgtJobStepsFiltered]
as
with js as (
select j.*
	, st.step_id
	, st.step_name
	, st.database_name
	, CHARINDEX('[', st.command) command_element1_start
	, CHARINDEX(']', st.command) command_element1_end
	, CHARINDEX('[', st.command, CHARINDEX('[',st.command)+1) command_element2_start
	, CHARINDEX(']', st.command, CHARINDEX(']',st.command)+1) command_element2_end
	, CHARINDEX('[', st.command, CHARINDEX('[',st.command, CHARINDEX('[',st.command)+1)+1) command_element3_start
	, CHARINDEX(']', st.command, CHARINDEX(']',st.command, CHARINDEX(']',st.command)+1)+1) command_element3_end
	, CHARINDEX('[', st.command, CHARINDEX('[', st.command, CHARINDEX('[',st.command, CHARINDEX('[',st.command)+1)+1)+1) command_element4_start
	, CHARINDEX(']', st.command, CHARINDEX(']', st.command, CHARINDEX(']',st.command, CHARINDEX(']',st.command)+1)+1)+1) command_element4_end
	, st.command
from [vwSqlMgtJobsFiltered] j
join JobSteps st on st.JobId = j.JobId
) 
select js.serverid	
	, js.ServerName	
	, js.JobId	
	, js.Jobname	
	, js.ScheduleDscr	
	, js.IsEnabled	
	, js.name_element1	
	, js.name_element2	
	, js.name_element3	
	, js.TargetServer	
	, js.TargetServerId	
	, js.PurposeName	
	, js.EnvironmentName	
	, js.step_id	
	, js.step_name	
	, js.database_name	
	, replace(substring (js.command, command_element1_start, command_element1_end - command_element1_start),'[','') command_element1
	, replace(substring (js.command, command_element2_start, command_element2_end - command_element2_start),'[','') command_element2
	, replace(substring (js.command, command_element3_start, command_element3_end - command_element3_start),'[','') command_element3
	, replace(substring (js.command, command_element4_start, command_element4_end - command_element4_start),'[','') command_element4
	, js.command
from js


GO
/****** Object:  Table [dbo].[IndexFragmentation]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[IndexFragmentation](
	[IndexFragmentationId] [int] IDENTITY(1,1) NOT NULL,
	[ServerId] [int] NULL,
	[DatabaseId] [int] NULL,
	[TableName] [varchar](128) NULL,
	[IndexName] [varchar](128) NULL,
	[index_type_desc] [varchar](20) NULL,
	[avg_fragmentation_in_percent] [decimal](9, 2) NULL,
	[fragment_count] [int] NULL,
	[page_count] [int] NULL,
	[SchemaName] [varchar](100) NULL,
 CONSTRAINT [PK_IndexFragmentation] PRIMARY KEY CLUSTERED 
(
	[IndexFragmentationId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  View [dbo].[vwIndexFragmentation]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




CREATE view [dbo].[vwIndexFragmentation]
as
select s.ServerName, d.DatabaseName
	, f.*
	, do.[RowCount], format(do.[RowCount], 'N') RowCount_S
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
	, allocation_desc
	, case when EngineEdition = 'SQL Azure' then 
		case when f.avg_fragmentation_in_percent between 0.1 and 0.3	then 'exec (''alter index ['+IndexName+'] on ['+f.SchemaName+'].['+f.TableName+'] REORGANIZE'') at ['+ServerName+'.'+DatabaseName+'] '
			 when f.avg_fragmentation_in_percent  > 0.3	then 'exec (''alter index ['+IndexName+'] on ['+f.SchemaName+'].['+f.TableName+'] REBUILD WITH (ONLINE = ON) '') at ['+ServerName+'.'+DatabaseName+'] '
			 else ''
		end  
	  else 
	    case when f.avg_fragmentation_in_percent between 0.1 and 0.3	then 'exec (''use ['+DatabaseName+'] alter index ['+IndexName+'] on ['+f.SchemaName+'].['+f.TableName+'] REORGANIZE'') at ['+ServerName+'] '
			 when f.avg_fragmentation_in_percent  > 0.3	then 'exec (''use ['+DatabaseName+'] alter index ['+IndexName+'] on ['+f.SchemaName+'].['+f.TableName+'] REBUILD WITH (SORT_IN_TEMPDB = ON, ONLINE = '+case when EngineEdition in ('Enterprise','Managed Instance') then 'ON' else 'OFF' end +')'') at ['+ServerName+'] '
			 else ''
		end
	end Maintenance
	--select top 100 *
from IndexFragmentation f
join Databases d on d.DatabaseId = f.DatabaseId
join servers s on s.ServerId = f.ServerId
left outer join DatabaseObjects do on do.DatabaseId = d.DatabaseId and do.SchemaName = f.schemaname and f.tablename = do.ObjectName and do.xtype='u'
left outer join IndexUsage iu on iu.DatabaseObjectId = do.DatabaseObjectId and iu.index_name = f.IndexName

GO
/****** Object:  View [dbo].[vwSqlBuilds]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE   VIEW [dbo].[vwSqlBuilds]
as
SELECT '2016' MajorVersion, '13.0.4474.0' Build, 'CU8 for Microsoft SQL Server 2016 SP1 (KB4077064)' Description, '2018 March 19' ReleaseDate  union all
select '2016' MajorVersion, '13.0.4466.4' Build, 'CU7 for Microsoft SQL Server 2016 SP1 (KB4057119)' Description, '2018 January 4' ReleaseDate  union all
select '2016' MajorVersion, '' Build, 'Security Update for SQL Server 2016 CU7 SP1 (KB4058561)' Description, '' ReleaseDate  union all
select '2016' MajorVersion, '13.0.4457.0' Build, 'CU6 for Microsoft SQL Server 2016 SP1 (KB4037354)' Description, '2017 November 21' ReleaseDate  union all
select '2016' MajorVersion, '13.0.4451.0' Build, 'CU5 for Microsoft SQL Server 2016 SP1 (KB4040714)' Description, '2017 September 18' ReleaseDate  union all
select '2016' MajorVersion, '13.0.4446.0' Build, 'CU4 for Microsoft SQL Server 2016 SP1 (KB4024305)' Description, '2017 August 8' ReleaseDate  union all
select '2016' MajorVersion, '13.0.4435.0' Build, 'CU3 for Microsoft SQL Server 2016 SP1 (KB4019916)' Description, '2017 May 15' ReleaseDate  union all
select '2016' MajorVersion, '13.0.4422.0' Build, 'CU2 for Microsoft SQL Server 2016 SP1 (KB4013106)' Description, '2017 March 20' ReleaseDate  union all
select '2016' MajorVersion, '13.0.4411.0' Build, 'CU1 for Microsoft SQL Server 2016 SP1 (KB3208177)' Description, '2017 January 18' ReleaseDate  union all
select '2016' MajorVersion, '13.0.4210.6' Build, 'Security Update for SQL Server 2016 Service Pack 1 GDR (KB4057118)' Description, '2018 January 4' ReleaseDate  union all
select '2016' MajorVersion, '13.0.4206.0' Build, 'Security Update for SQL Server 2016 Service Pack 1 GDR (KB4019089)' Description, '2017 August 8' ReleaseDate  union all
select '2016' MajorVersion, '13.0.4202.2' Build, 'GDR update package for SQL Server 2016 SP1 (KB3210089)' Description, '2016 December 16' ReleaseDate  union all
select '2016' MajorVersion, '13.0.4199.0' Build, 'FIX: Important update for SQL Server 2016 SP1 Reporting Services (KB3207512)' Description, '2016 November 23' ReleaseDate  union all
select '2016' MajorVersion, '13.0.4001.0' Build, 'SQL Server 2016 Service Pack 1 (SP1)' Description, '2016 November 16' ReleaseDate  union all
select '2016' MajorVersion, '13.0.2218.0' Build, 'Security Update for SQL Server 2016 RTM CU (KB4058559)' Description, '2018 January 8' ReleaseDate  union all
select '2016' MajorVersion, '13.0.2216.0' Build, 'CU9 for Microsoft SQL Server 2016 RTM (KB4037357)' Description, '2017 November 21' ReleaseDate  union all
select '2016' MajorVersion, '13.0.2213.0' Build, 'CU8 for Microsoft SQL Server 2016 RTM (KB4040713)' Description, '2017 September 18' ReleaseDate  union all
select '2016' MajorVersion, '13.0.2210.0' Build, 'CU7 for Microsoft SQL Server 2016 RTM (KB4024304)' Description, '2017 August 8' ReleaseDate  union all
select '2016' MajorVersion, '13.0.2204.0' Build, 'CU6 for Microsoft SQL Server 2016 RTM (KB4019914)' Description, '2017 May 15' ReleaseDate  union all
select '2016' MajorVersion, '13.0.2197.0' Build, 'CU5 for Microsoft SQL Server 2016 RTM (KB4013105)' Description, '2017 March 20' ReleaseDate  union all
select '2016' MajorVersion, '13.0.2193.0' Build, 'CU4 for Microsoft SQL Server 2016 RTM (KB3205052)' Description, '2017 January 18' ReleaseDate  union all
select '2016' MajorVersion, '13.0.2186.6' Build, 'MS16-136: CU3 for Microsoft SQL Server 2016 RTM (KB3205413)' Description, '2016 November 8' ReleaseDate  union all
select '2016' MajorVersion, '13.0.2170.0' Build, 'FIX: On-demand hotfix update package for SQL Server 2016 CU2 RTM (KB3199171)' Description, '2016 October 26' ReleaseDate  union all
select '2016' MajorVersion, '13.0.2169.0' Build, 'FIX: On-demand hotfix update package for SQL Server 2016 CU2 RTM (KB3195813)' Description, '2016 October 26' ReleaseDate  union all
select '2016' MajorVersion, '13.0.2164.0' Build, 'CU2 for Microsoft SQL Server 2016 RTM (KB3182270)' Description, '2016 September 22' ReleaseDate  union all
select '2016' MajorVersion, '13.0.2149.0' Build, 'CU1 for Microsoft SQL Server 2016 RTM (KB3164674)' Description, '2016 July 26' ReleaseDate  union all
select '2016' MajorVersion, '13.0.1745.2' Build, 'Security Update for SQL Server 2016 RTM GDR (KB4058560)' Description, '2018 January 8' ReleaseDate  union all
select '2016' MajorVersion, '13.0.1742.0' Build, 'Security Update for SQL Server 2016 RTM GDR (KB4019088)' Description, '2017 August 8' ReleaseDate  union all
select '2016' MajorVersion, '13.0.1728.2' Build, 'GDR update package for SQL Server 2016 RTM (KB3210111)' Description, '2016 December 16' ReleaseDate  union all
select '2016' MajorVersion, '13.0.1722.0' Build, 'MS16-136: Security Update for SQL Server 2016 GDR (KB3194716)' Description, '2016 November 8' ReleaseDate  union all
select '2016' MajorVersion, '13.0.1711.0' Build, 'FIX: Processing a partition causes data loss on other partitions after the database is restored in SQL Server 2016 (1200) (KB3179258)' Description, '2016 August 11' ReleaseDate  union all
select '2016' MajorVersion, '13.0.1708.0' Build, 'Critical update for SQL Server 2016 MSVCRT prerequisites' Description, '2016 June 3' ReleaseDate  union all
select '2016' MajorVersion, '13.0.1601.5' Build, 'SQL Server 2016 RTM' Description, '2016 June 1' ReleaseDate  union all
select '2016' MajorVersion, '13.0.1400.361' Build, 'SQL Server 2016 RC3 (Release Candidate 3)' Description, '2016 April 15' ReleaseDate  union all
select '2016' MajorVersion, '13.0.1300.275' Build, 'SQL Server 2016 RC2 (Release Candidate 2)' Description, '2016 April 1' ReleaseDate  union all
select '2016' MajorVersion, '13.0.1200.242' Build, 'SQL Server 2016 RC1 (Release Candidate 1)' Description, '2016 March 18' ReleaseDate  union all
select '2016' MajorVersion, '13.0.1100.288' Build, 'SQL Server 2016 RC0 (Release Candidate 0)' Description, '2016 March 7' ReleaseDate  union all
select '2016' MajorVersion, '13.0.1000.281' Build, 'SQL Server 2016 CTP 3.3 (Community Technology Preview 3.3)' Description, '2016 February 3' ReleaseDate  union all
select '2016' MajorVersion, '13.0.0900.73' Build, 'SQL Server 2016 CTP 3.2 (Community Technology Preview 3.2)' Description, '2015 December 16' ReleaseDate  union all
select '2016' MajorVersion, '13.0.0800.111' Build, 'SQL Server 2016 CTP 3.1 (Community Technology Preview 3.1)' Description, '2015 November 30' ReleaseDate  union all
select '2016' MajorVersion, '13.0.0700.1395' Build, 'SQL Server 2016 CTP 3.0 (Community Technology Preview 3.0)' Description, '2015 October 28' ReleaseDate  union all
select '2016' MajorVersion, '13.0.0600.65' Build, 'SQL Server 2016 CTP 2.4 (Community Technology Preview 2.4)' Description, '2015 September 30' ReleaseDate  union all
select '2016' MajorVersion, '13.0.0500.53' Build, 'SQL Server 2016 CTP 2.3 (Community Technology Preview 2.3)' Description, '2015 August 28' ReleaseDate  union all
select '2016' MajorVersion, '13.0.0407.1' Build, 'SQL Server 2016 CTP 2.2 (Community Technology Preview 2.2)' Description, '2015 July 29' ReleaseDate  union all
select '2016' MajorVersion, '13.0.0400.91' Build, 'SQL Server 2016 CTP 2.2 (Community Technology Preview 2.2) (replaced)' Description, '2015 July 22' ReleaseDate  union all
select '2016' MajorVersion, '13.0.0300.44' Build, 'SQL Server 2016 CTP 2.1 (Community Technology Preview 2.1)' Description, '2015 June 24' ReleaseDate  union all
select '2016' MajorVersion, '13.0.0200.172' Build, 'SQL Server 2016 CTP 2.0 (Community Technology Preview 2.0)' Description, '2015 May 27' ReleaseDate  union ALL


select '2014' MajorVersion, '12.0.5579.0' Build, 'CU11 for Microsoft SQL Server 2014 SP2 (KB4077063)' Description, '2018 March 19' ReleaseDate  union all
select '2014' MajorVersion, '12.0.5571.0' Build, 'CU10 for Microsoft SQL Server 2014 SP2 (KB4052725)' Description, '2018 January 16' ReleaseDate  union all
select '2014' MajorVersion, '' Build, 'Security Update for SQL Server 2014 SP2 CU (KB4057117)' Description, '' ReleaseDate  union all
select '2014' MajorVersion, '12.0.5563.0' Build, 'CU9 for Microsoft SQL Server 2014 SP2 (KB4055557)' Description, '2017 December 18' ReleaseDate  union all
select '2014' MajorVersion, '12.0.5557.0' Build, 'CU8 for Microsoft SQL Server 2014 SP2 (KB4037356)' Description, '2017 October 17' ReleaseDate  union all
select '2014' MajorVersion, '12.0.5556.0' Build, 'CU7 for Microsoft SQL Server 2014 SP2 (KB4032541)' Description, '2017 August 28' ReleaseDate  union all
select '2014' MajorVersion, '12.0.5553.0' Build, 'CU6 for Microsoft SQL Server 2014 SP2 (KB4019094)' Description, '2017 August 8' ReleaseDate  union all
select '2014' MajorVersion, '12.0.5546.0' Build, 'CU5 for Microsoft SQL Server 2014 SP2 (KB4013098)' Description, '2017 April 17' ReleaseDate  union all
select '2014' MajorVersion, '12.0.5540.0' Build, 'CU4 for Microsoft SQL Server 2014 SP2 (KB4010394)' Description, '2017 February 21' ReleaseDate  union all
select '2014' MajorVersion, '12.0.5538.0' Build, 'CU3 for Microsoft SQL Server 2014 SP2 (KB3204388)' Description, '2016 December 28' ReleaseDate  union all
select '2014' MajorVersion, '12.0.5532.0' Build, 'MS16-136: Security Update for SQL Server 2014 Service Pack 2 CU (KB3194718)' Description, '2016 November 8' ReleaseDate  union all
select '2014' MajorVersion, '12.0.5522.0' Build, 'CU2 for Microsoft SQL Server 2014 SP2 (KB3188778)' Description, '2016 October 17' ReleaseDate  union all
select '2014' MajorVersion, '12.0.5511.0' Build, 'CU1 for Microsoft SQL Server 2014 SP2 (KB3178925)' Description, '2016 August 26' ReleaseDate  union all
select '2014' MajorVersion, '12.0.5214.6' Build, 'Security Update for SQL Server 2014 Service Pack 2 GDR (KB4057120)' Description, '2018 January 16' ReleaseDate  union all
select '2014' MajorVersion, '12.0.5207.0' Build, 'Security Update for SQL Server 2014 Service Pack 2 GDR (KB4019093)' Description, '2017 August 8' ReleaseDate  union all
select '2014' MajorVersion, '12.0.5203.0' Build, 'MS16-136: Security Update for SQL Server 2014 Service Pack 2 GDR (KB3194714)' Description, '2016 November 8' ReleaseDate  union all
select '2014' MajorVersion, '12.0.5000.0' Build, 'Microsoft SQL Server 2014 Service Pack 2 (SP2)' Description, '2016 July 11' ReleaseDate  union all
select '2014' MajorVersion, '12.0.4522.0' Build, 'CU13 for Microsoft SQL Server 2014 SP1 (KB4019099)' Description, '2017 August 8' ReleaseDate  union all
select '2014' MajorVersion, '12.0.4511.0' Build, 'CU12 for Microsoft SQL Server 2014 SP1 (KB4017793)' Description, '2017 April 17' ReleaseDate  union all
select '2014' MajorVersion, '12.0.4502.0' Build, 'CU11 for Microsoft SQL Server 2014 SP1 (KB4010392)' Description, '2017 February 21' ReleaseDate  union all
select '2014' MajorVersion, '12.0.4491.0' Build, 'CU10 for Microsoft SQL Server 2014 SP1 (KB3204399)' Description, '2016 December 28' ReleaseDate  union all
select '2014' MajorVersion, '12.0.4487.0' Build, 'MS16-136: Security Update for SQL Server 2014 Service Pack 1 CU (KB3194722)' Description, '2016 November 8' ReleaseDate  union all
select '2014' MajorVersion, '12.0.4474.0' Build, 'CU9 for Microsoft SQL Server 2014 SP1 (KB3186964)' Description, '2016 October 17' ReleaseDate  union all
select '2014' MajorVersion, '12.0.4468.0' Build, 'CU8 for Microsoft SQL Server 2014 SP1 (KB3174038)' Description, '2016 August 15' ReleaseDate  union all
select '2014' MajorVersion, '12.0.4463.0' Build, 'FIX: A memory leak occurs when you use Azure Storage in SQL Server 2014 (KB3174370)' Description, '2016 August 4' ReleaseDate  union all
select '2014' MajorVersion, '12.0.4459.0' Build, 'CU7 for Microsoft SQL Server 2014 SP1 (KB3162659)' Description, '2016 June 20' ReleaseDate  union all
select '2014' MajorVersion, '12.0.4457.0' Build, 'CU6 (re-released) for Microsoft SQL Server 2014 SP1 (KB3167392)' Description, '2016 May 30' ReleaseDate  union all
select '2014' MajorVersion, '12.0.4449.0' Build, 'CU6 (replaced) for Microsoft SQL Server 2014 SP1 (KB3144524)' Description, '2016 April 18' ReleaseDate  union all
select '2014' MajorVersion, '12.0.4439.1' Build, 'CU5 for Microsoft SQL Server 2014 SP1 (KB3130926)' Description, '2016 February 21' ReleaseDate  union all
select '2014' MajorVersion, '12.0.4437.0' Build, 'On-demand hotfix update package for SQL Server 2014 SP1 CU4(KB3130999)' Description, '2016 February 5' ReleaseDate  union all
select '2014' MajorVersion, '12.0.4436.0' Build, 'CU4 for Microsoft SQL Server 2014 SP1 (KB3106660)' Description, '2015 December 22' ReleaseDate  union all
select '2014' MajorVersion, '12.0.4427.24' Build, 'CU3 for Microsoft SQL Server 2014 SP1 (KB3094221)' Description, '2015 October 20' ReleaseDate  union all
select '2014' MajorVersion, '12.0.4422.0' Build, 'CU2 for Microsoft SQL Server 2014 SP1 (KB3075950)' Description, '2015 August 17' ReleaseDate  union all
select '2014' MajorVersion, '12.0.4416.0' Build, 'CU1 for Microsoft SQL Server 2014 SP1 (KB3067839)' Description, '2015 June 22' ReleaseDate  union all
select '2014' MajorVersion, '12.0.4237.0' Build, 'Security Update for SQL Server 2014 Service Pack 1 GDR (KB4019091)' Description, '2017 August 8' ReleaseDate  union all
select '2014' MajorVersion, '12.0.4232.0' Build, 'MS16-136: Security Update for SQL Server 2014 Service Pack 1 GDR (KB3194720)' Description, '2016 November 8' ReleaseDate  union all
select '2014' MajorVersion, '12.0.4219.0' Build, 'TLS 1.2 support for Microsoft SQL Server 2014 SP1 GDR' Description, '2016 January 29' ReleaseDate  union all
select '2014' MajorVersion, '12.0.4213.0' Build, 'MS15-058: Nonsecurity update for SQL Server 2014 SP1 (GDR) (KB3070446)' Description, '2015 July 14' ReleaseDate  union all
select '2014' MajorVersion, '12.0.4100.1' Build, 'Microsoft SQL Server 2014 Service Pack 1' Description, '2015 May 15' ReleaseDate  union all
select '2014' MajorVersion, '12.0.4050.0' Build, '(Removed) Microsoft SQL Server 2014 SP1' Description, '2015 April 15' ReleaseDate  union all
select '2014' MajorVersion, '12.0.2569.0' Build, 'CU14 for Microsoft SQL Server 2014 (KB3158271)' Description, '2016 June 20' ReleaseDate  union all
select '2014' MajorVersion, '12.0.2568.0' Build, 'CU13 for Microsoft SQL Server 2014 (KB3144517)' Description, '2016 April 18' ReleaseDate  union all
select '2014' MajorVersion, '12.0.2564.0' Build, 'CU12 for Microsoft SQL Server 2014 (KB3130923)' Description, '2016 February 21' ReleaseDate  union all
select '2014' MajorVersion, '12.0.2560.0' Build, 'CU11 for Microsoft SQL Server 2014 (KB3106659)' Description, '2015 December 22' ReleaseDate  union all
select '2014' MajorVersion, '12.0.2556.4' Build, 'CU10 for Microsoft SQL Server 2014 (KB3094220)' Description, '2015 October 20' ReleaseDate  union all
select '2014' MajorVersion, '12.0.2553.0' Build, 'CU9 for Microsoft SQL Server 2014 (KB3075949)' Description, '2015 August 17' ReleaseDate  union all
select '2014' MajorVersion, '12.0.2548.0' Build, 'MS15-058: Security update for SQL Server 2014 (QFE) (KB3045323)' Description, '2015 July 14' ReleaseDate  union all
select '2014' MajorVersion, '12.0.2546.0' Build, 'CU8 for Microsoft SQL Server 2014 (KB3067836)' Description, '2015 June 22' ReleaseDate  union all
select '2014' MajorVersion, '12.0.2495.0' Build, 'CU7 for Microsoft SQL Server 2014 (KB3046038)' Description, '2015 April 23' ReleaseDate  union all
select '2014' MajorVersion, '12.0.2480.0' Build, 'CU6 for Microsoft SQL Server 2014 (KB3031047)' Description, '2015 February 16' ReleaseDate  union all
select '2014' MajorVersion, '12.0.2474.0' Build, 'FIX: AlwaysOn availability groups are reported as NOT SYNCHRONIZING (KB3034679)' Description, '2014 February 4' ReleaseDate  union all
select '2014' MajorVersion, '12.0.2456.0' Build, 'CU5 for Microsoft SQL Server 2014 (KB3011055)' Description, '2014 December 18' ReleaseDate  union all
select '2014' MajorVersion, '12.0.2430.0' Build, 'CU4 for Microsoft SQL Server 2014 (KB2999197)' Description, '2014 October 21' ReleaseDate  union all
select '2014' MajorVersion, '12.0.2402.0' Build, 'CU3 for Microsoft SQL Server 2014 (KB2984923)' Description, '2014 August 18' ReleaseDate  union all
select '2014' MajorVersion, '12.0.2381.0' Build, 'MS14-044: Security update for SQL Server 2014 (QFE) (KB2977316)' Description, '2014 August 12' ReleaseDate  union all
select '2014' MajorVersion, '12.0.2370.0' Build, 'CU2 for Microsoft SQL Server 2014 (KB2967546)' Description, '2014 June 27' ReleaseDate  union all
select '2014' MajorVersion, '12.0.2342.0' Build, 'CU1 for Microsoft SQL Server 2014 (KB2931693)' Description, '2014 April 21' ReleaseDate  union all
select '2014' MajorVersion, '12.0.2271.0' Build, 'TLS 1.2 support for Microsoft SQL Server 2014 GDR' Description, '2016 January 29' ReleaseDate  union all
select '2014' MajorVersion, '12.0.2269.0' Build, 'MS15-058: Security Update for SQL Server 2014 (GDR) (KB3045324)' Description, '2015 July 14' ReleaseDate  union all
select '2014' MajorVersion, '12.0.2254.0' Build, 'MS14-044: Security Update for SQL Server 2014 (GDR) (KB2977315)' Description, '2014 August 12' ReleaseDate  union all
select '2014' MajorVersion, '12.0.2000.0' Build, 'Microsoft SQL Server 2014 RTM' Description, '2014 April 1' ReleaseDate  union all
select '2012' MajorVersion, '11.0.7462.6' Build, 'Security Update for SQL Server 2012 SP4 GDR (KB4057116)' Description, '2018 January 12' ReleaseDate  union all
select '2012' MajorVersion, '11.0.7001.0' Build, 'SQL Server 2012 Service Pack 4 (KB4018073)' Description, '2017 October 5' ReleaseDate  union all
select '2012' MajorVersion, '11.0.6615.2' Build, 'Security Update for SQL Server 2012 SP3 CU (KB4057121)' Description, '2018 January 16' ReleaseDate  union all
select '2012' MajorVersion, '11.0.6607.3' Build, 'CU10 for Microsoft SQL Server 2012 SP3 (KB4025925)' Description, '2017 August 8' ReleaseDate  union all
select '2012' MajorVersion, '11.0.6598.0' Build, 'CU9 for Microsoft SQL Server 2012 SP3 (KB4016762)' Description, '2017 May 15' ReleaseDate  union ALL

select '2012' MajorVersion, '11.0.6594.0' Build, 'CU8 for Microsoft SQL Server 2012 SP3 (KB4013104)' Description, '2017 March 21' ReleaseDate  union all
select '2012' MajorVersion, '11.0.6579.0' Build, 'CU7 for Microsoft SQL Server 2012 SP3 (KB3205051)' Description, '2017 January 18' ReleaseDate  union all
select '2012' MajorVersion, '11.0.6567.0' Build, 'MS16-136: CU6 for Microsoft SQL Server 2012 SP3 (KB3194992)' Description, '2016 November 8' ReleaseDate  union all
select '2012' MajorVersion, '11.0.6544.0' Build, 'CU5 for Microsoft SQL Server 2012 SP3 (KB3180915)' Description, '2016 September 20' ReleaseDate  union all
select '2012' MajorVersion, '11.0.6540.0' Build, 'CU4 for Microsoft SQL Server 2012 SP3 (KB3165264)' Description, '2016 July 18' ReleaseDate  union all
select '2012' MajorVersion, '11.0.6537.0' Build, 'CU3 for Microsoft SQL Server 2012 SP3 (KB3152635)' Description, '2016 May 15' ReleaseDate  union all
select '2012' MajorVersion, '11.0.6523.0' Build, 'CU2 for Microsoft SQL Server 2012 SP3 (KB3137746)' Description, '2016 March 22' ReleaseDate  union all
select '2012' MajorVersion, '11.0.6518.0' Build, 'CU1 for Microsoft SQL Server 2012 SP3 (KB3123299)' Description, '2016 January 19' ReleaseDate  union all
select '2012' MajorVersion, '11.0.6260.1' Build, 'Security Update for SQL Server 2012 Service Pack 3 GDR (KB4057115)' Description, '2018 January 16' ReleaseDate  union all
select '2012' MajorVersion, '11.0.6251.0' Build, 'Security Update for SQL Server 2012 Service Pack 3 GDR (KB4019092)' Description, '2017 August 8' ReleaseDate  union all
select '2012' MajorVersion, '11.0.6248.0' Build, 'MS16-136: Security Update for SQL Server 2012 Service Pack 3 GDR (KB3194721)' Description, '2016 November 8' ReleaseDate  union all
select '2012' MajorVersion, '11.0.6216.27' Build, 'TLS 1.2 support for Microsoft SQL Server 2012 SP3 GDR' Description, '2016 January 29' ReleaseDate  union all
select '2012' MajorVersion, '11.0.6020' Build, 'SQL Server 2012 Service Pack 3 (KB3072779)' Description, '2015 November 21' ReleaseDate  union all
select '2012' MajorVersion, '11.0.5678.0' Build, 'CU16 for Microsoft SQL Server 2012 SP2 (KB3205054)' Description, '2017 January 18' ReleaseDate  union all
select '2012' MajorVersion, '11.0.5676.0' Build, 'MS16-136: CU15 for Microsoft SQL Server 2012 SP2 (KB3205416)' Description, '2016 November 8' ReleaseDate  union all
select '2012' MajorVersion, '11.0.5657.0' Build, 'CU14 for Microsoft SQL Server 2012 SP2 (KB3180914)' Description, '2016 September 20' ReleaseDate  union all
select '2012' MajorVersion, '11.0.5655.0' Build, 'CU13 for Microsoft SQL Server 2012 SP2 (KB3165266)' Description, '2016 July 18' ReleaseDate  union all
select '2012' MajorVersion, '11.0.5649.0' Build, 'CU12 for Microsoft SQL Server 2012 SP2 (KB3152637)' Description, '2016 May 15' ReleaseDate  union all
select '2012' MajorVersion, '11.0.5646.0' Build, 'CU11 for Microsoft SQL Server 2012 SP2 (KB3137745)' Description, '2016 March 22' ReleaseDate  union all
select '2012' MajorVersion, '11.0.5644.2' Build, 'CU10 for Microsoft SQL Server 2012 SP2 (KB3120313)' Description, '2016 January 19' ReleaseDate  union all
select '2012' MajorVersion, '11.0.5641' Build, 'CU9 for Microsoft SQL Server 2012 SP2 (KB3098512)' Description, '2015 November 18' ReleaseDate  union all
select '2012' MajorVersion, '11.0.5634.1' Build, 'CU8 for Microsoft SQL Server 2012 SP2 (KB3082561)' Description, '2015 September 21' ReleaseDate  union all
select '2012' MajorVersion, '11.0.5623' Build, 'CU7 for Microsoft SQL Server 2012 SP2 (KB3072100)' Description, '2015 July 20' ReleaseDate  union all
select '2012' MajorVersion, '11.0.5613' Build, 'MS15-058: Security Update for SQL Server 2012 SP2 QFE (KB3045319)' Description, '2015 July 14' ReleaseDate  union all
select '2012' MajorVersion, '11.0.5592' Build, 'CU6 for Microsoft SQL Server 2012 SP2 (KB3052468)' Description, '2015 May 18' ReleaseDate  union all
select '2012' MajorVersion, '11.0.5582' Build, 'CU5 for Microsoft SQL Server 2012 SP2 (KB3037255)' Description, '2015 March 16' ReleaseDate  union all
select '2012' MajorVersion, '11.0.5571' Build, 'FIX: AlwaysOn availability groups are reported as NOT SYNCHRONIZING (KB3034679)' Description, '2015 February 4' ReleaseDate  union all
select '2012' MajorVersion, '11.0.5569' Build, 'CU4 for Microsoft SQL Server 2012 SP2 (KB3007556)' Description, '2015 January 20' ReleaseDate  union all
select '2012' MajorVersion, '11.0.5556' Build, 'CU3 for Microsoft SQL Server 2012 SP2 (KB3002049)' Description, '2014 November 17' ReleaseDate  union all
select '2012' MajorVersion, '11.0.5548' Build, 'CU2 for Microsoft SQL Server 2012 SP2 (KB2983175)' Description, '2014 September 15' ReleaseDate  union all
select '2012' MajorVersion, '11.0.5532' Build, 'CU1 for Microsoft SQL Server 2012 SP2 (KB2976982)' Description, '2014 July 23' ReleaseDate  union all
select '2012' MajorVersion, '11.0.5522' Build, 'FIX for SQL Server 2012 SP2: Data loss in clustered index (KB2969896)' Description, '2014 June 20' ReleaseDate  union all
select '2012' MajorVersion, '11.0.5388.0' Build, 'MS16-136: Security Update for SQL Server 2012 Service Pack 2 GD2 (KB3194719)' Description, '2016 November 8' ReleaseDate  union all
select '2012' MajorVersion, '11.0.5352' Build, 'TLS 1.2 support for Microsoft SQL Server 2012 SP2 GDR' Description, '2016 January 29' ReleaseDate  union all
select '2012' MajorVersion, '11.0.5343' Build, 'MS15-058: Security Update for SQL Server 2012 SP2 GDR (KB3045321)' Description, '2014 July 14' ReleaseDate  union all
select '2012' MajorVersion, '11.0.5058' Build, 'SQL Server 2012 Service Pack 2 (KB2958429)' Description, '2014 June 10' ReleaseDate  union all
select '2012' MajorVersion, '11.0.3513' Build, 'MS15-058: Security Update for SQL Server 2012 SP1 QFE (KB3045317)' Description, '2015 July 14' ReleaseDate  union all
select '2012' MajorVersion, '11.0.3492' Build, 'CU16 for Microsoft SQL Server 2012 SP1 (KB3052476)' Description, '2015 May 18' ReleaseDate  union all
select '2012' MajorVersion, '11.0.3487' Build, 'CU15 for Microsoft SQL Server 2012 SP1 (KB3038001)' Description, '2015 March 16' ReleaseDate  union all
select '2012' MajorVersion, '11.0.3486' Build, 'CU14 for Microsoft SQL Server 2012 SP1 (KB3007556)' Description, '2015 January 21' ReleaseDate  union all
select '2012' MajorVersion, '11.0.3482' Build, 'CU13 for Microsoft SQL Server 2012 SP1 (KB3002044)' Description, '2014 November 17' ReleaseDate  union all
select '2012' MajorVersion, '11.0.3470' Build, 'CU12 for Microsoft SQL Server 2012 SP1 (KB2975396)' Description, '2014 September 15' ReleaseDate  union all
select '2012' MajorVersion, '11.0.3467' Build, 'FIX: Log Reader Agent crashes during initialization when you use transactional replication in SQL Server(KB2975402)' Description, '2014 August 28' ReleaseDate  union all
select '2012' MajorVersion, '11.0.3460' Build, 'MS14-044: Security Update for Microsoft SQL Server 2012 SP1 (QFE)(KB2977325)' Description, '2014 August 12' ReleaseDate  union all
select '2012' MajorVersion, '11.0.3449' Build, 'CU11 for Microsoft SQL Server 2012 SP1 (KB2975396)' Description, '2014 July 21' ReleaseDate  union all
select '2012' MajorVersion, '11.0.3437' Build, 'FIX for SQL Server 2012 SP1: Data loss in clustered index (KB2969896)' Description, '2014 June 10' ReleaseDate  union all
select '2012' MajorVersion, '11.0.3431' Build, 'CU10 for Microsoft SQL Server 2012 SP1 (KB2954099)' Description, '2014 May 19' ReleaseDate  union all
select '2012' MajorVersion, '11.0.3412' Build, 'CU9 for Microsoft SQL Server 2012 SP1 (KB2931078)' Description, '2014 March 18' ReleaseDate  union all
select '2012' MajorVersion, '11.0.3401' Build, 'CU8 for Microsoft SQL Server 2012 SP1 (KB2917531)' Description, '2014 January 20' ReleaseDate  union all
select '2012' MajorVersion, '11.0.3393' Build, 'CU7 for Microsoft SQL Server 2012 SP1 (KB2894115)' Description, '2013 November 18' ReleaseDate  union all
select '2012' MajorVersion, '11.0.3381' Build, 'CU6 for Microsoft SQL Server 2012 SP1 (KB2874879)' Description, '2013 September 16' ReleaseDate  union all
select '2012' MajorVersion, '11.0.3373' Build, 'CU5 for Microsoft SQL Server 2012 SP1 (KB2861107)' Description, '2013 July 16' ReleaseDate  union all
select '2012' MajorVersion, '11.0.3368' Build, 'CU4 for Microsoft SQL Server 2012 SP1 (KB2833645)' Description, '2013 May 31' ReleaseDate  union all
select '2012' MajorVersion, '11.0.3349' Build, 'CU3 for Microsoft SQL Server 2012 SP1 (KB2812412)' Description, '2013 March 18' ReleaseDate  union all
select '2012' MajorVersion, '11.0.3339' Build, 'CU2 for Microsoft SQL Server 2012 SP1 (KB2790947)' Description, '2013 January 25' ReleaseDate  union all
select '2012' MajorVersion, '11.0.3321' Build, 'CU1 for Microsoft SQL Server 2012 SP1 (KB2765331)' Description, '2012 November 20' ReleaseDate  union all
select '2012' MajorVersion, '11.0.3156' Build, 'MS15-058: Security Update for SQL Server 2012 SP1 GDR (KB3045318)' Description, '2015 July 14' ReleaseDate  union all
select '2012' MajorVersion, '11.0.3153' Build, 'MS14-044: Security Update for SQL Server 2012 SP1 GDR (KB2977326)' Description, '2014 August 12' ReleaseDate  union all
select '2012' MajorVersion, '11.0.3128' Build, 'FIX: Windows Installer starts repeatedly after you install SQL Server 2012 SP1 (KB2793634)' Description, '2013 January 3' ReleaseDate  union all
select '2012' MajorVersion, '11.0.3000' Build, 'SQL Server 2012 Service Pack 1 (KB2674319)' Description, '2012 November 6' ReleaseDate  union all
select '2012' MajorVersion, '11.0.2424' Build, 'CU11 for Microsoft SQL Server 2012 (KB2908007)' Description, '2013 December 17' ReleaseDate  union all
select '2012' MajorVersion, '11.0.2420' Build, 'CU10 for Microsoft SQL Server 2012 (KB2891666)' Description, '2013 October 21' ReleaseDate  union all
select '2012' MajorVersion, '11.0.2419' Build, 'CU9 for Microsoft SQL Server 2012 (KB2867319)' Description, '2013 August 21' ReleaseDate  union all
select '2012' MajorVersion, '11.0.2410' Build, 'CU8 for Microsoft SQL Server 2012 (KB2844205)' Description, '2013 June 18' ReleaseDate  union all
select '2012' MajorVersion, '11.0.2405' Build, 'CU7 for Microsoft SQL Server 2012 (KB2823247)' Description, '2013 April 15' ReleaseDate  union all
select '2012' MajorVersion, '11.0.2401' Build, 'CU6 for Microsoft SQL Server 2012 (KB2728897)' Description, '2013 February 18' ReleaseDate  union all
select '2012' MajorVersion, '11.0.2395' Build, 'CU5 for Microsoft SQL Server 2012 (KB2777772)' Description, '2012 December 18' ReleaseDate  union all
select '2012' MajorVersion, '11.0.2383' Build, 'CU4 for Microsoft SQL Server 2012 (KB2758687)' Description, '2012 October 18' ReleaseDate  union all
select '2012' MajorVersion, '11.0.2376' Build, 'MS12-070: Security Update for SQL Server 2012 QFE (KB2716441)' Description, '2012 October 9' ReleaseDate  union all
select '2012' MajorVersion, '11.0.2332' Build, 'CU3 for Microsoft SQL Server 2012 (KB2723749)' Description, '2012 August 29' ReleaseDate  union all
select '2012' MajorVersion, '11.0.2325' Build, 'CU2 for Microsoft SQL Server 2012 (KB2703275)' Description, '2012 June 18' ReleaseDate  union all
select '2012' MajorVersion, '11.0.2316' Build, 'CU1 for Microsoft SQL Server 2012 (KB2679368)' Description, '2012 April 12' ReleaseDate  union all
select '2012' MajorVersion, '11.0.2218' Build, 'MS12-070: Security Update for SQL Server 2012 GDR (KB2716442)' Description, '2012 October 9' ReleaseDate  union ALL
select '2012' MajorVersion, '11.0.5058.0' Build, '' Description, 'May 14 2014' ReleaseDate  union ALL


select '2008R2' MajorVersion, '10.50.6560' Build, 'Security Update for SQL Server 2008 SP3 R2 GDR (KB4057113)' Description, '2018 January 6' ReleaseDate  union all
select '2008R2' MajorVersion, '10.50.6542' Build, 'TLS 1.2 support for Microsoft SQL Server 2008 R2 SP3 (updated)' Description, '2016 March 3' ReleaseDate  union all
select '2008R2' MajorVersion, '10.50.6537' Build, 'TLS 1.2 support for Microsoft SQL Server 2008 R2 SP3 (replaced).' Description, '2016 January 29' ReleaseDate  union all
select '2008R2' MajorVersion, '' Build, 'See KB3146034' Description, '' ReleaseDate  union all
select '2008R2' MajorVersion, '10.50.6529' Build, 'MS15-058: Security Update for SQL Server 2008R2 SP3 QFE (KB3045314)' Description, '2015 July 14' ReleaseDate  union all
select '2008R2' MajorVersion, '10.50.6525' Build, 'FIX: On-demand Hotfix Update Package for SQL Server 2008R2 SP3 (KB3033860)' Description, '2015 February 9' ReleaseDate  union all
select '2008R2' MajorVersion, '10.50.6220' Build, 'MS15-058: Security Update for SQL Server 2008R2 SP3 GDR (KB3045316)' Description, '2015 July 14' ReleaseDate  union all
select '2008R2' MajorVersion, '10.50.6000' Build, 'SQL Server 2008R2 Service Pack 3' Description, '2014 September 26' ReleaseDate  union all
select '2008R2' MajorVersion, '10.50.4344' Build, 'TLS 1.2 support for SQL Server 2008 R2 SP2 (IA-64) (updated)' Description, '2016 March 3' ReleaseDate  union all
select '2008R2' MajorVersion, '10.50.4343' Build, 'TLS 1.2 support for SQL Server 2008 R2 SP2 (IA-64) (replaced)' Description, '2016 January 29' ReleaseDate  union all
select '2008R2' MajorVersion, '' Build, 'See KB3146034' Description, '' ReleaseDate  union all
select '2008R2' MajorVersion, '10.50.4339' Build, 'MS15-058: Security Update for SQL Server 2008R2 SP2 QFE (KB3045312)' Description, '2015 July 14' ReleaseDate  union all
select '2008R2' MajorVersion, '10.50.4331' Build, 'MS14-044: Security Update for SQL Server 2008R2 SP2 QFE (KB2977319)' Description, '2014 August 12' ReleaseDate  union all
select '2008R2' MajorVersion, '10.50.4319' Build, 'CU13 for Microsoft SQL Server 2008R2 SP2(KB2967540)' Description, '2014 June 30' ReleaseDate  union all
select '2008R2' MajorVersion, '10.50.4305' Build, 'CU12 for Microsoft SQL Server 2008R2 SP2(KB2938478)' Description, '2014 April 21' ReleaseDate  union all
select '2008R2' MajorVersion, '10.50.4302' Build, 'CU11 for Microsoft SQL Server 2008R2 SP2(KB2926028)' Description, '2014 February 18' ReleaseDate  union all
select '2008R2' MajorVersion, '10.50.4297' Build, 'CU10 for Microsoft SQL Server 2008R2 SP2(KB2908087)' Description, '2013 December 16' ReleaseDate  union all
select '2008R2' MajorVersion, '10.50.4295' Build, 'CU9 for Microsoft SQL Server 2008R2 SP2(KB2887606)' Description, '2013 October 29' ReleaseDate  union all
select '2008R2' MajorVersion, '10.50.4290' Build, 'CU8 for Microsoft SQL Server 2008R2 SP2(KB2871401)' Description, '2013 August 30' ReleaseDate  union all
select '2008R2' MajorVersion, '10.50.4286' Build, 'CU7 for Microsoft SQL Server 2008R2 SP2(KB2844090)' Description, '2013 June 17' ReleaseDate  union all
select '2008R2' MajorVersion, '10.50.4285' Build, 'CU6 re-released for Microsoft SQL Server 2008R2 SP2(KB2830140)' Description, '2013 June 13' ReleaseDate  union all
select '2008R2' MajorVersion, '10.50.4279' Build, 'CU6 (replaced) for Microsoft SQL Server 2008R2 SP2(KB2830140)' Description, '2013 April 15' ReleaseDate  union all
select '2008R2' MajorVersion, '10.50.4276' Build, 'CU5 for Microsoft SQL Server 2008R2 SP2(KB2797460)' Description, '2013 February 18' ReleaseDate  union all
select '2008R2' MajorVersion, '10.50.4270' Build, 'CU4 for Microsoft SQL Server 2008R2 SP2(KB2777358)' Description, '2012 December 17' ReleaseDate  union all
select '2008R2' MajorVersion, '10.50.4266' Build, 'CU3 for Microsoft SQL Server 2008R2 SP2(KB2754552)' Description, '2012 October 15' ReleaseDate  union all
select '2008R2' MajorVersion, '10.50.4263' Build, 'CU2 for Microsoft SQL Server 2008R2 SP2(KB2740411)' Description, '2012 August 29' ReleaseDate  union all
select '2008R2' MajorVersion, '10.50.4260' Build, 'CU1 for Microsoft SQL Server 2008R2 SP2(KB2720425)' Description, '2012 August 1' ReleaseDate  union all
select '2008R2' MajorVersion, '10.50.4047' Build, 'TLS 1.2 support for SQL Server 2008 R2 SP2 GDR (IA-64) (updated)' Description, '2016 March 3' ReleaseDate  union all
select '2008R2' MajorVersion, '10.50.4046' Build, 'TLS 1.2 support for SQL Server 2008 R2 SP2 GDR (IA-64) (replaced)' Description, '2016 January 29' ReleaseDate  union ALL
select '2008R2' MajorVersion, '10.50.6000.34' Build, '(SP4)' Description, 'Aug 19 2014' ReleaseDate  union ALL

select '2008R2' MajorVersion, '' Build, 'See KB3146034' Description, '' ReleaseDate  union all
select '2008R2' MajorVersion, '10.50.4042' Build, 'MS15-058: Security Update for SQL Server 2008R2 SP2 GDR (KB3045313)' Description, '2015 July 14' ReleaseDate  union all
select '2008R2' MajorVersion, '10.50.4033' Build, 'MS14-044: Security Update for SQL Server 2008R2 SP2 GDR (KB2977320)' Description, '2014 August 12' ReleaseDate  union all
select '2008R2' MajorVersion, '10.50.4000' Build, 'SQL Server 2008R2 Service Pack 2' Description, '2012 July 26' ReleaseDate  union all
select '2008R2' MajorVersion, '10.50.2500' Build, 'SQL Server 2008R2 Service Pack 1' Description, '2011 July 11' ReleaseDate  union all
select '2008R2' MajorVersion, '10.50.1600' Build, 'SQL Server 2008R2 RTM' Description, '2010 April 21' ReleaseDate  union ALL

select '2008' MajorVersion, '10.00.6556' Build, 'Security Update for SQL Server 2008 SP4 GDR (KB4057114)' Description, '2018 January 6' ReleaseDate  union all
select '2008' MajorVersion, '10.00.6547' Build, 'TLS 1.2 support for Microsoft SQL Server 2008 SP4 (updated)' Description, '2016 March 3' ReleaseDate  union all
select '2008' MajorVersion, '10.00.6543' Build, 'TLS 1.2 support for Microsoft SQL Server 2008 SP4 (replaced).' Description, '2016 January 29' ReleaseDate  union all
select '2008' MajorVersion, '' Build, 'See KB3146034' Description, '' ReleaseDate  union all
select '2008' MajorVersion, '10.00.6535' Build, 'MS15-058: Security Update for SQL Server 2008 SP4 QFE (KB3045308)' Description, '2015 July 14' ReleaseDate  union all
select '2008' MajorVersion, '10.00.6526' Build, 'FIX: On-demand Hotfix Update Package for SQL Server 2008 SP4 (KB3034373)' Description, '2015 February 9' ReleaseDate  union all
select '2008' MajorVersion, '10.00.6241' Build, 'MS15-058: Security Update for SQL Server 2008 SP4 GDR (KB3045311)' Description, '2015 July 14' ReleaseDate  union all
select '2008' MajorVersion, '10.00.6000' Build, 'SQL Server 2008 Service Pack 4' Description, '2014 September 30' ReleaseDate  union ALL
select '2008' MajorVersion, '10.0.6000.29' Build, 'SQL Server 2008 Service Pack 4' Description, '2014 September 3' ReleaseDate  union all
select '2008' MajorVersion, '10.0.6241.0' Build, 'SQL Server 2008 Service Pack 4' Description, 'Apr 17 2015' ReleaseDate  union all

SELECT '2008' MajorVersion, '10.00.5896' Build, 'TLS 1.2 support for SQL Server 2008 SP3 (IA-64) (updated)' Description, '2016 March 3' ReleaseDate  union all
select '2008' MajorVersion, '10.00.5894' Build, 'TLS 1.2 support for SQL Server 2008 SP3 (IA-64) (replaced)' Description, '2016 January 29' ReleaseDate  union all
select '2008' MajorVersion, '' Build, 'See KB3146034' Description, '' ReleaseDate  union all
select '2008' MajorVersion, '10.00.5890' Build, 'MS15-058: Security Update for SQL Server 2008 SP3 QFE (KB3045303)' Description, '2015 July 14' ReleaseDate  union all
select '2008' MajorVersion, '10.00.5869' Build, 'MS14-044: Security Update for SQL Server 2008 SP3 QFE (KB2977322)' Description, '2014 August 12' ReleaseDate  union all
select '2008' MajorVersion, '10.00.5861' Build, 'CU17 for Microsoft SQL Server 2008 SP3(KB2958696)' Description, '2014 May 19' ReleaseDate  union all
select '2008' MajorVersion, '10.00.5852' Build, 'CU16 for Microsoft SQL Server 2008 SP3(KB2936421)' Description, '2014 March 17' ReleaseDate  union all
select '2008' MajorVersion, '10.00.5850' Build, 'CU15 for Microsoft SQL Server 2008 SP3(KB2923520)' Description, '2014 January 20' ReleaseDate  union all
select '2008' MajorVersion, '10.00.5848' Build, 'CU14 for Microsoft SQL Server 2008 SP3(KB2893410)' Description, '2013 November 18' ReleaseDate  union all
select '2008' MajorVersion, '10.00.5846' Build, 'CU13 for Microsoft SQL Server 2008 SP3(KB2880350)' Description, '2013 September 16' ReleaseDate  union all
select '2008' MajorVersion, '10.00.5844' Build, 'CU12 for Microsoft SQL Server 2008 SP3(KB2863205)' Description, '2013 July 16' ReleaseDate  union all
select '2008' MajorVersion, '10.00.5841' Build, 'CU11 (updated) for Microsoft SQL Server 2008 SP3(KB2834048)' Description, '2013 June 13' ReleaseDate  union all
select '2008' MajorVersion, '10.00.5840' Build, 'CU11 (replaced) for Microsoft SQL Server 2008 SP3(KB2834048)' Description, '2013 May 20' ReleaseDate  union all
select '2008' MajorVersion, '10.00.5835' Build, 'CU10 for Microsoft SQL Server 2008 SP3(KB2814783)' Description, '2013 March 18' ReleaseDate  union all
select '2008' MajorVersion, '10.00.5829' Build, 'CU9 for Microsoft SQL Server 2008 SP3(KB2799883)' Description, '2013 January 20' ReleaseDate  union all
select '2008' MajorVersion, '10.00.5828' Build, 'CU8 for Microsoft SQL Server 2008 SP3(KB2771833)' Description, '2012 November 19' ReleaseDate  union all
select '2008' MajorVersion, '10.00.5826' Build, 'MS12-070: Security Update for SQL Server 2008 SP3 QFE (KB2716435)' Description, '2012 October 9' ReleaseDate  union all
select '2008' MajorVersion, '10.00.5794' Build, 'CU7 for Microsoft SQL Server 2008 SP3(KB2738350)' Description, '2012 September 21' ReleaseDate  union all
select '2008' MajorVersion, '10.00.5788' Build, 'CU6 for Microsoft SQL Server 2008 SP3(KB2715953)' Description, '2012 July 16' ReleaseDate  union all
select '2008' MajorVersion, '10.00.5785' Build, 'CU5 for Microsoft SQL Server 2008 SP3(KB2696626)' Description, '2012 May 19' ReleaseDate  union all
select '2008' MajorVersion, '10.00.5775' Build, 'CU4 for Microsoft SQL Server 2008 SP3(KB2673383)' Description, '2012 March 20' ReleaseDate  union all
select '2008' MajorVersion, '10.00.5770' Build, 'CU3 for Microsoft SQL Server 2008 SP3(KB2648098)' Description, '2012 January 16' ReleaseDate  union all
select '2008' MajorVersion, '10.00.5768' Build, 'CU2 for Microsoft SQL Server 2008 SP3(KB2633143)' Description, '2011 November 22' ReleaseDate  union all
select '2008' MajorVersion, '10.00.5766' Build, 'CU1 for Microsoft SQL Server 2008 SP3(KB2617146)' Description, '2011 October 18' ReleaseDate  union all
select '2008' MajorVersion, '10.00.5545' Build, 'TLS 1.2 support for SQL Server 2008 SP3 GDR (IA-64) (updated)' Description, '2016 March 3' ReleaseDate  union all
select '2008' MajorVersion, '10.00.5544' Build, 'TLS 1.2 support for SQL Server 2008 SP3 GDR (IA-64) (replaced)' Description, '2016 January 29' ReleaseDate  union all
select '2008' MajorVersion, '' Build, 'See KB3146034' Description, '' ReleaseDate  union all
select '2008' MajorVersion, '10.00.5538' Build, 'MS15-058: Security Update for SQL Server 2008 SP3 GDR (KB3045305)' Description, '2015 July 14' ReleaseDate  union all
select '2008' MajorVersion, '10.00.5520' Build, 'MS14-044: Security Update for SQL Server 2008 SP3 GDR (KB2977321)' Description, '2014 August 12' ReleaseDate  union all
select '2008' MajorVersion, '10.00.5512' Build, 'MS12-070: Security Update for SQL Server 2008 SP3 GDR (KB2716436)' Description, '2012 October 9' ReleaseDate  union all
select '2008' MajorVersion, '10.00.5500' Build, 'SQL Server 2008 Service Pack 3' Description, '2011 October 6' ReleaseDate  union all
select '2008' MajorVersion, '10.00.4000' Build, 'SQL Server 2008 Service Pack 2' Description, '2010 September 29' ReleaseDate  union all
select '2008' MajorVersion, '10.00.2531' Build, 'SQL Server 2008 Service Pack 1' Description, '2009 April 7' ReleaseDate  union all
select '2008' MajorVersion, '10.00.1600' Build, 'SQL Server 2008 RTM' Description, '2008 August 7' ReleaseDate  union all

SELECT '2005' MajorVersion, '9.00.5324' Build, 'MS12-070: Security Update for SQL Server 2005 SP4 QFE (KB2716427)' Description, '2012 October 9' ReleaseDate  union all
select '2005' MajorVersion, '9.00.5296' Build, 'FIX: “Msg 7359” error when a view uses another view in SQL Server 2005 if the schema version of a remote table is updated (KB2615425)' Description, '2011 October 24' ReleaseDate  union all
select '2005' MajorVersion, '9.00.5295' Build, 'FIX: SQL Server Agent job randomly stops when you schedule the job to run past midnight on specific days in SQL Server 2005 (KB2598903)' Description, '2011 September 15' ReleaseDate  union all
select '2005' MajorVersion, '9.00.5294' Build, 'FIX: Error 5180 when you use the ONLINE option to rebuild an index in SQL Server 2005 (KB2572407)' Description, '2011 August 10' ReleaseDate  union all
select '2005' MajorVersion, '9.00.5292' Build, 'MS11-049: Security Update for SQL Server 2005 SP4 QFE (KB2494123)' Description, '2011 June 14' ReleaseDate  union all
select '2005' MajorVersion, '9.00.5266' Build, 'CU3 for Microsoft SQL Server 2005 SP4 (KB2507769)' Description, '2011 March 22' ReleaseDate  union all
select '2005' MajorVersion, '9.00.5259' Build, 'CU2 for Microsoft SQL Server 2005 SP4 (KB2489409)' Description, '2011 February 22' ReleaseDate  union all
select '2005' MajorVersion, '9.00.5254' Build, 'CU1 for Microsoft SQL Server 2005 SP4 (KB2464079)' Description, '2010 December 24' ReleaseDate  union all
select '2005' MajorVersion, '9.00.5069' Build, 'MS12-070: Security Update for SQL Server 2005 SP4 GDR (KB2716429)' Description, '2012 October 9' ReleaseDate  union all
select '2005' MajorVersion, '9.00.5057' Build, 'MS11-049: Security Update for SQL Server 2005 SP4 GDR (KB2494120)' Description, '2011 June 14' ReleaseDate  union all
select '2005' MajorVersion, '9.00.5000' Build, 'SQL Server 2005 Service Pack 4' Description, '2010 December 17' ReleaseDate  union all
select '2005' MajorVersion, '9.00.4035' Build, 'SQL Server 2005 Service Pack 3' Description, '2008 December 15' ReleaseDate  union all
select '2005' MajorVersion, '9.00.3042' Build, 'SQL Server 2005 Service Pack 2' Description, '2007 February 19' ReleaseDate  union all
select '2005' MajorVersion, '9.00.2047' Build, 'SQL Server 2005 Service Pack 1' Description, '2006 April 18' ReleaseDate  union all
select '2005' MajorVersion, '9.00.1399' Build, 'SQL Server 2005 RTM' Description, '2005 November 7' ReleaseDate  union all

SELECT '2000' MajorVersion, '8.00.2305' Build, 'MS12-060: ecurity update for SQL Server 2000 Service Pack 4 QFE (KB983811)' Description, '2012 August 14' ReleaseDate  union all
select '2000' MajorVersion, '8.00.2039' Build, 'SQL Server 2000 Service Pack 4' Description, '2005 May 6' ReleaseDate  union all
select '2000' MajorVersion, '8.00.760' Build, 'SQL Server 2000 Service Pack 3/3a' Description, '2003 May 19' ReleaseDate  union all
select '2000' MajorVersion, '8.00.532' Build, 'SQL Server 2000 Service Pack 2' Description, '2001 November 30' ReleaseDate  union all
select '2000' MajorVersion, '8.00.384' Build, 'SQL Server 2000 Service Pack 1' Description, '2001 June 12' ReleaseDate  union all
select '2000' MajorVersion, '8.00.194' Build, 'SQL Server 2000 RTM' Description, '2000 November' ReleaseDate  union all

SELECT '7' MajorVersion, '7.00.1063' Build, 'SQL Server 7.0 Service Pack 4' Description, '2002 April 26' ReleaseDate  union all
select '7' MajorVersion, '7.00.961' Build, 'SQL Server 7.0 Service Pack 3' Description, '2000 December 15' ReleaseDate  union all
select '7' MajorVersion, '7.00.842' Build, 'SQL Server 7.0 Service Pack 2' Description, '2000 March 20' ReleaseDate  union all
select '7' MajorVersion, '7.00.699' Build, 'SQL Server 7.0 Service Pack 1' Description, '1999 July 1' ReleaseDate  union all
select '7' MajorVersion, '7.00.623' Build, 'SQL Server 7.0 RTM' Description, '1998 November 27' ReleaseDate  union all

SELECT '6.5' MajorVersion, '6.50.416' Build, 'SQL Server 6.5 Service Pack 5a' Description, '1998 December 24' ReleaseDate  union all
select '6.5' MajorVersion, '6.50.415' Build, 'SQL Server 6.5 Service Pack 5' Description, '' ReleaseDate  union all
select '6.5' MajorVersion, '6.50.281' Build, 'SQL Server 6.5 Service Pack 4' Description, '' ReleaseDate  union all
select '6.5' MajorVersion, '6.50.258' Build, 'SQL Server 6.5 Service Pack 3a' Description, '' ReleaseDate  union all
select '6.5' MajorVersion, '6.50.252' Build, 'SQL Server 6.5 Service Pack 3' Description, '' ReleaseDate  union all
select '6.5' MajorVersion, '6.50.240' Build, 'SQL Server 6.5 Service Pack 2' Description, '' ReleaseDate  union all
select '6.5' MajorVersion, '6.50.213' Build, 'SQL Server 6.5 Service Pack 1' Description, '' ReleaseDate  union all
select '6.5' MajorVersion, '6.50.201' Build, 'SQL Server 6.5 RTM' Description, '1996 June 30' ReleaseDate  union all

SELECT '6' MajorVersion, '6.00.151' Build, 'SQL Server 6.0 Service Pack 3' Description, '' ReleaseDate  union all
select '6' MajorVersion, '6.00.139' Build, 'SQL Server 6.0 Service Pack 2' Description, '' ReleaseDate  union all
select '6' MajorVersion, '6.00.124' Build, 'SQL Server 6.0 Service Pack 1' Description, '' ReleaseDate  union all
select '6' MajorVersion, '6.00.121' Build, 'SQL Server 6.0 RTM' Description, '' ReleaseDate--  union all
GO
/****** Object:  Table [dbo].[AvailabilityGroups]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[AvailabilityGroups](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[ServerId] [int] NULL,
	[AvailabiityGroup] [varchar](100) NULL,
	[replica_server_name] [varchar](100) NULL,
	[IsPrimaryServer] [bit] NULL,
	[ReadableSecondary] [bit] NULL,
	[Synchronous] [bit] NULL,
	[failover_mode_desc] [varchar](100) NULL,
	[synchronization_health_desc] [varchar](100) NULL,
PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[ClusterNodes]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[ClusterNodes](
	[ClusterNodeId] [int] IDENTITY(1,1) NOT NULL,
	[ServerId] [int] NULL,
	[NodeName] [varchar](512) NULL,
	[status] [int] NULL,
	[status_description] [varchar](512) NULL,
	[is_current_owner] [bit] NULL,
 CONSTRAINT [PK_ClusterNodes] PRIMARY KEY CLUSTERED 
(
	[ClusterNodeId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  View [dbo].[vwServers]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE view [dbo].[vwServers]
as
select p.PurposeName, e.EnvironmentName
	, s.*
	,  /*SUBSTRING(version, 22,4)*/ isnull(b.MajorVersion, SUBSTRING(version, 22,4)) MajorVersion
	, b.ReleaseDate
	, sv.service_account
	, sv.last_startup_time
	, cn.NodeName
	, l.*
	, d.*
	, v.*
	, ag.*
	, j.*
	, Pub.*
	, sub.*
	, top_wait
	, TopSql
	, m.providername,	m.datasource,	m.location,	m.providerstring
from servers s
left join purpose p on p.purposeid = s.purposeid
left join environment e on e.environmentid = s.environmentid
left outer join Services sv on sv.ServerId = s.ServerId and sv.servicename='SQL Server (MSSQLSERVER)'
left outer join ClusterNodes cn on cn.ServerId = s.ServerId and cn.is_current_owner=1
outer apply (
	select top 1 * from [vwSqlBuilds] b
	where b.Build = s.Build
) b
outer apply (
	select sum(1) Logins
		, sum( case when sysadmin=1 then 1 else 0 end) SysAdmins
		, sum( case when isntuser=1 then 1 else 0 end) NTUsers
		, sum( case when isntgroup=1 then 1 else 0 end) NTGroups
	from [dbo].Logins l
	where l.ServerId = s.ServerId
) l
outer apply (
	select sum(1) Databases
		, sum(d.DataMB) DataMB
		, sum(d.LogMB) LogMB
		, sum(d.CachedSizeMbs) CachedSizeMbs
		, sum(d.CPUTime) CPUTime
		, count(distinct case when d.GSync_Published_Tables > 0 then DatabaseId end) GSync_Published_Dbs
		, count(distinct case when d.GSync_Subscribed_Tables > 0 then DatabaseId end) GSync_SubscribedDbs
	from [dbo].vwDatabases d 
	where d.ServerId = s.ServerId
)  d
outer apply (
	select sum(1) AvailabilityGroups
		, sum(cast(IsPrimaryServer as int)) PrimaryServer
		, sum(1-cast(IsPrimaryServer as int)) ReplicaServer
	from [dbo].[AvailabilityGroups] ag 
	where ag.ServerId = s.ServerId
)  v
outer apply (
	select sum(1) Volumes
		, sum(v.TotalGB) TotalGB
		, sum(v.AvailableGB) AvailableGB
	from [dbo].Volumes v 
	where v.ServerId = s.ServerId
)  ag
outer apply (
	select sum(1) Jobs
	from [dbo].Jobs j 
	where j.ServerId = s.ServerId
)  j
outer apply (
	select sum(1) PublicationArticles
	from [dbo].Articles Pub 
	where Pub.ServerId = s.ServerId
)  Pub
outer apply (
	select sum(1) SubscriptionArticles
	from [dbo].Subscriptions Sub 
	where Sub.ServerId = s.ServerId
)  Sub
outer apply (
	select top 1 [wait_type] top_wait
	from [dbo].[TopWait] w
	where w.ServerId = s.ServerId
	order by [wait_time_ms] desc
)  W
outer apply (
	select top 1 SPName TopSql
	from [dbo].[TopSql] ts
	where ts.ServerId = s.ServerId
	order by [TotalWorkerTime] desc
)  ts
outer apply (
	select sum(1) AvailabilityGroups
		, sum(case when [IsPrimaryServer]= 1 then 1 else 0 end) PrimaryAvailabilityGroups
		, sum(case when [IsPrimaryServer]= 0 then 1 else 0 end) ReplicaAvailabilityGroups
	from [dbo].[AvailabilityGroups] ags
	where ags.ServerId = s.ServerId
)  Ags
left outer join master..sysservers m on m.srvname = s.ServerName

GO
/****** Object:  View [dbo].[vwServersToPatch]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[vwServersToPatch]
as
SELECT s.ServerName, s.PurposeName, s.MajorVersion, s.Domain, s.WindowsRelease
	, s.Edition, s.Build
	, b.Build LatestBuild, b.ReleaseDate, b.Description LastBuildDesc
 FROM vwServers s 
CROSS apply (SELECT TOP 1 * FROM [dbo].[vwSqlBuilds] b 
	WHERE s.MajorVersion  = b.MajorVersion
	ORDER BY b.Build DESC
) b
WHERE s.build < b.Build
GO
/****** Object:  Table [dbo].[Publishers]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Publishers](
	[PublisherId] [int] IDENTITY(1,1) NOT NULL,
	[Serverid] [int] NULL,
	[PublisherName] [varchar](100) NULL,
	[distribution_db] [varchar](100) NULL,
	[working_directory] [varchar](100) NULL,
	[active] [bit] NULL,
	[publisher_type] [varchar](100) NULL,
 CONSTRAINT [pk_publishers] PRIMARY KEY CLUSTERED 
(
	[PublisherId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  View [dbo].[vwReplication]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE view [dbo].[vwReplication]
as
select s.servername, p.PublisherName, p.Distribution_db, b.publisher_db, b.publication
	, a.source_owner, a.source_object, a.article
	, n.subscriber_server, n.subscriber_db
	, a.destination_object
	, po.[RowCount] PublisherRows
	, so.[RowCount] SubscriberRows, so.size_mbs, so.writes, so.reads, so.PKCols
	, h.ObjectName RplStgTable
from servers s
join Publishers p on s.serverid=p.serverid
join Publications b on b.PublisherId = p.PublisherId
join articles a on a.publicationid = b.publicationid
join subscriptions n on n.articleid = a.articleid

outer apply (
	select top 1 * from vwDatabaseObjects o
		where o.ServerName = p.PublisherName
		and o.DatabaseName = b.publisher_db
		and o.SchemaName = a.source_owner
		and o.ObjectName = a.source_object
		and o.Xtype='U'
) po
outer apply (
	select top 1 * from vwDatabaseObjects o
		where o.ServerName = n.subscriber_server
		and o.DatabaseName = n.subscriber_db
		and o.SchemaName = a.source_owner
		and o.ObjectName = a.destination_object
		and o.Xtype='U'
) so
outer apply (select ObjectName from vwDatabaseObjects o
	where o.servername= n.subscriber_server and o.databasename= n.subscriber_db
	and o.SchemaName = 'rpl'
	and o.ObjectName = 'stg_'+a.source_owner+'_'+a.destination_object
	) h


GO
/****** Object:  Table [dbo].[RplImportLogDetail]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[RplImportLogDetail](
	[rowid] [int] IDENTITY(1,1) NOT NULL,
	[serverid] [int] NULL,
	[databaseid] [int] NULL,
	[ImportLogDetailId] [int] NULL,
	[ImportLogId] [int] NULL,
	[SchemaName] [varchar](100) NULL,
	[TableName] [varchar](128) NULL,
	[TotalRows] [bigint] NULL,
	[RplImportLogRowId] [int] NULL,
	[TotalKbs] [bigint] NULL,
 CONSTRAINT [PK_RplImportLogDetail] PRIMARY KEY CLUSTERED 
(
	[rowid] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  View [dbo].[vwRplImportLog]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




CREATE view [dbo].[vwRplImportLog]
as
select d.ServerName, d.DatabaseName, do.*, datediff(ss, do.startdate, do.EndDate) ImportSeconds--, datediff(mi, l.startdate, getdate()) MinutesBehind
 , ld.TotalRows Products
from RplImportLog do
join vwDatabases d on d.DatabaseId = do.DatabaseId
left join RplImportLogDetail ld on ld.RplImportLogRowId = do.rowid and ld.TableName='stg_shopservice_TBL_Products'



GO
/****** Object:  View [dbo].[vwRplImportLogDetail]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO





CREATE view [dbo].[vwRplImportLogDetail]
as
select d.ServerName, d.DatabaseName, l.StartDate, l.RvFrom, do.*
from RplImportLogDetail do
join vwRplImportLog l on do.RplImportLogRowId = l.rowid
join vwDatabases d on d.DatabaseId = do.DatabaseId





GO
/****** Object:  View [dbo].[vwServerLatency]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE view [dbo].[vwServerLatency]
as
select distinct servername
	, avg(ImportSeconds*1.0) AvgSeconds
	, sum(1) Runs
	, sum(case when ImportSeconds > 90 then 1 else 0 end) RunsOver90Secs
	, sum(case when ImportSeconds > 600 then 1 else 0 end) RunsOver10Mins
	
	, Max(ImportSeconds) MaxSeconds
	, avg(TotalRows) AvgRows
	, sum(case when Success=0 then 1 else 0 end) Failures
	, min(StartDate) First
from vwRplImportLog
where rvfrom > 0x
group by servername
GO
/****** Object:  Table [dbo].[RplPublicationTable]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[RplPublicationTable](
	[rowid] [int] IDENTITY(1,1) NOT NULL,
	[serverid] [int] NULL,
	[databaseid] [int] NULL,
	[TableId] [int] NOT NULL,
	[SchemaName] [varchar](100) NULL,
	[TableName] [varchar](128) NULL,
	[PkName] [varchar](128) NULL,
	[KeyCount] [smallint] NULL,
	[has_identity] [bit] NULL,
	[IsCustom] [bit] NULL,
 CONSTRAINT [PK_RplPublicationTable] PRIMARY KEY CLUSTERED 
(
	[rowid] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  View [dbo].[vwRplPublicationTable]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



CREATE view [dbo].[vwRplPublicationTable]
as
select sd.ServerName PublisherServer, sd.DatabaseName PublisherDatabase
	, s.*
	--select *
from vwDatabases sd
join RplPublicationTable s on s.databaseid = sd.DatabaseId

GO
/****** Object:  Table [dbo].[JobsRunning]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[JobsRunning](
	[JobRunningId] [int] IDENTITY(1,1) NOT NULL,
	[ServerId] [int] NULL,
	[JobId] [int] NULL,
	[jobidentifier] [uniqueidentifier] NOT NULL,
	[job_name] [sysname] NOT NULL,
	[start_execution_date] [datetime] NULL,
	[seconds] [int] NULL,
 CONSTRAINT [PK_JobsRunning] PRIMARY KEY CLUSTERED 
(
	[JobRunningId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  View [dbo].[vwJobHistory]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO





CREATE VIEW [dbo].[vwJobHistory]
AS
SELECT p.*, e.*
	, s.ServerName
	, s.ServerDescription, s.DailyChecks
	, je.*
	, STUFF(STUFF(STUFF(RIGHT(REPLICATE('0', 8) + CAST(je.run_duration as varchar(8)), 8), 3, 0, ':'), 6, 0, ':'), 9, 0, ':') [run_duration(DD:HH:MM:SS)]
	,  je.run_duration / 10000 * 3600
       + je.run_duration % 10000 / 100 * 60
       + je.run_duration % 100 Seconds
FROM servers s
left JOIN purpose p ON p.purposeid = s.purposeid
left JOIN environment e ON e.environmentid = s.environmentid
left JOIN Jobs j ON s.serverid=j.serverid
left JOIN JobHistory je ON je.jobid = j.jobid

GO
/****** Object:  View [dbo].[vwJobsRunning]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE view [dbo].[vwJobsRunning]
as
select distinct 
	s.servername, p.PurposeName
	, r.job_name, r.start_execution_date, r.seconds 
	, avg(h.Seconds) AvgSeconds
	, count(*) RecentRunCount
	, min(RunDateTime) FirstRun
	, max(RunDateTime) LastRun
	, r.ServerId, r.jobidentifier
from JobsRunning r
join servers s on s.ServerId = r.ServerId
join Purpose p on p.PurposeId = s.PurposeId
cross apply (
	--get of last 5 successfull runs
	select instance_id
		, min(RunDateTime) RunDateTime
		, sum(Seconds) Seconds
	from vwJobHistory h
	where h.JobId = r.JobId
	--and h.run_status = 1
	group by h.instance_id
 ) h
where 1=1
-- and r.Seconds > 600 --ignore jobs under 10 minutes
group by s.servername, p.PurposeName, r.job_name, r.start_execution_date, r.seconds, r.ServerId, r.jobidentifier

GO
/****** Object:  Table [dbo].[RplSubscriptionRoutine]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[RplSubscriptionRoutine](
	[rowid] [int] IDENTITY(1,1) NOT NULL,
	[RplSubscriptionRowId] [int] NULL,
	[serverid] [int] NULL,
	[databaseid] [int] NULL,
	[RoutineId] [int] NOT NULL,
	[SubscriptionId] [int] NOT NULL,
	[RoutineName] [varchar](100) NULL,
	[IsActive] [bit] NULL,
	[RoutineSequence] [int] NULL,
 CONSTRAINT [PK_RplSubscriptionRoutine] PRIMARY KEY CLUSTERED 
(
	[rowid] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  View [dbo].[vwRplSubscriptionRoutine]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE view [dbo].[vwRplSubscriptionRoutine]
as
select sd.ServerName PublisherServer, sd.DatabaseName PublisherDatabase
	, s.*
	--select *
from vwDatabases sd
join RplSubscriptionRoutine s on s.databaseid = sd.DatabaseId


GO
/****** Object:  View [dbo].[vwIndexUsage]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




CREATE view [dbo].[vwIndexUsage]
as
select s.ServerName, d.DatabaseName, do.[RowCount]
	, format(do.[RowCount], 'N') RowCount_S
	, format(iu.size_mbs, 'N') size_mbs_S
	, iu.*
	, dbo.InStringCount(cols+isnull(','+included,''), ',')+1 as ColCount
from IndexUsage iu
join Databases d on d.DatabaseId = iu.DatabaseId
join servers s on s.ServerId = iu.ServerId
left outer join DatabaseObjects do on do.DatabaseId = d.DatabaseId and do.SchemaName = iu.table_schema and iu.table_name = do.ObjectName


GO
/****** Object:  View [dbo].[vwDatabasesOnLineNonSystem]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE   view [dbo].[vwDatabasesOnLineNonSystem]
as
select * 
from vwdatabases 
where state_desc = 'online'
and ServerName = coalesce(PrimaryReplicaServerName,ServerName)
and databasename not in ('master','tempdb','msdb','model','reportserver','reportservertempdb')
and isnull(edition,'') <> 'DataWarehouse'

GO
/****** Object:  View [dbo].[vwBadIndexes]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE VIEW [dbo].[vwBadIndexes]
AS
SELECT  ServerId, ServerName, DatabaseName, data_space, table_schema, table_name, index_name, index_type, is_unique, database_file, size_mbs
	--, format(writes,'n') writes_n, format(reads,'n') reads_n
	, writes, reads, fill_factor, cols, included, filter_definition, [RowCount] Rows
	, drop_cmd, disable_cmd
FROM vwIndexUsage
WHERE is_unique=0 
AND is_disabled=0 
AND index_type NOT IN ('CLUSTERED','HEAP')
AND isnull(reads,0)<100
AND isnull(writes,0)>100000
and size_mbs>100

--order by reads-writes 
GO
/****** Object:  View [dbo].[vwDatabasesOnLine]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE   view [dbo].[vwDatabasesOnLine]
as
select * 
from vwdatabases 
where state_desc = 'online'
and ServerName = coalesce(PrimaryReplicaServerName,ServerName)
and isnull(edition,'') <> 'DataWarehouse'

GO
/****** Object:  View [dbo].[vwServersVms]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create   view [dbo].[vwServersVms]
as
select * 
from servers 
where Edition <> 'SQL Azure'
GO
/****** Object:  View [dbo].[vwServersNonAzureSql]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create   view [dbo].[vwServersNonAzureSql]
as
select * 
from servers 
where EngineEdition <> 'SQL Azure'
GO
/****** Object:  View [dbo].[vwDatabasesNotOwnedBySA]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE   view [dbo].[vwDatabasesNotOwnedBySA]
as
select servername, environmentname, purposename , databasename, owner
	, sa.sysadmin IsSAEnabled
	,'exec(''ALTER AUTHORIZATION ON DATABASE::['+databasename+'] to sa'') at ['+servername+']' ChangeOwner
,'exec(''use ['+databasename+'] 
create user ['+owner+'] from login ['+owner+']
exec sp_addrolemember ''''db_ownwer'''', '''''+owner+''''' '') at ['+servername+']' AddLogin
from vwDatabases d
 outer apply (select top 1 * from Logins l where loginname='sa' and l.serverid=d.serverid) sa
where owner <> 'sa'
GO
/****** Object:  Table [dbo].[RplSubscription]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[RplSubscription](
	[rowid] [int] IDENTITY(1,1) NOT NULL,
	[serverid] [int] NULL,
	[databaseid] [int] NULL,
	[SubscriptionId] [int] NOT NULL,
	[ServerName] [varchar](100) NULL,
	[DatabaseName] [varchar](100) NULL,
	[IsActive] [bit] NULL,
	[FrequencyInMinutes] [int] NULL,
	[Initialize] [bit] NULL,
	[SubscriptionName] [varchar](100) NULL,
	[PriorityGroup] [tinyint] NULL,
	[Login] [varchar](100) NULL,
	[Pass] [varchar](100) NULL,
	[DoubleReadRVRange] [bit] NULL,
	[DelayAlertInMinutes] [int] NULL,
	[SubscriptionSequence] [tinyint] NULL,
 CONSTRAINT [PK_RplSubscription] PRIMARY KEY CLUSTERED 
(
	[rowid] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[RplSubscriptionTable]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[RplSubscriptionTable](
	[rowid] [int] IDENTITY(1,1) NOT NULL,
	[serverid] [int] NULL,
	[databaseid] [int] NULL,
	[TableId] [int] NOT NULL,
	[SubscriptionId] [int] NOT NULL,
	[SchemaName] [varchar](100) NULL,
	[TableName] [varchar](128) NULL,
	[PublisherSchemaName] [varchar](128) NULL,
	[PublisherTableName] [varchar](128) NULL,
	[IsActive] [bit] NULL,
	[PkName] [varchar](128) NULL,
	[KeyCount] [smallint] NULL,
	[has_identity] [bit] NULL,
	[Initialize] [bit] NULL,
	[RplSubscriptionRowId] [int] NULL,
	[InitialRowCount] [bigint] NULL,
	[IsCustom] [bit] NULL,
	[GetProcName] [varchar](100) NULL,
	[ExcludeFromChecks] [bit] NULL,
 CONSTRAINT [PK_RplSubscriptionTable] PRIMARY KEY CLUSTERED 
(
	[rowid] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  View [dbo].[vwRplSubscriptionTable]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



CREATE view [dbo].[vwRplSubscriptionTable]
as
select s.ServerName PublisherServer, s.DatabaseName PublisherDatabase
	, sd.serverName SubscriberServer, sd.DatabaseName SubscriberDatabase
	--, st.PublisherSchemaName, st.PublisherTableName
	, so.SchemaName SubscriberSchemaName, so.ObjectName SubscriberObjectName
	, po.[RowCount] PublisherRowCount , so.[RowCount] SubscriberRowCount
	, so.size_mbs, so.writes, so.reads, so.PKCols
	, case when po.[RowCount] > 0 then abs(po.[RowCount] - so.[RowCount])*1.0 / po.[RowCount] else 0 end *100 discrepancy 
	--, s.rowid subscriber
	, st.*
	--select *
from vwDatabases sd
join RplSubscription s on s.databaseid = sd.DatabaseId
join RplSubscriptionTable st on st.databaseid = s.databaseid and st.SubscriptionId = s.SubscriptionId
join vwDatabaseObjects so on so.DatabaseId = sd.DatabaseId and so.SchemaName = st.SchemaName and so.ObjectName = st.TableName and so.Xtype = 'u'

left join vwDatabaseObjects po on po.DatabaseName = s.DatabaseName 
	and po.ServerName = s.ServerName
	and po.Xtype='u' 
	and po.SchemaName = st.PublisherSchemaName
	and po.ObjectName = st.PublisherTableName 





GO
/****** Object:  View [dbo].[vwDatabaseFiles]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE view [dbo].[vwDatabaseFiles]
as
select d.EnvironmentName, d.PurposeName, d.ServerName, d.IsActive, d.DailyChecks, d.DatabaseName, d.PurposeId, df.*
from databaseFiles df
join vwDatabases d on d.DatabaseId = df.DatabaseId

GO
/****** Object:  Table [dbo].[RplDates]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[RplDates](
	[rowid] [int] IDENTITY(1,1) NOT NULL,
	[serverid] [int] NULL,
	[databaseid] [int] NULL,
	[Date] [datetime] NULL,
 CONSTRAINT [PK_RplDates] PRIMARY KEY CLUSTERED 
(
	[rowid] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  View [dbo].[vwRplDates]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE view [dbo].[vwRplDates]
as
select d.ServerName, d.DatabaseName, da.*, datediff(mi, da.date, (select max(date) from importlog)) MinutesBehind
from vwDatabases d
cross apply (
	select top 1 * from RplDates da
	where d.DatabaseId = da.DatabaseId
	order by Date desc
)  da
 



GO
/****** Object:  View [dbo].[vwRplSubscription]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



CREATE view [dbo].[vwRplSubscription]
as
select s.ServerName PublisherServer, s.DatabaseName PublisherDatabase
	, sd.serverName SubscriberServer, sd.DatabaseName SubscriberDatabase
	, s.*
from vwDatabases sd
join RplSubscription s on s.databaseid = sd.DatabaseId


GO
/****** Object:  UserDefinedFunction [dbo].[fnPendingServers]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE function [dbo].[fnPendingServers] (@serverid int)
returns table
as 
return(	
	SELECT  SERVERNAME, serverid FROM SERVERS s
	where isActive=1 
	and (	s.ServerId = @serverid
			or (@serverid =0 and not exists(select * from DatabaseObjects d where s.ServerId =d.serverid))
		)
)


GO
/****** Object:  Table [dbo].[ADGroupMembers]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[ADGroupMembers](
	[rowid] [int] IDENTITY(1,1) NOT NULL,
	[account] [varchar](100) NULL,
	[type] [varchar](20) NULL,
	[privilege] [varchar](20) NULL,
	[mapped_login] [varchar](100) NULL,
	[permission_path] [varchar](100) NULL,
PRIMARY KEY CLUSTERED 
(
	[rowid] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  View [dbo].[vwADGroupMembers]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE VIEW [dbo].[vwADGroupMembers]
AS
SELECT LEFT(permission_path, CHARINDEX('\',permission_path)-1) Domain
 , SUBSTRING(permission_path, CHARINDEX('\',permission_path)+1, LEN(permission_path)) ADGroup 
 , account, type, privilege 
 FROM dbo.ADGroupMembers
GO
/****** Object:  View [dbo].[vwAvailabilityGroups]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE VIEW [dbo].[vwAvailabilityGroups]
AS
SELECT p.*, e.*
	, s.ServerName
	, s.ServerDescription
	, s.Version
	, v.*
FROM servers s
LEFT JOIN purpose p ON p.purposeid = s.purposeid
LEFT JOIN environment e ON e.environmentid = s.environmentid
JOIN AvailabilityGroups v ON v.serverid=s.serverid
GO
/****** Object:  Table [dbo].[BackupFiles]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[BackupFiles](
	[serverid] [int] NOT NULL,
	[FileId] [int] NOT NULL,
	[name] [varchar](8000) NULL,
	[date] [datetime] NULL,
	[size] [bigint] NULL,
	[folderid] [int] NULL,
	[Gbs] [bigint] NULL,
	[Type] [varchar](4) NULL,
	[p1] [int] NULL,
	[DatabaseName] [varchar](128) NULL,
	[databaseid] [int] NULL,
 CONSTRAINT [pk_BackupFiles] PRIMARY KEY CLUSTERED 
(
	[serverid] ASC,
	[FileId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[BackupFolders]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[BackupFolders](
	[serverid] [int] NOT NULL,
	[FolderId] [int] NOT NULL,
	[folder] [varchar](8000) NULL,
	[size] [bigint] NULL,
	[subfolders] [int] NULL,
	[files] [int] NULL,
	[biggest_file] [bigint] NULL,
 CONSTRAINT [pk_BackupFolders] PRIMARY KEY CLUSTERED 
(
	[serverid] ASC,
	[FolderId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  View [dbo].[vwBackupReport]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create view [dbo].[vwBackupReport]
as
with b as (
		select fo.folder
			, fi.*
			, ROW_NUMBER() over (partition by folder, databasename, type order by fi.date desc) backupid
		from BackupFiles fi
		join BackupFolders fo on fo.FolderId = fi.folderid
	--	where fo.folder like '%SEC%'
	) ,
dbs as (
select s.ServerName, d.DatabaseName, d.DataMB, d.LogMB, d.DataMB+ d.LogMB TotalMb
	, b.*
	, LastFull.*
from servers s
join Databases d on s.ServerId = d.ServerId and s.IsActive = 1 and s.backupfolder is not null and s.backupfolder <> ''
outer apply (
	select count(*) TotalBackupFiles
		, sum(b.size)/1024/1024/1024 TotalGbs
		, sum(case when type='Full' then 1 else 0 end) FullBackupFiles
		, sum(case when type='Full' then b.size else 0 end)/1024/1024/1024 FullBackupsGbs
		
		, sum(case when type='Diff' then 1 else 0 end) DiffBackupFiles
		, sum(case when type='Diff' then b.size else 0 end)/1024/1024/1024 DiffBackupsGbs

		, sum(case when type='Log' then 1 else 0 end) LogBackupFiles
		, sum(case when type='Log' then b.size else 0 end)/1024/1024/1024 LogBackupsGbs
	from b
	where b.folder like +s.BackupFolder+'%'
	and b.DatabaseName = d.DatabaseName
	) b
outer apply (
	select name LastFull, folder, date LastFullDate, size/1024/1024 LastFullMbs
	from b
	where b.folder like +s.BackupFolder+'%'
	and b.DatabaseName = d.DatabaseName
	and b.backupid = 1
	and b.Type='full'
	) LastFull
)
select * 
	, LastFullMbs*1.0 / TotalMb Ratio
from dbs
--where TotalGbs is not null
--order by 1,2

GO
/****** Object:  View [dbo].[vwBackups]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



CREATE view [dbo].[vwBackups]
as
with b as (
		select fo.folder
			, fi.*
			, ROW_NUMBER() over (partition by folder, databasename, type order by fi.date desc) backupid
		from BackupFiles fi
		join BackupFolders fo on fo.FolderId = fi.folderid
		--where fo.folder like '\\Dbbackupsec1lvv\shopdbclusterlv$\Database%'
	) ,
dbs as (
select s.ServerName, s.BackupFolder, s.ServerId
	, d.DatabaseName, d.databaseid, d.DataMB, d.LogMB, d.DataMB+ d.LogMB TotalMb
	, b.*
	, LastFull.*
from servers s
join Databases d on s.ServerId = d.ServerId and s.IsActive = 1 and s.backupfolder is not null and s.backupfolder <> ''
outer apply (
	select count(*) TotalBackupFiles
		, sum(b.size)/1024/1024/1024 TotalGbs
		, sum(case when type='Full' then 1 else 0 end) FullBackupFiles
		, sum(case when type='Full' then b.size else 0 end)/1024/1024/1024 FullBackupsGbs
		
		, sum(case when type='Diff' then 1 else 0 end) DiffBackupFiles
		, sum(case when type='Diff' then b.size else 0 end)/1024/1024/1024 DiffBackupsGbs

		, sum(case when type='Log' then 1 else 0 end) LogBackupFiles
		, sum(case when type='Log' then b.size else 0 end)/1024/1024/1024 LogBackupsGbs
	from b
	where b.folder like +s.BackupFolder+'%'
	and b.DatabaseName = d.DatabaseName
	) b
outer apply (
	select name LastFull, folder, date LastFullDate, size/1024/1024 LastFullMbs
	from b
	where b.folder like +s.BackupFolder+'%'
	and b.DatabaseName = d.DatabaseName
	and b.backupid = 1
	and b.Type='full'
	) LastFull
)
select * 
	, LastFullMbs*1.0 / TotalMb Ratio
from dbs



GO
/****** Object:  View [dbo].[vwClusterNodes]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE view [dbo].[vwClusterNodes]
as
select p.PurposeName, e.EnvironmentName, p.PurposeId, e.EnvironmentId
	, s.ServerName
	, d.*
from servers s
join purpose p on p.purposeid = s.purposeid
join environment e on e.environmentid = s.environmentid
join ClusterNodes
 d on s.serverid=d.serverid
GO
/****** Object:  Table [dbo].[DatabaseObjectPerms]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[DatabaseObjectPerms](
	[DatabaseObjectPermId] [int] IDENTITY(1,1) NOT NULL,
	[ServerId] [int] NULL,
	[LoginId] [int] NULL,
	[DatabaseId] [int] NULL,
	[USERNAME] [varchar](100) NULL,
	[type_desc] [varchar](100) NULL,
	[perm_name] [varchar](100) NULL,
	[state_desc] [varchar](100) NULL,
	[class_desc] [varchar](100) NULL,
	[ObjectName] [varchar](100) NULL,
 CONSTRAINT [PK_DatabaseObjectPerms] PRIMARY KEY CLUSTERED 
(
	[DatabaseObjectPermId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  View [dbo].[vwDatabaseObjectPerms]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE view [dbo].[vwDatabaseObjectPerms]
as
select p.*, e.*
	, s.ServerName
	, s.ServerDescription
	, d.DatabaseName
	, l.LoginName, l.isntgroup
	, dp.*
	--select *
from DatabaseObjectPerms dp 
left join databases d on dp.databaseid=d.databaseid
left join Logins l on l.loginid = dp.loginid
left join servers s on dp.serverId = s.ServerId
left join purpose p on p.purposeid = s.purposeid
left join environment e on e.environmentid = s.environmentid

GO
/****** Object:  View [dbo].[vwDatabasePerms]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE view [dbo].[vwDatabasePerms]
as
select p.*, e.*
	, s.ServerName
	, s.ServerDescription
	, d.DatabaseName
	, l.LoginName, l.isntgroup
	, dp.*
from DatabasePerms dp 
join databases d on dp.databaseid=d.databaseid
join Logins l on l.loginid = dp.loginid
join servers s on dp.serverId = s.ServerId
join purpose p on p.purposeid = s.purposeid
join environment e on e.environmentid = s.environmentid

GO
/****** Object:  Table [dbo].[QueryStoreTopExecCounts]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[QueryStoreTopExecCounts](
	[RowId] [int] IDENTITY(1,1) NOT NULL,
	[ServerId] [int] NULL,
	[DatabaseId] [int] NULL,
	[query_id] [int] NULL,
	[query_text_id] [int] NULL,
	[query_sql_text] [varchar](max) NULL,
	[total_execution_count] [bigint] NULL,
 CONSTRAINT [PK_QueryStoreTopExecCounts] PRIMARY KEY CLUSTERED 
(
	[RowId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  View [dbo].[vwQueryStoreTopExecCounts]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




create   view [dbo].[vwQueryStoreTopExecCounts]
as
select d.EnvironmentName, d.PurposeName, d.ServerName, d.IsActive, d.DailyChecks, d.DatabaseName, d.PurposeId, df.*
from QueryStoreTopExecCounts df
join vwDatabases d on d.DatabaseId = df.DatabaseId

GO
/****** Object:  Table [dbo].[Deadlocks]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Deadlocks](
	[DeadlockId] [int] IDENTITY(1,1) NOT NULL,
	[ServerId] [int] NULL,
	[event_timestamp] [datetime] NULL,
	[event_data] [xml] NULL,
	[isExported] [bit] NULL,
 CONSTRAINT [PK_Deadlocks] PRIMARY KEY CLUSTERED 
(
	[DeadlockId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  View [dbo].[vwDeadLocks]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




CREATE view [dbo].[vwDeadLocks]
as
with a as
(select s.servername, s.PurposeId, s.EnvironmentId,
	cast(event_Data as varchar(max)) code,
	d.*
	from Deadlocks d
	join servers s on d.ServerId = s.serverid
),
b as (
	select *, charindex('<frame procname="', code)+17 as i1
	from a	),
b2 as (
	select *, charindex('.', code, i1)+1 as i2, charindex('"', code, i1) as i3
	from b ),
c as (
	select case when i2 > i1 then substring (code, i1, i2 - i1 -1) end db
		, case when i3 > i2 then substring (code, i2, i3-i2) end obj
		, *
	from b2),
d as (
	select
	case when db like 'adhoc%' then 'adhoc' else db end dbname
	, *
	from c
	)
select  deadlockid, PurposeId, EnvironmentId, serverid, servername, dbname, obj, event_timestamp, isExported
	, 'http://cmssqlp01/Deadlocks/deadlock_'+REPLICATE('0', 6-LEN(CAST(deadlockid AS VARCHAR)) )+CAST(deadlockid AS VARCHAR) +'.xml' Link
	, event_data
	, code
from d 



GO
/****** Object:  Table [dbo].[Errors]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Errors](
	[ErrorId] [int] IDENTITY(1,1) NOT NULL,
	[ServerId] [int] NULL,
	[event_timestamp] [datetime] NULL,
	[errornumber] [int] NULL,
	[severity] [int] NULL,
	[errormessage] [varchar](max) NULL,
	[sql_text] [varchar](max) NULL,
	[database_name] [varchar](100) NULL,
	[username] [varchar](100) NULL,
	[client_hostname] [varchar](100) NULL,
	[client_app_name] [varchar](200) NULL,
 CONSTRAINT [PK_Errors] PRIMARY KEY CLUSTERED 
(
	[ErrorId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  View [dbo].[vwErrors]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



CREATE view [dbo].[vwErrors] 
as
select s.ServerName, e.*
from Errors e
join servers s on e.ServerId = s.ServerId




GO
/****** Object:  View [dbo].[vwJobs]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



CREATE view [dbo].[vwJobs]
as
select p.*, e.*
	, s.ServerName
	, s.ServerDescription
	, j.*
--select *
from servers s
left join purpose p on p.purposeid = s.purposeid
left join environment e on e.environmentid = s.environmentid
left join Jobs j on s.serverid=j.serverid
--where s.ServerId=171


GO
/****** Object:  UserDefinedFunction [dbo].[fJobHist]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create function [dbo].[fJobHist] (@days int)
returns table as
return(
SELECT c.RunDateTime,      
       c.job_name, 
       c.step_name, 
       c.run_duration, 
       c.command, 
       a.[message],
	   a.run_status,
	   a.step_id
FROM    msdb.dbo.sysjobhistory a ( NOLOCK )
INNER JOIN ( SELECT TOP 999999999
					j.name AS job_name
				  , jh.step_name
				  , jh.step_id
				  , jh.job_id
				  , js.subsystem
				  , LEFT(js.command, 4000) AS command
				  , js.output_file_name
				  , jh.run_date
				  , jh.run_time
				  , CAST(STUFF(STUFF(CAST(jh.run_date AS VARCHAR), 5, 0, '/'), 8, 0, '/') + ' ' + STUFF(STUFF(RIGHT('000000'
																													+ CAST(jh.run_time AS VARCHAR), 6),
																											  3, 0, ':'), 6, 0, ':') AS DATETIME) AS RunDateTime
				  , jh.run_duration
				  , jh.instance_id
				FROM   msdb.dbo.sysjobhistory jh ( NOLOCK )
				INNER JOIN msdb.dbo.sysjobs j ( NOLOCK ) ON jh.job_id = j.job_id
				INNER JOIN msdb.dbo.sysjobsteps js ( NOLOCK ) ON jh.job_id = js.job_id AND js.step_id = jh.step_id
				WHERE  run_status IN ( 0, 4 )
					AND jh.step_id > 0
					AND jh.run_date >= CAST(CONVERT(VARCHAR(8), DATEADD(DAY, -@days, GETDATE()), 112) AS INT)
				ORDER BY j.job_id
				  , jh.step_id
				  , jh.run_date
				  , jh.run_time
				) c ON a.step_id = c.step_id
				AND a.job_id = c.job_id
				AND a.run_date = c.run_date
				AND a.run_time = c.run_time

WHERE a.run_date >= CAST(CONVERT(VARCHAR(8), DATEADD(DAY, -@days , GETDATE()), 112) AS INT)
)
GO
/****** Object:  View [dbo].[vblocks]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create view [dbo].[vblocks]
as
	select  b.spid blocker_spid, b.last_batch blocker_last_batch, b.hostname blocker_hostname, b.program_name blocker_program_name, b.loginame blocker_loginame, btx.text blocker_text 
		,p.spid, p.lastwaittype, p.waitresource, p.last_batch, p.hostname, p.program_name, p.loginame, tx.text
	from sys.sysprocesses p (nolock)
	outer APPLY sys.dm_exec_sql_text(p.sql_handle) as tx
	left outer join sys.sysprocesses b (nolock) on b.spid = p.blocked
	outer APPLY sys.dm_exec_sql_text(b.sql_handle) as btx 
	where p.blocked>0


GO
/****** Object:  View [dbo].[vBlockTree]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
create view [dbo].[vBlockTree]
as
with r as 
	(
	SELECT spid, 
		   blocked, 
		   Replace (Replace (T.text, Char(10), ' '), Char (13), ' ') AS BATCH 
	FROM   sys.sysprocesses R 
		   CROSS apply sys.Dm_exec_sql_text(R.sql_handle) T 
	)
, blockers (spid, blocked, level, batch) 
     AS (SELECT spid, 
                blocked, 
                Cast (Replicate ('0', 4-Len (Cast (spid AS VARCHAR))) 
                      + Cast (spid AS VARCHAR) AS VARCHAR (1000)) AS LEVEL, 
                batch 
         FROM   R 
         WHERE  ( blocked = 0 
                   OR blocked = spid ) 
                AND EXISTS (SELECT * 
                            FROM   R as R2 
                            WHERE  R2.blocked = R.spid 
                                   AND R2.blocked <> R2.spid) 
         UNION ALL 
         SELECT R.spid, 
                R.blocked, 
                Cast (blockers.level 
                      + RIGHT (Cast ((1000 + R.spid) AS VARCHAR (100)), 4) AS 
                      VARCHAR 
                      ( 
                      1000)) AS 
                LEVEL, 
                R.batch 
         FROM   R 
                INNER JOIN blockers 
                        ON R.blocked = blockers.spid 
         WHERE  R.blocked > 0 
                AND R.blocked <> R.spid) 
SELECT level, N'    ' 
       + Replicate (N'|         ', Len (level)/4 - 1) 
       + CASE WHEN (Len(level)/4 - 1) = 0 THEN 'HEAD -  ' ELSE '|------  ' END + 
       Cast ( 
       spid AS NVARCHAR (10)) + N' ' + batch AS BLOCKING_TREE 
FROM   blockers 

GO
/****** Object:  View [dbo].[VIV_TotalRows]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
create view [dbo].[VIV_TotalRows]
as
	SELECT sch.Name SchemaName, tbl.name TableName,   SUM(PART.rows) TotalRows,   SUM(PART.rows) TotalRowsFormatted
	FROM sys.tables TBL with (nolock)
	INNER JOIN sys.schemas sch on sch.schema_id = tbl.schema_id
	INNER JOIN sys.partitions PART with (nolock) ON TBL.object_id = PART.object_id
	INNER JOIN sys.indexes IDX with (nolock) ON PART.object_id = IDX.object_id	AND PART.index_id = IDX.index_id
	WHERE IDX.index_id < 2--get cix or head 
	group by sch.Name, tbl.name 
GO
/****** Object:  Table [dbo].[Admin_DatabasesNotToBackup]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Admin_DatabasesNotToBackup](
	[ServerId] [int] NULL,
	[DatabaseName] [varchar](100) NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Applications]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Applications](
	[ApplicationId] [int] IDENTITY(1,1) NOT NULL,
	[ApplicationName] [varchar](100) NULL,
	[Owner] [varchar](100) NULL,
 CONSTRAINT [pk_Applications] PRIMARY KEY CLUSTERED 
(
	[ApplicationId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[ApplicationServers]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[ApplicationServers](
	[ApplicationServerId] [int] IDENTITY(1,1) NOT NULL,
	[ApplicationId] [int] NULL,
	[ServerId] [int] NULL,
	[PurposeId] [int] NULL,
 CONSTRAINT [pk_ApplicationServers] PRIMARY KEY CLUSTERED 
(
	[ApplicationServerId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[blocks]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[blocks](
	[blocker_spid] [smallint] NULL,
	[blocker_last_batch] [datetime] NULL,
	[blocker_hostname] [nchar](128) NULL,
	[blocker_program_name] [nchar](128) NULL,
	[blocker_loginame] [nchar](128) NULL,
	[blocker_text] [nvarchar](max) NULL,
	[spid] [smallint] NOT NULL,
	[lastwaittype] [nchar](32) NOT NULL,
	[waitresource] [nchar](256) NOT NULL,
	[last_batch] [datetime] NOT NULL,
	[hostname] [nchar](128) NOT NULL,
	[program_name] [nchar](128) NOT NULL,
	[loginame] [nchar](128) NOT NULL,
	[text] [nvarchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[BrokenRoutines]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[BrokenRoutines](
	[xtype] [varchar](2) NULL,
	[sch] [varchar](100) NULL,
	[obj] [varchar](100) NULL,
	[error] [varchar](255) NULL,
	[code] [nvarchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Command]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Command](
	[CommandId] [int] IDENTITY(1,1) NOT NULL,
	[ServerName] [varchar](100) NULL,
	[Command] [varchar](max) NOT NULL,
	[Priority] [tinyint] NULL,
	[CreateDate] [datetime] NULL,
	[StartDate] [datetime] NULL,
	[EndDate] [datetime] NULL,
	[Message] [varchar](max) NULL,
 CONSTRAINT [pk_command] PRIMARY KEY CLUSTERED 
(
	[CommandId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[CustomerMismatch]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[CustomerMismatch](
	[ServerName] [varchar](100) NULL,
	[LogonName] [nvarchar](100) NULL,
	[ShopCustomerId] [uniqueidentifier] NULL,
	[LocalCustomerId] [uniqueidentifier] NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[CustomerMismatchOrders]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[CustomerMismatchOrders](
	[ServerName] [varchar](100) NULL,
	[LocalCustomerId] [uniqueidentifier] NULL,
	[OrderNumber] [int] NULL,
	[DateCreated] [datetime] NULL,
	[Transfered] [bit] NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[DatabaseFilesHist]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[DatabaseFilesHist](
	[RowId] [int] IDENTITY(1,1) NOT NULL,
	[Date] [datetime] NULL,
	[ServerName] [varchar](100) NULL,
	[DatabaseName] [varchar](100) NULL,
	[FileName] [varchar](200) NULL,
	[PhysicalName] [varchar](500) NULL,
	[TotalMbs] [int] NULL,
	[AvailableMbs] [int] NULL,
	[fileid] [int] NULL,
	[filegroupname] [varchar](100) NULL,
 CONSTRAINT [PK_DatabaseFilesHist] PRIMARY KEY CLUSTERED 
(
	[RowId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[DatabasesHist]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[DatabasesHist](
	[RowId] [int] IDENTITY(1,1) NOT NULL,
	[Date] [datetime] NULL,
	[PurposeName] [varchar](50) NULL,
	[EnvironmentName] [varchar](50) NULL,
	[ServerName] [varchar](100) NULL,
	[IsActive] [bit] NULL,
	[DailyChecks] [bit] NULL,
	[Version] [varchar](255) NULL,
	[EngineEdition] [varchar](50) NULL,
	[datasource] [nvarchar](4000) NULL,
	[location] [nvarchar](4000) NULL,
	[DatabaseName] [varchar](100) NULL,
	[DataMB] [int] NULL,
	[DataMB_S] [nvarchar](4000) NULL,
	[LogMB] [int] NULL,
	[State_Desc] [varchar](100) NULL,
	[RecoveryModel] [varchar](100) NULL,
	[LogSizeKB] [bigint] NULL,
	[LogUsedKB] [bigint] NULL,
	[LogUsedPercentage] [varchar](50) NULL,
	[DBCompatibilityLevel] [varchar](50) NULL,
	[PageVerifyOption] [varchar](50) NULL,
	[is_auto_create_stats_on] [bit] NULL,
	[is_auto_update_stats_on] [bit] NULL,
	[is_auto_update_stats_async_on] [bit] NULL,
	[is_parameterization_forced] [bit] NULL,
	[snapshot_isolation_state_desc] [varchar](50) NULL,
	[is_read_committed_snapshot_on] [bit] NULL,
	[is_auto_close_on] [bit] NULL,
	[is_auto_shrink_on] [bit] NULL,
	[target_recovery_time_in_seconds] [int] NULL,
	[Create_Date] [datetime] NULL,
	[is_published] [bit] NULL,
	[is_subscribed] [bit] NULL,
	[Collation] [varchar](100) NULL,
	[CachedSizeMbs] [int] NULL,
	[CPUTime] [bigint] NULL,
	[Is_Read_Only] [bit] NULL,
	[delayed_durability_desc] [varchar](20) NULL,
	[containment_desc] [varchar](20) NULL,
	[is_cdc_enabled] [bit] NULL,
	[is_broker_enabled] [bit] NULL,
	[is_encrypted] [bit] NULL,
	[is_memory_optimized_elevate_to_snapshot_on] [bit] NULL,
	[AvailabilityGroup] [varchar](100) NULL,
	[PrimaryReplicaServerName] [varchar](100) NULL,
	[LocalReplicaRole] [tinyint] NULL,
	[SynchronizationState] [tinyint] NULL,
	[IsSuspended] [bit] NULL,
	[IsJoined] [bit] NULL,
	[SourceDatabaseName] [varchar](200) NULL,
	[owner] [varchar](100) NULL,
	[mirroring_state] [varchar](255) NULL,
	[mirroring_role] [varchar](255) NULL,
	[mirroring_safety_level] [varchar](255) NULL,
	[mirroring_partner] [varchar](255) NULL,
	[mirroring_partner_instance] [varchar](255) NULL,
	[mirroring_witness] [varchar](255) NULL,
	[mirroring_witness_state] [varchar](255) NULL,
	[mirroring_connection_timeout] [int] NULL,
	[mirroring_redo_queue] [int] NULL,
	[edition] [varchar](100) NULL,
	[service_objective] [varchar](100) NULL,
	[elastic_pool_name] [varchar](100) NULL,
	[SLO_Name] [varchar](100) NULL,
	[Service_Tier] [varchar](100) NULL,
	[Hardware_Gen] [varchar](100) NULL,
	[Logical_CPUs] [numeric](5, 2) NULL,
	[Memory] [numeric](5, 2) NULL,
	[DTUs] [int] NULL,
	[CPUs] [int] NULL,
	[Files] [int] NULL,
	[FileGroups] [int] NULL,
	[Users] [int] NULL,
	[DBO_Users] [int] NULL,
	[Rows] [bigint] NULL,
	[Tables] [int] NULL,
	[Views] [int] NULL,
	[Procs] [int] NULL,
	[Functions] [int] NULL,
	[Gsync_Published_Tables] [int] NULL,
	[Gsync_Subscribed_Tables] [int] NULL,
	[GsyncReplication_LastSuccess] [datetime] NULL,
	[GsyncReplication_Lag] [int] NULL,
	[MS_Published_Tables] [int] NULL,
	[MS_Subscribed_Tables] [int] NULL,
	[Indexes] [int] NULL,
	[Index_data_size] [int] NULL,
	[Index_used_size] [int] NULL,
 CONSTRAINT [PK_DatabasesHist] PRIMARY KEY CLUSTERED 
(
	[RowId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[DeadLockFiles]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[DeadLockFiles](
	[rowid] [int] IDENTITY(1,1) NOT NULL,
	[foldername] [varchar](255) NULL,
	[filename] [varchar](255) NULL,
	[date] [datetime] NULL,
	[size] [bigint] NULL,
	[serverid] [int] NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[ErrorFiles]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[ErrorFiles](
	[rowid] [int] IDENTITY(1,1) NOT NULL,
	[foldername] [varchar](255) NULL,
	[filename] [varchar](255) NULL,
	[date] [datetime] NULL,
	[size] [bigint] NULL,
	[serverid] [int] NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[ExecErrors]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[ExecErrors](
	[id] [int] IDENTITY(1,1) NOT NULL,
	[dt] [datetime] NULL,
	[message] [varchar](255) NULL,
	[command] [varchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[ImportError]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[ImportError](
	[ImportErrorId] [int] IDENTITY(1,1) NOT NULL,
	[FileName] [varchar](255) NULL,
	[ImportDate] [datetime] NULL,
	[Message] [varchar](255) NULL,
PRIMARY KEY CLUSTERED 
(
	[ImportErrorId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 90, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[ImportFile]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[ImportFile](
	[ImportFileId] [int] IDENTITY(1,1) NOT NULL,
	[FileName] [varchar](255) NULL,
	[ImportDate] [datetime] NULL,
	[Rows] [int] NULL,
PRIMARY KEY CLUSTERED 
(
	[ImportFileId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 90, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[IndexUsageHist]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[IndexUsageHist](
	[RowId] [int] IDENTITY(1,1) NOT NULL,
	[Date] [datetime] NULL,
	[ServerName] [varchar](100) NULL,
	[DatabaseName] [varchar](100) NULL,
	[RowCount] [bigint] NULL,
	[data_space] [varchar](200) NULL,
	[allocation_desc] [varchar](200) NULL,
	[table_schema] [varchar](200) NULL,
	[object_type] [varchar](200) NULL,
	[table_name] [varchar](200) NULL,
	[index_type] [varchar](200) NULL,
	[index_name] [varchar](200) NULL,
	[is_unique] [bit] NULL,
	[is_disabled] [bit] NULL,
	[database_file] [varchar](200) NULL,
	[size_mbs] [int] NULL,
	[used_size] [int] NULL,
	[data_size] [int] NULL,
	[writes] [bigint] NULL,
	[reads] [bigint] NULL,
	[index_id] [int] NULL,
	[fill_factor] [float] NULL,
	[avg_fragmentation_in_percent] [float] NULL,
	[cols] [varchar](1000) NULL,
	[included] [varchar](8000) NULL,
	[filter_definition] [varchar](1000) NULL,
	[data_compression_desc] [nvarchar](120) NULL,
	[ColCount] [int] NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[LongSqlFiles]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[LongSqlFiles](
	[rowid] [int] IDENTITY(1,1) NOT NULL,
	[foldername] [varchar](255) NULL,
	[filename] [varchar](255) NULL,
	[date] [datetime] NULL,
	[size] [bigint] NULL,
	[serverid] [int] NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[ObjectTypes]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[ObjectTypes](
	[xtype] [varchar](2) NOT NULL,
	[object_type] [varchar](20) NULL,
 CONSTRAINT [pk_ObjectTypes] PRIMARY KEY CLUSTERED 
(
	[xtype] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[PerfMonApp]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[PerfMonApp](
	[PerfMonAppId] [int] IDENTITY(1,1) NOT NULL,
	[ServerId] [int] NULL,
	[ImportFileId] [int] NULL,
	[MetricDate] [date] NULL,
	[MetricTime] [time](7) NULL,
	[TotalCommittedBytes] [float] NULL,
	[ApplicationRestarts] [float] NULL,
	[RequestWaitTime] [float] NULL,
	[RequestsQueued] [float] NULL,
	[RequestsPerSec] [float] NULL,
	[C_PencentageDiskTime] [float] NULL,
	[D_PercentageDiskTime] [float] NULL,
	[MemoryAvailableMBytes] [float] NULL,
	[MemoryPagesPerSec] [float] NULL,
	[PhisicalPercentageDiskTime] [float] NULL,
	[ProcessorQueueLength] [float] NULL,
	[PostRequestsPerSec] [float] NULL,
	[CurrentConnections] [float] NULL,
	[NetworkBytesTotalPerSec] [float] NULL,
	[PercentageProcessorTime] [float] NULL,
 CONSTRAINT [PK_PerfMonApp] PRIMARY KEY CLUSTERED 
(
	[PerfMonAppId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[PerfMonAppStg]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[PerfMonAppStg](
	[MetricDate] [varchar](100) NULL,
	[TotalCommittedBytes] [varchar](100) NULL,
	[ApplicationRestarts] [varchar](100) NULL,
	[RequestWaitTime] [varchar](100) NULL,
	[RequestsQueued] [varchar](100) NULL,
	[RequestsPerSec] [varchar](100) NULL,
	[C_PencentageDiskTime] [varchar](100) NULL,
	[D_PercentageDiskTime] [varchar](100) NULL,
	[MemoryAvailableMBytes] [varchar](100) NULL,
	[MemoryPagesPerSec] [varchar](100) NULL,
	[PhisicalPercentageDiskTime] [varchar](100) NULL,
	[ProcessorQueueLength] [varchar](100) NULL,
	[PostRequestsPerSec] [varchar](100) NULL,
	[CurrentConnections] [varchar](100) NULL,
	[NetworkBytesTotalPerSec] [varchar](100) NULL,
	[PercentageProcessorTime] [varchar](100) NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[PerfMonStg]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[PerfMonStg](
	[MetricDate] [varchar](100) NULL,
	[MemoryAvailableMBytes] [varchar](100) NULL,
	[PercentageProcessorTime] [varchar](100) NULL,
	[ForwardedRecordsPerSec] [varchar](100) NULL,
	[FullScansPerSec] [varchar](100) NULL,
	[IndexSearchesPerSec] [varchar](100) NULL,
	[PageLifeExpectancy] [varchar](100) NULL,
	[PageReadsPerSec] [varchar](100) NULL,
	[PageWritesPerSec] [varchar](100) NULL,
	[LazyWritesPerSec] [varchar](100) NULL,
	[C_AvgDiskBytesPerRead] [varchar](100) NULL,
	[C_AvgDiskBytesPerWrite] [varchar](100) NULL,
	[C_AvgDiskQueueLength] [varchar](100) NULL,
	[C_AvgDiskSecPerRead] [varchar](100) NULL,
	[C_AvgDiskSecPerWrite] [varchar](100) NULL,
	[D_AvgDiskBytesPerRead] [varchar](100) NULL,
	[D_AvgDiskBytesPerWrite] [varchar](100) NULL,
	[D_AvgDiskQueueLength] [varchar](100) NULL,
	[D_AvgDiskSecPerRead] [varchar](100) NULL,
	[D_AvgDiskSecPerWrite] [varchar](255) NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[PublicationPendingCommands]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[PublicationPendingCommands](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[PublisherId] [int] NULL,
	[ServerId] [int] NULL,
	[Publication] [varchar](100) NULL,
	[Article] [varchar](100) NULL,
	[AgentName] [varchar](100) NULL,
	[UndelivCmdsInDistDB] [int] NULL,
	[DelivCmdsInDistDB] [int] NULL,
 CONSTRAINT [pk_PublicationPendingCommands] PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[ReplicationCheck]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[ReplicationCheck](
	[ServerName] [varchar](100) NULL,
	[DatabaseName] [varchar](100) NULL,
	[PublisherServer] [varchar](100) NULL,
	[PublisherDatabase] [varchar](100) NULL,
	[SubscriptionId] [int] NULL,
	[DateFromSubscription] [datetime] NULL,
	[StartDate] [datetime] NULL,
	[EndDate] [datetime] NULL,
	[TotalRows] [bigint] NULL,
	[Message] [varchar](max) NULL,
	[TableList] [varchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[SecurityGroups]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[SecurityGroups](
	[SecurityGroupId] [int] IDENTITY(1,1) NOT NULL,
	[SecurityGroup] [varchar](100) NULL,
	[SecurityTypeId] [int] NULL,
	[PurposeId] [int] NULL,
	[ApplicationId] [int] NULL,
 CONSTRAINT [pk_SecurityGroups] PRIMARY KEY CLUSTERED 
(
	[SecurityGroupId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[SecurityTypes]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[SecurityTypes](
	[SecurityTypeId] [int] IDENTITY(1,1) NOT NULL,
	[SecurityType] [varchar](100) NULL,
 CONSTRAINT [pk_SecurityTypes] PRIMARY KEY CLUSTERED 
(
	[SecurityTypeId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[ServerAudit]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[ServerAudit](
	[AuditDate] [datetime] NOT NULL,
	[LoginName] [sysname] NOT NULL,
	[EventType] [sysname] NOT NULL,
	[ServerName] [sysname] NOT NULL,
	[DatabaseName] [sysname] NULL,
	[SchemaName] [sysname] NULL,
	[ObjectName] [sysname] NULL,
	[TSQLCommand] [varchar](max) NULL,
	[XMLEventData] [xml] NOT NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[ServersHist]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[ServersHist](
	[RowId] [int] IDENTITY(1,1) NOT NULL,
	[Date] [datetime] NULL,
	[PurposeName] [varchar](50) NULL,
	[EnvironmentName] [varchar](50) NULL,
	[ServerName] [varchar](100) NULL,
	[ServerDescription] [varchar](255) NULL,
	[WindowsRelease] [varchar](20) NULL,
	[CreatedDate] [datetime] NULL,
	[Version] [varchar](255) NULL,
	[Edition] [varchar](255) NULL,
	[ProductLevel] [varchar](50) NULL,
	[Collation] [varchar](50) NULL,
	[LogicalCPUCount] [int] NULL,
	[HyperthreadRatio] [int] NULL,
	[PhysicalCPUCount] [int] NULL,
	[PhysicalMemoryMB] [int] NULL,
	[VMType] [varchar](50) NULL,
	[Hardware] [varchar](100) NULL,
	[ProcessorNameString] [varchar](100) NULL,
	[BlockedProcessEvents] [varchar](255) NULL,
	[DeadlockEvents] [varchar](255) NULL,
	[ErrorEvents] [varchar](255) NULL,
	[LongQueryEvents] [varchar](255) NULL,
	[PerfMonLogs] [varchar](255) NULL,
	[IsActive] [bit] NULL,
	[IP] [varchar](30) NULL,
	[Error] [varchar](255) NULL,
	[BackupFolder] [varchar](500) NULL,
	[DailyChecks] [bit] NULL,
	[Domain] [varchar](100) NULL,
	[BackupChecks] [smallint] NULL,
	[Build] [varchar](50) NULL,
	[ErrorDate] [datetime] NULL,
	[resource_governor_enabled_functions] [tinyint] NULL,
	[RemoteUser] [varchar](100) NULL,
	[MajorVersion] [varchar](6) NULL,
	[ReleaseDate] [varchar](17) NULL,
	[service_account] [varchar](512) NULL,
	[last_startup_time] [datetime] NULL,
	[NodeName] [varchar](512) NULL,
	[Logins] [int] NULL,
	[SysAdmins] [int] NULL,
	[NTUsers] [int] NULL,
	[NTGroups] [int] NULL,
	[Databases] [int] NULL,
	[DataMB] [int] NULL,
	[LogMB] [int] NULL,
	[CachedSizeMbs] [int] NULL,
	[CPUTime] [bigint] NULL,
	[GSync_Published_Dbs] [int] NULL,
	[GSync_SubscribedDbs] [int] NULL,
	[AvailabilityGroups] [int] NULL,
	[PrimaryServer] [int] NULL,
	[ReplicaServer] [int] NULL,
	[Volumes] [int] NULL,
	[TotalGB] [int] NULL,
	[AvailableGB] [int] NULL,
	[Jobs] [int] NULL,
	[PublicationArticles] [int] NULL,
	[SubscriptionArticles] [int] NULL,
	[top_wait] [nvarchar](60) NULL,
	[TopSql] [sysname] NULL,
	[providername] [nvarchar](128) NULL,
	[datasource] [nvarchar](4000) NULL,
	[location] [nvarchar](4000) NULL,
	[providerstring] [nvarchar](4000) NULL,
 CONSTRAINT [PK_ServersHist] PRIMARY KEY CLUSTERED 
(
	[RowId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[SubscriptionPendingCommands]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[SubscriptionPendingCommands](
	[SubscriptionId] [int] NOT NULL,
	[PublicationId] [int] NULL,
	[PublisherId] [int] NULL,
	[ServerId] [int] NULL,
	[PendingCmdCount] [int] NULL,
	[EstimatedProcessTime] [int] NULL,
 CONSTRAINT [pk_SubscriptionPendingCommands] PRIMARY KEY CLUSTERED 
(
	[SubscriptionId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[TempFiles]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[TempFiles](
	[FileId] [int] NULL,
	[name] [varchar](8000) NULL,
	[date] [datetime] NULL,
	[size] [bigint] NULL,
	[folderid] [int] NULL,
	[Gbs] [bigint] NULL,
	[Type] [varchar](4) NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[TempFolders]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[TempFolders](
	[FolderId] [int] NULL,
	[folder] [varchar](8000) NULL,
	[size] [bigint] NULL,
	[subfolders] [int] NULL,
	[files] [int] NULL,
	[biggest_file] [bigint] NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[VolumesHist]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[VolumesHist](
	[RowId] [int] IDENTITY(1,1) NOT NULL,
	[Date] [datetime] NULL,
	[ServerName] [varchar](100) NULL,
	[volume_mount_point] [varchar](10) NULL,
	[TotalGB] [int] NULL,
	[AvailableGB] [int] NULL,
	[PercentageFree] [numeric](9, 2) NULL,
 CONSTRAINT [PK_VolumesHist] PRIMARY KEY CLUSTERED 
(
	[RowId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
SET ANSI_PADDING ON
GO
/****** Object:  Index [idx_ADGroupMembers_account]    Script Date: 10/19/2022 4:18:32 PM ******/
CREATE NONCLUSTERED INDEX [idx_ADGroupMembers_account] ON [dbo].[ADGroupMembers]
(
	[account] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
SET ANSI_PADDING ON
GO
/****** Object:  Index [idx_ADGroupMembers_permission_path]    Script Date: 10/19/2022 4:18:32 PM ******/
CREATE NONCLUSTERED INDEX [idx_ADGroupMembers_permission_path] ON [dbo].[ADGroupMembers]
(
	[permission_path] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
/****** Object:  Index [idx_DatabaseFiles_ServerId]    Script Date: 10/19/2022 4:18:32 PM ******/
CREATE NONCLUSTERED INDEX [idx_DatabaseFiles_ServerId] ON [dbo].[DatabaseFiles]
(
	[ServerId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
/****** Object:  Index [ix_databaseobjectColumns_DatabaseObjectId]    Script Date: 10/19/2022 4:18:32 PM ******/
CREATE NONCLUSTERED INDEX [ix_databaseobjectColumns_DatabaseObjectId] ON [dbo].[DatabaseObjectColumns]
(
	[DatabaseObjectId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
SET ANSI_PADDING ON
GO
/****** Object:  Index [idx_DatabaseObjects_Obj]    Script Date: 10/19/2022 4:18:32 PM ******/
CREATE NONCLUSTERED INDEX [idx_DatabaseObjects_Obj] ON [dbo].[DatabaseObjects]
(
	[DatabaseId] ASC,
	[ObjectName] ASC,
	[SchemaName] ASC,
	[Xtype] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
/****** Object:  Index [idx_DatabasePerms_DatabaseId]    Script Date: 10/19/2022 4:18:32 PM ******/
CREATE NONCLUSTERED INDEX [idx_DatabasePerms_DatabaseId] ON [dbo].[DatabasePerms]
(
	[DatabaseId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
SET ANSI_PADDING ON
GO
/****** Object:  Index [udx_databases_serverid]    Script Date: 10/19/2022 4:18:32 PM ******/
CREATE UNIQUE NONCLUSTERED INDEX [udx_databases_serverid] ON [dbo].[Databases]
(
	[ServerId] ASC,
	[DatabaseName] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
/****** Object:  Index [idx_Errors_event_timestamp_servers]    Script Date: 10/19/2022 4:18:32 PM ******/
CREATE NONCLUSTERED INDEX [idx_Errors_event_timestamp_servers] ON [dbo].[Errors]
(
	[event_timestamp] ASC,
	[ServerId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
SET ANSI_PADDING ON
GO
/****** Object:  Index [idx_IndexFragmentation]    Script Date: 10/19/2022 4:18:32 PM ******/
CREATE NONCLUSTERED INDEX [idx_IndexFragmentation] ON [dbo].[IndexFragmentation]
(
	[DatabaseId] ASC,
	[SchemaName] ASC,
	[TableName] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
/****** Object:  Index [idx_IndexUsage_DatabaseId]    Script Date: 10/19/2022 4:18:32 PM ******/
CREATE NONCLUSTERED INDEX [idx_IndexUsage_DatabaseId] ON [dbo].[IndexUsage]
(
	[DatabaseId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
/****** Object:  Index [ix_IndexUsage_DatabaseObjectId]    Script Date: 10/19/2022 4:18:32 PM ******/
CREATE NONCLUSTERED INDEX [ix_IndexUsage_DatabaseObjectId] ON [dbo].[IndexUsage]
(
	[DatabaseObjectId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
/****** Object:  Index [idx_JobErrors_JobId]    Script Date: 10/19/2022 4:18:32 PM ******/
CREATE NONCLUSTERED INDEX [idx_JobErrors_JobId] ON [dbo].[JobHistory]
(
	[JobId] ASC,
	[instance_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
/****** Object:  Index [idx_joberrors_serverid]    Script Date: 10/19/2022 4:18:32 PM ******/
CREATE NONCLUSTERED INDEX [idx_joberrors_serverid] ON [dbo].[JobHistory]
(
	[ServerID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
/****** Object:  Index [idx_jobs_serverid]    Script Date: 10/19/2022 4:18:32 PM ******/
CREATE NONCLUSTERED INDEX [idx_jobs_serverid] ON [dbo].[Jobs]
(
	[ServerId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
/****** Object:  Index [idx_Msdb_Backups_databaseid]    Script Date: 10/19/2022 4:18:32 PM ******/
CREATE NONCLUSTERED INDEX [idx_Msdb_Backups_databaseid] ON [dbo].[Msdb_Backups]
(
	[databaseid] ASC,
	[backup_start_date] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
/****** Object:  Index [idx_Msdb_Backups_serverid]    Script Date: 10/19/2022 4:18:32 PM ******/
CREATE NONCLUSTERED INDEX [idx_Msdb_Backups_serverid] ON [dbo].[Msdb_Backups]
(
	[ServerId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
SET ANSI_PADDING ON
GO
/****** Object:  Index [idx_publications_publisher_db]    Script Date: 10/19/2022 4:18:32 PM ******/
CREATE NONCLUSTERED INDEX [idx_publications_publisher_db] ON [dbo].[Publications]
(
	[ServerId] ASC,
	[publisher_db] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
/****** Object:  Index [idx_Publications_Serverid]    Script Date: 10/19/2022 4:18:32 PM ******/
CREATE NONCLUSTERED INDEX [idx_Publications_Serverid] ON [dbo].[Publications]
(
	[ServerId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
/****** Object:  Index [idx_RplImportLog_databaseid]    Script Date: 10/19/2022 4:18:32 PM ******/
CREATE NONCLUSTERED INDEX [idx_RplImportLog_databaseid] ON [dbo].[RplImportLog]
(
	[databaseid] ASC,
	[ImportLogId] ASC
)
INCLUDE([StartDate],[TotalRows],[Success]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
/****** Object:  Index [idx_RplImportLogDetail_databaseid]    Script Date: 10/19/2022 4:18:32 PM ******/
CREATE NONCLUSTERED INDEX [idx_RplImportLogDetail_databaseid] ON [dbo].[RplImportLogDetail]
(
	[databaseid] ASC,
	[ImportLogId] ASC,
	[RplImportLogRowId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
/****** Object:  Index [idx_RplImportLogDetail_RplImportLogRowId]    Script Date: 10/19/2022 4:18:32 PM ******/
CREATE NONCLUSTERED INDEX [idx_RplImportLogDetail_RplImportLogRowId] ON [dbo].[RplImportLogDetail]
(
	[RplImportLogRowId] ASC
)
INCLUDE([rowid],[databaseid],[ImportLogId]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
/****** Object:  Index [idx_RplSubscription_databaseid]    Script Date: 10/19/2022 4:18:32 PM ******/
CREATE NONCLUSTERED INDEX [idx_RplSubscription_databaseid] ON [dbo].[RplSubscription]
(
	[databaseid] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
/****** Object:  Index [idx_RplSubscriptionTable_databaseid]    Script Date: 10/19/2022 4:18:32 PM ******/
CREATE NONCLUSTERED INDEX [idx_RplSubscriptionTable_databaseid] ON [dbo].[RplSubscriptionTable]
(
	[databaseid] ASC,
	[SubscriptionId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
SET ANSI_PADDING ON
GO
/****** Object:  Index [udx_servers_servername]    Script Date: 10/19/2022 4:18:32 PM ******/
CREATE UNIQUE NONCLUSTERED INDEX [udx_servers_servername] ON [dbo].[Servers]
(
	[ServerName] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
/****** Object:  Index [idx_Subscriptions_serverid]    Script Date: 10/19/2022 4:18:32 PM ******/
CREATE NONCLUSTERED INDEX [idx_Subscriptions_serverid] ON [dbo].[Subscriptions]
(
	[ServerId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
SET ANSI_PADDING ON
GO
/****** Object:  Index [idx_Subscriptions_subscriber_db]    Script Date: 10/19/2022 4:18:32 PM ******/
CREATE NONCLUSTERED INDEX [idx_Subscriptions_subscriber_db] ON [dbo].[Subscriptions]
(
	[subscriber_server] ASC,
	[subscriber_db] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
/****** Object:  Index [idx_TopSql_serverid]    Script Date: 10/19/2022 4:18:32 PM ******/
CREATE NONCLUSTERED INDEX [idx_TopSql_serverid] ON [dbo].[TopSql]
(
	[ServerId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
/****** Object:  Index [idx_TopWait_serverid]    Script Date: 10/19/2022 4:18:32 PM ******/
CREATE NONCLUSTERED INDEX [idx_TopWait_serverid] ON [dbo].[TopWait]
(
	[ServerId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
/****** Object:  Index [idx_Volumes_ServerId]    Script Date: 10/19/2022 4:18:32 PM ******/
CREATE NONCLUSTERED INDEX [idx_Volumes_ServerId] ON [dbo].[Volumes]
(
	[ServerId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
ALTER TABLE [dbo].[Command] ADD  CONSTRAINT [df_Command_Priority]  DEFAULT ((10)) FOR [Priority]
GO
ALTER TABLE [dbo].[Command] ADD  CONSTRAINT [df_Commnad_CreateDate]  DEFAULT (getdate()) FOR [CreateDate]
GO
ALTER TABLE [dbo].[DatabaseFilesHist] ADD  DEFAULT (getdate()) FOR [Date]
GO
ALTER TABLE [dbo].[DatabasesHist] ADD  DEFAULT (getdate()) FOR [Date]
GO
ALTER TABLE [dbo].[Deadlocks] ADD  DEFAULT ((0)) FOR [isExported]
GO
ALTER TABLE [dbo].[ExecErrors] ADD  DEFAULT (getdate()) FOR [dt]
GO
ALTER TABLE [dbo].[IndexUsageHist] ADD  DEFAULT (getdate()) FOR [Date]
GO
ALTER TABLE [dbo].[RplSubscriptionTable] ADD  DEFAULT ((0)) FOR [IsCustom]
GO
ALTER TABLE [dbo].[Servers] ADD  DEFAULT ((1)) FOR [IsActive]
GO
ALTER TABLE [dbo].[ServersHist] ADD  DEFAULT (getdate()) FOR [Date]
GO
ALTER TABLE [dbo].[TempConnections] ADD  CONSTRAINT [df_TempConnections_DateCreated]  DEFAULT (getdate()) FOR [DateCreated]
GO
ALTER TABLE [dbo].[Admin_DatabasesNotToBackup]  WITH NOCHECK ADD  CONSTRAINT [fk_Admin_DatabasesNotToBackup_Servers] FOREIGN KEY([ServerId])
REFERENCES [dbo].[Servers] ([ServerId])
GO
ALTER TABLE [dbo].[Admin_DatabasesNotToBackup] CHECK CONSTRAINT [fk_Admin_DatabasesNotToBackup_Servers]
GO
ALTER TABLE [dbo].[ApplicationServers]  WITH NOCHECK ADD  CONSTRAINT [fk_ApplicationServer_Purpose] FOREIGN KEY([PurposeId])
REFERENCES [dbo].[Purpose] ([PurposeId])
GO
ALTER TABLE [dbo].[ApplicationServers] CHECK CONSTRAINT [fk_ApplicationServer_Purpose]
GO
ALTER TABLE [dbo].[ApplicationServers]  WITH NOCHECK ADD  CONSTRAINT [fk_ApplicationServers_Application] FOREIGN KEY([ApplicationId])
REFERENCES [dbo].[Applications] ([ApplicationId])
GO
ALTER TABLE [dbo].[ApplicationServers] CHECK CONSTRAINT [fk_ApplicationServers_Application]
GO
ALTER TABLE [dbo].[ApplicationServers]  WITH NOCHECK ADD  CONSTRAINT [fk_ApplicationServers_Server] FOREIGN KEY([ServerId])
REFERENCES [dbo].[Servers] ([ServerId])
GO
ALTER TABLE [dbo].[ApplicationServers] CHECK CONSTRAINT [fk_ApplicationServers_Server]
GO
ALTER TABLE [dbo].[Articles]  WITH NOCHECK ADD  CONSTRAINT [fk_Articles_Servers] FOREIGN KEY([ServerId])
REFERENCES [dbo].[Servers] ([ServerId])
GO
ALTER TABLE [dbo].[Articles] CHECK CONSTRAINT [fk_Articles_Servers]
GO
ALTER TABLE [dbo].[AvailabilityGroups]  WITH NOCHECK ADD  CONSTRAINT [fk_AvailabilityGroups_Servers] FOREIGN KEY([ServerId])
REFERENCES [dbo].[Servers] ([ServerId])
GO
ALTER TABLE [dbo].[AvailabilityGroups] CHECK CONSTRAINT [fk_AvailabilityGroups_Servers]
GO
ALTER TABLE [dbo].[ClusterNodes]  WITH NOCHECK ADD  CONSTRAINT [fk_ClusterNodes_Servers] FOREIGN KEY([ServerId])
REFERENCES [dbo].[Servers] ([ServerId])
GO
ALTER TABLE [dbo].[ClusterNodes] CHECK CONSTRAINT [fk_ClusterNodes_Servers]
GO
ALTER TABLE [dbo].[DatabaseFiles]  WITH NOCHECK ADD  CONSTRAINT [fk_DatabaseFiles_Databases] FOREIGN KEY([DatabaseId])
REFERENCES [dbo].[Databases] ([DatabaseId])
GO
ALTER TABLE [dbo].[DatabaseFiles] CHECK CONSTRAINT [fk_DatabaseFiles_Databases]
GO
ALTER TABLE [dbo].[DatabaseFiles]  WITH NOCHECK ADD  CONSTRAINT [fk_DatabaseFiles_Servers] FOREIGN KEY([ServerId])
REFERENCES [dbo].[Servers] ([ServerId])
GO
ALTER TABLE [dbo].[DatabaseFiles] CHECK CONSTRAINT [fk_DatabaseFiles_Servers]
GO
ALTER TABLE [dbo].[DatabaseObjectColumns]  WITH NOCHECK ADD  CONSTRAINT [fk_DatabaseObjectColumns_DatabaseObjects] FOREIGN KEY([DatabaseObjectId])
REFERENCES [dbo].[DatabaseObjects] ([DatabaseObjectId])
GO
ALTER TABLE [dbo].[DatabaseObjectColumns] CHECK CONSTRAINT [fk_DatabaseObjectColumns_DatabaseObjects]
GO
ALTER TABLE [dbo].[DatabaseObjectColumns]  WITH NOCHECK ADD  CONSTRAINT [fk_DatabaseObjectColumns_Databases] FOREIGN KEY([DatabaseId])
REFERENCES [dbo].[Databases] ([DatabaseId])
GO
ALTER TABLE [dbo].[DatabaseObjectColumns] CHECK CONSTRAINT [fk_DatabaseObjectColumns_Databases]
GO
ALTER TABLE [dbo].[DatabaseObjectColumns]  WITH NOCHECK ADD  CONSTRAINT [fk_DatabaseObjectColumns_Servers] FOREIGN KEY([ServerId])
REFERENCES [dbo].[Servers] ([ServerId])
GO
ALTER TABLE [dbo].[DatabaseObjectColumns] CHECK CONSTRAINT [fk_DatabaseObjectColumns_Servers]
GO
ALTER TABLE [dbo].[DatabaseObjectPerms]  WITH NOCHECK ADD  CONSTRAINT [fk_DatabaseObjectPerms_Databases] FOREIGN KEY([DatabaseId])
REFERENCES [dbo].[Databases] ([DatabaseId])
GO
ALTER TABLE [dbo].[DatabaseObjectPerms] CHECK CONSTRAINT [fk_DatabaseObjectPerms_Databases]
GO
ALTER TABLE [dbo].[DatabaseObjectPerms]  WITH NOCHECK ADD  CONSTRAINT [fk_DatabaseObjectPerms_LoginId] FOREIGN KEY([LoginId])
REFERENCES [dbo].[Logins] ([LoginId])
GO
ALTER TABLE [dbo].[DatabaseObjectPerms] CHECK CONSTRAINT [fk_DatabaseObjectPerms_LoginId]
GO
ALTER TABLE [dbo].[DatabaseObjectPerms]  WITH NOCHECK ADD  CONSTRAINT [FK_DatabaseObjectPerms_Logins] FOREIGN KEY([LoginId])
REFERENCES [dbo].[Logins] ([LoginId])
GO
ALTER TABLE [dbo].[DatabaseObjectPerms] CHECK CONSTRAINT [FK_DatabaseObjectPerms_Logins]
GO
ALTER TABLE [dbo].[DatabaseObjectPerms]  WITH NOCHECK ADD  CONSTRAINT [fk_DatabaseObjectPerms_Servers] FOREIGN KEY([ServerId])
REFERENCES [dbo].[Servers] ([ServerId])
GO
ALTER TABLE [dbo].[DatabaseObjectPerms] CHECK CONSTRAINT [fk_DatabaseObjectPerms_Servers]
GO
ALTER TABLE [dbo].[DatabaseObjects]  WITH NOCHECK ADD  CONSTRAINT [fk_DatabaseObjects_Databases] FOREIGN KEY([DatabaseId])
REFERENCES [dbo].[Databases] ([DatabaseId])
GO
ALTER TABLE [dbo].[DatabaseObjects] CHECK CONSTRAINT [fk_DatabaseObjects_Databases]
GO
ALTER TABLE [dbo].[DatabaseObjects]  WITH NOCHECK ADD  CONSTRAINT [fk_DatabaseObjects_Servers] FOREIGN KEY([ServerId])
REFERENCES [dbo].[Servers] ([ServerId])
GO
ALTER TABLE [dbo].[DatabaseObjects] CHECK CONSTRAINT [fk_DatabaseObjects_Servers]
GO
ALTER TABLE [dbo].[DatabasePerms]  WITH NOCHECK ADD  CONSTRAINT [fk_DatabasePerms_Databases] FOREIGN KEY([DatabaseId])
REFERENCES [dbo].[Databases] ([DatabaseId])
GO
ALTER TABLE [dbo].[DatabasePerms] CHECK CONSTRAINT [fk_DatabasePerms_Databases]
GO
ALTER TABLE [dbo].[DatabasePerms]  WITH NOCHECK ADD  CONSTRAINT [fk_DatabasePerms_Servers] FOREIGN KEY([ServerId])
REFERENCES [dbo].[Servers] ([ServerId])
GO
ALTER TABLE [dbo].[DatabasePerms] CHECK CONSTRAINT [fk_DatabasePerms_Servers]
GO
ALTER TABLE [dbo].[Databases]  WITH NOCHECK ADD  CONSTRAINT [fk_Databases_Servers] FOREIGN KEY([ServerId])
REFERENCES [dbo].[Servers] ([ServerId])
GO
ALTER TABLE [dbo].[Databases] CHECK CONSTRAINT [fk_Databases_Servers]
GO
ALTER TABLE [dbo].[DeadLockFiles]  WITH NOCHECK ADD  CONSTRAINT [fk_DeadLockFiles_Servers] FOREIGN KEY([serverid])
REFERENCES [dbo].[Servers] ([ServerId])
GO
ALTER TABLE [dbo].[DeadLockFiles] CHECK CONSTRAINT [fk_DeadLockFiles_Servers]
GO
ALTER TABLE [dbo].[Deadlocks]  WITH NOCHECK ADD  CONSTRAINT [fk_Deadlocks_Servers] FOREIGN KEY([ServerId])
REFERENCES [dbo].[Servers] ([ServerId])
GO
ALTER TABLE [dbo].[Deadlocks] CHECK CONSTRAINT [fk_Deadlocks_Servers]
GO
ALTER TABLE [dbo].[ErrorFiles]  WITH NOCHECK ADD  CONSTRAINT [fk_ErrorFiles_Servers] FOREIGN KEY([serverid])
REFERENCES [dbo].[Servers] ([ServerId])
GO
ALTER TABLE [dbo].[ErrorFiles] CHECK CONSTRAINT [fk_ErrorFiles_Servers]
GO
ALTER TABLE [dbo].[Errors]  WITH NOCHECK ADD  CONSTRAINT [fk_Errors_Servers] FOREIGN KEY([ServerId])
REFERENCES [dbo].[Servers] ([ServerId])
GO
ALTER TABLE [dbo].[Errors] CHECK CONSTRAINT [fk_Errors_Servers]
GO
ALTER TABLE [dbo].[IndexFragmentation]  WITH NOCHECK ADD  CONSTRAINT [fk_IndexFragmentation_Databases] FOREIGN KEY([DatabaseId])
REFERENCES [dbo].[Databases] ([DatabaseId])
GO
ALTER TABLE [dbo].[IndexFragmentation] CHECK CONSTRAINT [fk_IndexFragmentation_Databases]
GO
ALTER TABLE [dbo].[IndexFragmentation]  WITH NOCHECK ADD  CONSTRAINT [fk_IndexFragmentation_Servers] FOREIGN KEY([ServerId])
REFERENCES [dbo].[Servers] ([ServerId])
GO
ALTER TABLE [dbo].[IndexFragmentation] CHECK CONSTRAINT [fk_IndexFragmentation_Servers]
GO
ALTER TABLE [dbo].[IndexUsage]  WITH NOCHECK ADD  CONSTRAINT [fk_IndexUsage_DatabaseObjects] FOREIGN KEY([DatabaseObjectId])
REFERENCES [dbo].[DatabaseObjects] ([DatabaseObjectId])
GO
ALTER TABLE [dbo].[IndexUsage] CHECK CONSTRAINT [fk_IndexUsage_DatabaseObjects]
GO
ALTER TABLE [dbo].[IndexUsage]  WITH NOCHECK ADD  CONSTRAINT [fk_IndexUsage_Databases] FOREIGN KEY([DatabaseId])
REFERENCES [dbo].[Databases] ([DatabaseId])
GO
ALTER TABLE [dbo].[IndexUsage] CHECK CONSTRAINT [fk_IndexUsage_Databases]
GO
ALTER TABLE [dbo].[IndexUsage]  WITH NOCHECK ADD  CONSTRAINT [fk_IndexUsage_Servers] FOREIGN KEY([ServerId])
REFERENCES [dbo].[Servers] ([ServerId])
GO
ALTER TABLE [dbo].[IndexUsage] CHECK CONSTRAINT [fk_IndexUsage_Servers]
GO
ALTER TABLE [dbo].[JobHistory]  WITH NOCHECK ADD  CONSTRAINT [FK_JobErrors_Jobs] FOREIGN KEY([JobId])
REFERENCES [dbo].[Jobs] ([JobId])
GO
ALTER TABLE [dbo].[JobHistory] CHECK CONSTRAINT [FK_JobErrors_Jobs]
GO
ALTER TABLE [dbo].[Jobs]  WITH NOCHECK ADD  CONSTRAINT [fk_Jobs_Servers] FOREIGN KEY([ServerId])
REFERENCES [dbo].[Servers] ([ServerId])
GO
ALTER TABLE [dbo].[Jobs] CHECK CONSTRAINT [fk_Jobs_Servers]
GO
ALTER TABLE [dbo].[JobSteps]  WITH NOCHECK ADD  CONSTRAINT [fk_JobSteps_JobId] FOREIGN KEY([JobId])
REFERENCES [dbo].[Jobs] ([JobId])
GO
ALTER TABLE [dbo].[JobSteps] CHECK CONSTRAINT [fk_JobSteps_JobId]
GO
ALTER TABLE [dbo].[JobSteps]  WITH NOCHECK ADD  CONSTRAINT [FK_JobSteps_Jobs] FOREIGN KEY([JobId])
REFERENCES [dbo].[Jobs] ([JobId])
GO
ALTER TABLE [dbo].[JobSteps] CHECK CONSTRAINT [FK_JobSteps_Jobs]
GO
ALTER TABLE [dbo].[Logins]  WITH NOCHECK ADD  CONSTRAINT [fk_Logins_Servers] FOREIGN KEY([ServerId])
REFERENCES [dbo].[Servers] ([ServerId])
GO
ALTER TABLE [dbo].[Logins] CHECK CONSTRAINT [fk_Logins_Servers]
GO
ALTER TABLE [dbo].[LongSql]  WITH NOCHECK ADD  CONSTRAINT [fk_LongSql_Servers] FOREIGN KEY([ServerId])
REFERENCES [dbo].[Servers] ([ServerId])
GO
ALTER TABLE [dbo].[LongSql] CHECK CONSTRAINT [fk_LongSql_Servers]
GO
ALTER TABLE [dbo].[LongSqlFiles]  WITH NOCHECK ADD  CONSTRAINT [fk_LongSqlFiles_Servers] FOREIGN KEY([serverid])
REFERENCES [dbo].[Servers] ([ServerId])
GO
ALTER TABLE [dbo].[LongSqlFiles] CHECK CONSTRAINT [fk_LongSqlFiles_Servers]
GO
ALTER TABLE [dbo].[MissingIndexes]  WITH NOCHECK ADD  CONSTRAINT [fk_MissingIndexes_Databases] FOREIGN KEY([DatabaseId])
REFERENCES [dbo].[Databases] ([DatabaseId])
GO
ALTER TABLE [dbo].[MissingIndexes] CHECK CONSTRAINT [fk_MissingIndexes_Databases]
GO
ALTER TABLE [dbo].[MissingIndexes]  WITH NOCHECK ADD  CONSTRAINT [fk_MissingIndexes_Servers] FOREIGN KEY([ServerId])
REFERENCES [dbo].[Servers] ([ServerId])
GO
ALTER TABLE [dbo].[MissingIndexes] CHECK CONSTRAINT [fk_MissingIndexes_Servers]
GO
ALTER TABLE [dbo].[Msdb_Backups]  WITH NOCHECK ADD  CONSTRAINT [fk_Msdb_Backups_Servers] FOREIGN KEY([ServerId])
REFERENCES [dbo].[Servers] ([ServerId])
GO
ALTER TABLE [dbo].[Msdb_Backups] CHECK CONSTRAINT [fk_Msdb_Backups_Servers]
GO
ALTER TABLE [dbo].[QueryStoreLongestAvgTimes]  WITH NOCHECK ADD  CONSTRAINT [fk_QueryStoreLongestAvgTimes_Databases] FOREIGN KEY([DatabaseId])
REFERENCES [dbo].[Databases] ([DatabaseId])
GO
ALTER TABLE [dbo].[QueryStoreLongestAvgTimes] CHECK CONSTRAINT [fk_QueryStoreLongestAvgTimes_Databases]
GO
ALTER TABLE [dbo].[QueryStoreLongestAvgTimes]  WITH NOCHECK ADD  CONSTRAINT [fk_QueryStoreLongestAvgTimes_Servers] FOREIGN KEY([ServerId])
REFERENCES [dbo].[Servers] ([ServerId])
GO
ALTER TABLE [dbo].[QueryStoreLongestAvgTimes] CHECK CONSTRAINT [fk_QueryStoreLongestAvgTimes_Servers]
GO
ALTER TABLE [dbo].[QueryStoreTopExecCounts]  WITH NOCHECK ADD  CONSTRAINT [fk_QueryStoreTopExecCounts_Databases] FOREIGN KEY([DatabaseId])
REFERENCES [dbo].[Databases] ([DatabaseId])
GO
ALTER TABLE [dbo].[QueryStoreTopExecCounts] CHECK CONSTRAINT [fk_QueryStoreTopExecCounts_Databases]
GO
ALTER TABLE [dbo].[QueryStoreTopExecCounts]  WITH NOCHECK ADD  CONSTRAINT [fk_QueryStoreTopExecCounts_Servers] FOREIGN KEY([ServerId])
REFERENCES [dbo].[Servers] ([ServerId])
GO
ALTER TABLE [dbo].[QueryStoreTopExecCounts] CHECK CONSTRAINT [fk_QueryStoreTopExecCounts_Servers]
GO
ALTER TABLE [dbo].[RplDates]  WITH NOCHECK ADD  CONSTRAINT [fk_RplDates_Databases] FOREIGN KEY([databaseid])
REFERENCES [dbo].[Databases] ([DatabaseId])
GO
ALTER TABLE [dbo].[RplDates] CHECK CONSTRAINT [fk_RplDates_Databases]
GO
ALTER TABLE [dbo].[RplDates]  WITH NOCHECK ADD  CONSTRAINT [fk_RplDates_Servers] FOREIGN KEY([serverid])
REFERENCES [dbo].[Servers] ([ServerId])
GO
ALTER TABLE [dbo].[RplDates] CHECK CONSTRAINT [fk_RplDates_Servers]
GO
ALTER TABLE [dbo].[RplImportLog]  WITH NOCHECK ADD  CONSTRAINT [fk_RplImportLog_Databases] FOREIGN KEY([databaseid])
REFERENCES [dbo].[Databases] ([DatabaseId])
GO
ALTER TABLE [dbo].[RplImportLog] CHECK CONSTRAINT [fk_RplImportLog_Databases]
GO
ALTER TABLE [dbo].[RplImportLog]  WITH NOCHECK ADD  CONSTRAINT [fk_RplImportLog_Servers] FOREIGN KEY([serverid])
REFERENCES [dbo].[Servers] ([ServerId])
GO
ALTER TABLE [dbo].[RplImportLog] CHECK CONSTRAINT [fk_RplImportLog_Servers]
GO
ALTER TABLE [dbo].[RplImportLogDetail]  WITH NOCHECK ADD  CONSTRAINT [fk_RplImportLogDetail_Databases] FOREIGN KEY([databaseid])
REFERENCES [dbo].[Databases] ([DatabaseId])
GO
ALTER TABLE [dbo].[RplImportLogDetail] CHECK CONSTRAINT [fk_RplImportLogDetail_Databases]
GO
ALTER TABLE [dbo].[RplImportLogDetail]  WITH NOCHECK ADD  CONSTRAINT [fk_RplImportLogDetail_Servers] FOREIGN KEY([serverid])
REFERENCES [dbo].[Servers] ([ServerId])
GO
ALTER TABLE [dbo].[RplImportLogDetail] CHECK CONSTRAINT [fk_RplImportLogDetail_Servers]
GO
ALTER TABLE [dbo].[RplPublicationTable]  WITH NOCHECK ADD  CONSTRAINT [fk_RplPublicationTable_Databases] FOREIGN KEY([databaseid])
REFERENCES [dbo].[Databases] ([DatabaseId])
GO
ALTER TABLE [dbo].[RplPublicationTable] CHECK CONSTRAINT [fk_RplPublicationTable_Databases]
GO
ALTER TABLE [dbo].[RplPublicationTable]  WITH NOCHECK ADD  CONSTRAINT [fk_RplPublicationTable_Servers] FOREIGN KEY([serverid])
REFERENCES [dbo].[Servers] ([ServerId])
GO
ALTER TABLE [dbo].[RplPublicationTable] CHECK CONSTRAINT [fk_RplPublicationTable_Servers]
GO
ALTER TABLE [dbo].[RplSubscription]  WITH NOCHECK ADD  CONSTRAINT [fk_RplSubscription_Databases] FOREIGN KEY([databaseid])
REFERENCES [dbo].[Databases] ([DatabaseId])
GO
ALTER TABLE [dbo].[RplSubscription] CHECK CONSTRAINT [fk_RplSubscription_Databases]
GO
ALTER TABLE [dbo].[RplSubscription]  WITH NOCHECK ADD  CONSTRAINT [fk_RplSubscription_Servers] FOREIGN KEY([serverid])
REFERENCES [dbo].[Servers] ([ServerId])
GO
ALTER TABLE [dbo].[RplSubscription] CHECK CONSTRAINT [fk_RplSubscription_Servers]
GO
ALTER TABLE [dbo].[RplSubscriptionRoutine]  WITH NOCHECK ADD  CONSTRAINT [fk_RplSubscriptionRoutine_Databases] FOREIGN KEY([databaseid])
REFERENCES [dbo].[Databases] ([DatabaseId])
GO
ALTER TABLE [dbo].[RplSubscriptionRoutine] CHECK CONSTRAINT [fk_RplSubscriptionRoutine_Databases]
GO
ALTER TABLE [dbo].[RplSubscriptionRoutine]  WITH NOCHECK ADD  CONSTRAINT [fk_RplSubscriptionRoutine_Servers] FOREIGN KEY([serverid])
REFERENCES [dbo].[Servers] ([ServerId])
GO
ALTER TABLE [dbo].[RplSubscriptionRoutine] CHECK CONSTRAINT [fk_RplSubscriptionRoutine_Servers]
GO
ALTER TABLE [dbo].[RplSubscriptionTable]  WITH NOCHECK ADD  CONSTRAINT [fk_RplSubscriptionTable_Databases] FOREIGN KEY([databaseid])
REFERENCES [dbo].[Databases] ([DatabaseId])
GO
ALTER TABLE [dbo].[RplSubscriptionTable] CHECK CONSTRAINT [fk_RplSubscriptionTable_Databases]
GO
ALTER TABLE [dbo].[RplSubscriptionTable]  WITH NOCHECK ADD  CONSTRAINT [fk_RplSubscriptionTable_Servers] FOREIGN KEY([serverid])
REFERENCES [dbo].[Servers] ([ServerId])
GO
ALTER TABLE [dbo].[RplSubscriptionTable] CHECK CONSTRAINT [fk_RplSubscriptionTable_Servers]
GO
ALTER TABLE [dbo].[SecurityGroups]  WITH NOCHECK ADD  CONSTRAINT [fk_SecurityGroups_Application] FOREIGN KEY([ApplicationId])
REFERENCES [dbo].[Applications] ([ApplicationId])
GO
ALTER TABLE [dbo].[SecurityGroups] CHECK CONSTRAINT [fk_SecurityGroups_Application]
GO
ALTER TABLE [dbo].[SecurityGroups]  WITH NOCHECK ADD  CONSTRAINT [fk_SecurityGroups_Purpose] FOREIGN KEY([PurposeId])
REFERENCES [dbo].[Purpose] ([PurposeId])
GO
ALTER TABLE [dbo].[SecurityGroups] CHECK CONSTRAINT [fk_SecurityGroups_Purpose]
GO
ALTER TABLE [dbo].[SecurityGroups]  WITH NOCHECK ADD  CONSTRAINT [fk_SecurityGroups_SecurityType] FOREIGN KEY([SecurityTypeId])
REFERENCES [dbo].[SecurityTypes] ([SecurityTypeId])
GO
ALTER TABLE [dbo].[SecurityGroups] CHECK CONSTRAINT [fk_SecurityGroups_SecurityType]
GO
ALTER TABLE [dbo].[Sequences]  WITH NOCHECK ADD  CONSTRAINT [fk_Sequences_Databases] FOREIGN KEY([DatabaseId])
REFERENCES [dbo].[Databases] ([DatabaseId])
GO
ALTER TABLE [dbo].[Sequences] CHECK CONSTRAINT [fk_Sequences_Databases]
GO
ALTER TABLE [dbo].[Sequences]  WITH NOCHECK ADD  CONSTRAINT [fk_Sequences_Servers] FOREIGN KEY([ServerId])
REFERENCES [dbo].[Servers] ([ServerId])
GO
ALTER TABLE [dbo].[Sequences] CHECK CONSTRAINT [fk_Sequences_Servers]
GO
ALTER TABLE [dbo].[Servers]  WITH NOCHECK ADD  CONSTRAINT [FK_Servers_Environment] FOREIGN KEY([EnvironmentId])
REFERENCES [dbo].[Environment] ([EnvironmentId])
GO
ALTER TABLE [dbo].[Servers] CHECK CONSTRAINT [FK_Servers_Environment]
GO
ALTER TABLE [dbo].[Servers]  WITH NOCHECK ADD  CONSTRAINT [fk_Servers_EnvironmentId] FOREIGN KEY([EnvironmentId])
REFERENCES [dbo].[Environment] ([EnvironmentId])
GO
ALTER TABLE [dbo].[Servers] CHECK CONSTRAINT [fk_Servers_EnvironmentId]
GO
ALTER TABLE [dbo].[Servers]  WITH NOCHECK ADD  CONSTRAINT [fk_servers_purpose] FOREIGN KEY([PurposeId])
REFERENCES [dbo].[Purpose] ([PurposeId])
GO
ALTER TABLE [dbo].[Servers] CHECK CONSTRAINT [fk_servers_purpose]
GO
ALTER TABLE [dbo].[Services]  WITH NOCHECK ADD  CONSTRAINT [fk_Services_Servers] FOREIGN KEY([ServerId])
REFERENCES [dbo].[Servers] ([ServerId])
GO
ALTER TABLE [dbo].[Services] CHECK CONSTRAINT [fk_Services_Servers]
GO
ALTER TABLE [dbo].[SQLAzure]  WITH NOCHECK ADD  CONSTRAINT [fk_SQLAzure_Databases] FOREIGN KEY([DatabaseId])
REFERENCES [dbo].[Databases] ([DatabaseId])
GO
ALTER TABLE [dbo].[SQLAzure] CHECK CONSTRAINT [fk_SQLAzure_Databases]
GO
ALTER TABLE [dbo].[SQLAzure]  WITH NOCHECK ADD  CONSTRAINT [fk_SQLAzure_Servers] FOREIGN KEY([ServerId])
REFERENCES [dbo].[Servers] ([ServerId])
GO
ALTER TABLE [dbo].[SQLAzure] CHECK CONSTRAINT [fk_SQLAzure_Servers]
GO
ALTER TABLE [dbo].[SubscriptionPendingCommands]  WITH NOCHECK ADD  CONSTRAINT [fk_SubscriptionPendingCommands_Servers] FOREIGN KEY([ServerId])
REFERENCES [dbo].[Servers] ([ServerId])
GO
ALTER TABLE [dbo].[SubscriptionPendingCommands] CHECK CONSTRAINT [fk_SubscriptionPendingCommands_Servers]
GO
ALTER TABLE [dbo].[Subscriptions]  WITH NOCHECK ADD  CONSTRAINT [fk_Subscriptions_ArticleId] FOREIGN KEY([ArticleId])
REFERENCES [dbo].[Articles] ([ArticleId])
GO
ALTER TABLE [dbo].[Subscriptions] CHECK CONSTRAINT [fk_Subscriptions_ArticleId]
GO
ALTER TABLE [dbo].[Subscriptions]  WITH NOCHECK ADD  CONSTRAINT [fk_Subscriptions_Servers] FOREIGN KEY([ServerId])
REFERENCES [dbo].[Servers] ([ServerId])
GO
ALTER TABLE [dbo].[Subscriptions] CHECK CONSTRAINT [fk_Subscriptions_Servers]
GO
ALTER TABLE [dbo].[TopSql]  WITH NOCHECK ADD  CONSTRAINT [fk_TopSql_Databases] FOREIGN KEY([DatabaseId])
REFERENCES [dbo].[Databases] ([DatabaseId])
GO
ALTER TABLE [dbo].[TopSql] CHECK CONSTRAINT [fk_TopSql_Databases]
GO
ALTER TABLE [dbo].[TopSql]  WITH NOCHECK ADD  CONSTRAINT [fk_TopSql_Servers] FOREIGN KEY([ServerId])
REFERENCES [dbo].[Servers] ([ServerId])
GO
ALTER TABLE [dbo].[TopSql] CHECK CONSTRAINT [fk_TopSql_Servers]
GO
ALTER TABLE [dbo].[TopWait]  WITH NOCHECK ADD  CONSTRAINT [fk_TopWait_Servers] FOREIGN KEY([ServerId])
REFERENCES [dbo].[Servers] ([ServerId])
GO
ALTER TABLE [dbo].[TopWait] CHECK CONSTRAINT [fk_TopWait_Servers]
GO
ALTER TABLE [dbo].[Volumes]  WITH NOCHECK ADD  CONSTRAINT [fk_Volumes_Servers] FOREIGN KEY([ServerId])
REFERENCES [dbo].[Servers] ([ServerId])
GO
ALTER TABLE [dbo].[Volumes] CHECK CONSTRAINT [fk_Volumes_Servers]
GO
/****** Object:  StoredProcedure [dbo].[spAddColumn]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE proc [dbo].[spAddColumn] @SchemaName varchar(50), @TableName varchar(100), @ColumnName varchar(100), @Type varchar(100), @Debug bit=1, @Exec bit=0
as

declare @ServerName varchar(100), @DatabaseName varchar(100), @Sql varchar(max), @SubscriptionId varchar(10)

--Add column
declare t_cursor cursor fast_forward for
	select distinct ServerName, DatabaseName 
	from vwDatabaseObjects do
	where xtype='u' 
	and ObjectName = @TableName
	and SchemaName = @SchemaName
	AND NOT EXISTS (SELECT * FROM dbo.DatabaseObjectColumns doc
		WHERE doc.DatabaseId = do.DatabaseId
		AND doc.TABLE_SCHEMA = do.SchemaName
		AND doc.TABLE_NAME = do.ObjectName
		AND doc.COLUMN_NAME = @ColumnName)
	order by 1,2
open t_cursor
fetch next from t_cursor into @ServerName, @DatabaseName 
while @@FETCH_STATUS=0
begin
	if @servername like '%.database.windows.net%' 
		set @sql = 'exec (''alter table ['+@SchemaName+'].['+@TableName+'] add ['+@ColumnName+'] '+@Type+' '') at ['+@ServerName+'.'+@DatabaseName+'] '
	else
	set @sql = 'exec (''use ['+@DatabaseName+'] alter table ['+@SchemaName+'].['+@TableName+'] add ['+@ColumnName+'] '+@Type+' '') at ['+@ServerName+'] '
	exec spExec @Sql, @Debug, @Exec
	fetch next from t_cursor into @ServerName, @DatabaseName 
end
close t_cursor
deallocate t_cursor

--RePublish
declare t_cursor cursor fast_forward for
	select distinct PublisherServer, PublisherDatabase 
		--select *
	from [dbo].[vwRplSubscriptionTable]
	where PublisherTableName = @TableName
	and PublisherSchemaName = @SchemaName
open t_cursor
fetch next from t_cursor into @ServerName, @DatabaseName 
while @@FETCH_STATUS=0
begin
	if @servername like '%.database.windows.net%' 
		set @sql = 'exec (''exec rpl.spPublishTable @SchemaName='''''+@SchemaName+''''', @TableName='''''+@TableName+''''', @debug = '+cast(@Debug as varchar)+', @exec = 1, @ProcsOnly = 1, @RebuildDeleteTable = 0 '') at ['+@ServerName+'.'+@DatabaseName+'] '
	else
		set @sql = 'exec (''use ['+@DatabaseName+'] exec rpl.spPublishTable @SchemaName='''''+@SchemaName+''''', @TableName='''''+@TableName+''''', @debug = '+cast(@Debug as varchar)+', @exec = 1, @ProcsOnly = 1, @RebuildDeleteTable = 0 '') at ['+@ServerName+'] '
	exec spExec @Sql, @Debug, @Exec
	fetch next from t_cursor into @ServerName, @DatabaseName 
end
close t_cursor
deallocate t_cursor

--ReSubscribe
declare t_cursor cursor fast_forward for
	select distinct SubscriberServer, SubscriberDatabase, SubscriptionId
	from [dbo].[vwRplSubscriptionTable]
	where TableName = @TableName
	and SchemaName = @SchemaName
open t_cursor
fetch next from t_cursor into @ServerName, @DatabaseName, @SubscriptionId
while @@FETCH_STATUS=0
begin
	if @servername like '%.database.windows.net%' 
		set @sql = 'exec (''exec rpl.spSubscribeTable @SubscriptionId = '+@SubscriptionId+', @SchemaName='''''+@SchemaName+''''', @TableName='''''+@TableName+''''', @debug = '+cast(@Debug as varchar)+', @exec = 1  '') at ['+@ServerName+'.'+@DatabaseName+'] '
	else 
		set @sql = 'exec (''use ['+@DatabaseName+'] exec rpl.spSubscribeTable @SubscriptionId = '+@SubscriptionId+', @SchemaName='''''+@SchemaName+''''', @TableName='''''+@TableName+''''', @debug = '+cast(@Debug as varchar)+', @exec = 1  '') at ['+@ServerName+'] '
	exec spExec @Sql, @Debug, @Exec
	fetch next from t_cursor into @ServerName, @DatabaseName, @SubscriptionId
end
close t_cursor
deallocate t_cursor

GO
/****** Object:  StoredProcedure [dbo].[spBackup]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


create proc [dbo].[spBackup]
as
print '--Backup'
declare @sql varchar(max)
set @sql = 'backup database Servers to disk=''c:\Backups\Servers_'+convert( varchar, getdate(),112)+'.bak'' with init'--, compression not supported in express
exec(@sql)




GO
/****** Object:  StoredProcedure [dbo].[spCleanLogFiles]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE proc [dbo].[spCleanLogFiles]
as
declare @sql varchar(max), @step int
declare t_cursor cursor fast_forward for 
	select 'exec(''ALTER EVENT SESSION errors ON SERVER STATE = stop'') at ['+servername+'];', 1
	from servers where ErrorEvents is not null and IsActive=1 and ErrorEvents <> ''
	union all
	select 'exec(''ALTER EVENT SESSION deadlocks ON SERVER STATE = stop'') at ['+servername+'];', 2
	from servers where DeadlockEvents is not null and IsActive=1 and DeadlockEvents <> ''
	union all
	select 'exec(''ALTER EVENT SESSION longblocks ON SERVER STATE = stop'') at ['+servername+'];', 3
	from servers where BlockedProcessEvents is not null and IsActive=1 and BlockedProcessEvents <> ''
	union all
	select 'exec(''ALTER EVENT SESSION longqueries ON SERVER STATE = stop'') at ['+servername+'];', 4
	from servers where LongQueryEvents is not null and IsActive=1 and LongQueryEvents <> ''


	union all
	select 'exec master..xp_cmdshell ''del '+ s.BlockedProcessEvents+'''', 5
	from servers s where BlockedProcessEvents is not null and IsActive=1 and BlockedProcessEvents <> ''
	union  all
	select 'exec master..xp_cmdshell ''del '+ s.DeadlockEvents+'''', 6
	from servers s where DeadlockEvents is not null and IsActive=1 and DeadlockEvents <> ''
	union  all
	select 'exec master..xp_cmdshell ''del '+ s.ErrorEvents+'''', 7
	from servers s where ErrorEvents is not null and IsActive=1 and ErrorEvents <> ''
	union  all
	select 'exec master..xp_cmdshell ''del '+ s.LongQueryEvents+'''', 8
	from servers s where LongQueryEvents is not null and IsActive=1 and LongQueryEvents <> ''
	union  all
	select 'exec master..xp_cmdshell ''del '+ s.PerfMonLogs + ' /Q'+'''', 9
	from servers s where PerfMonLogs is not null and IsActive=1 and PerfMonLogs <> ''

	/*
	union all
	select 'exec(''ALTER EVENT SESSION errors ON SERVER STATE = start'') at ['+servername+'];', 10
	from servers where ErrorEvents is not null and IsActive=1 and ErrorEvents <> ''
	union all
	select 'exec(''ALTER EVENT SESSION deadlocks ON SERVER STATE = start'') at ['+servername+'];', 11
	from servers where DeadlockEvents is not null and IsActive=1 and DeadlockEvents <> ''
	union all
	select 'exec(''ALTER EVENT SESSION longblocks ON SERVER STATE = start'') at ['+servername+'];', 12
	from servers where BlockedProcessEvents is not null and IsActive=1 and BlockedProcessEvents <> ''
	union all
	select 'exec(''ALTER EVENT SESSION longqueries ON SERVER STATE = start'') at ['+servername+'];', 13
	from servers where LongQueryEvents is not null and IsActive=1 and LongQueryEvents <> ''
	*/
	order by 2
open t_cursor
fetch next from t_cursor into @sql, @step
while @@fetch_Status=0
begin
	begin try
		exec (@sql)
	end try
	begin catch
		print @sql
		print error_message()
	end catch
	fetch next from t_cursor into @sql, @step
end
close t_cursor
deallocate t_cursor

GO
/****** Object:  StoredProcedure [dbo].[spCleanOldData]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE proc [dbo].[spCleanOldData] @days int = 40
as
declare @dt date = dateadd(dd, -@days, getdate())

delete from PerfMon	where MetricDate < @dt
delete from PerfMonApp	where MetricDate  < @dt
delete from Errors	where event_timestamp < @dt
delete from LongSql	where  event_timestamp < @dt
delete from Deadlocks	where  event_timestamp < @dt
delete from JobHistory where  RunDateTime < @dt

delete from Errors 
where errormessage like 'this database is not enabled for publication.'
or errormessage like 'invalid object name ''sp_helpsubscription''.'

/*
dbcc dbreindex (PerfMon)
dbcc dbreindex (PerfMonApp)
dbcc dbreindex (Errors)
dbcc dbreindex (LongSql)
dbcc dbreindex (Deadlocks)
dbcc dbreindex (JobHistory)

*/



GO
/****** Object:  StoredProcedure [dbo].[spCleanOldPerfMon]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE proc [dbo].[spCleanOldPerfMon]
as

/*
; with p as (
select MetricDate, MetricTime
, row_number() over(partition by serverid order by serverid, metricdate, metrictime) id
 from PerfMonApp
where MetricDate < dateadd(dd, -30, getdate())
)
delete p where id%4 <> 1

; with p as (
select MetricDate, MetricTime
, row_number() over(partition by serverid order by serverid, metricdate, metrictime) id
 from PerfMon
where MetricDate < dateadd(dd, -30, getdate())
)
delete p where id%4 <> 1
 
 */
 
delete a from PerfMon a 
where exists 
(select * from PerfMon b
	where a.serverid = b.serverid and a.metricdate = b.metricdate 
	and datepart(hour, a.metrictime) = datepart(hour, b.metrictime) 
	and datepart(minute, a.metrictime) = datepart(minute, b.metrictime) 
	and a.metrictime >  b.metrictime
	)

delete a from PerfMonApp a 
where exists 
(select * from PerfMonApp b
	where a.serverid = b.serverid and a.metricdate = b.metricdate 
	and datepart(hour, a.metrictime) = datepart(hour, b.metrictime) 
	and datepart(minute, a.metrictime) = datepart(minute, b.metrictime) 
	and a.metrictime >  b.metrictime
	)

GO
/****** Object:  StoredProcedure [dbo].[spCleanup]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[spCleanup] @debug bit=0
AS

declare @sql varchar(max)
		,  @FK_Schema 	varchar(128)
		,  @FK_Table	varchar(128)
		,  @FK_Name		varchar(128)
		,  @FK_Column	varchar(128)
		,  @PK_Schema	varchar(128)
		,  @PK_Table	varchar(128)
		,  @PK_Column	varchar(128)
		,  @SchemaName  varchar(128)
		,  @TableName  varchar(128)


	IF OBJECT_ID('tempdb..#Fks') IS NOT NULL
		DROP TABLE #Fks

	; with constraint_columns as (
			select kf.TABLE_SCHEMA, kf.TABLE_NAME, KF.CONSTRAINT_NAME 
			 ,STUFF(
                   (SELECT
                        ', ' + kf2.COLUMN_NAME
                        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE KF2
                        WHERE KF2.CONSTRAINT_NAME = KF.CONSTRAINT_NAME
                        ORDER BY kf2.ORDINAL_POSITION
                        FOR XML PATH(''), TYPE
                   ).value('.','varchar(max)')
                   ,1,2, ''
              ) AS COLUMN_NAME
			from INFORMATION_SCHEMA.KEY_COLUMN_USAGE KF
			group by kf.TABLE_SCHEMA, kf.TABLE_NAME, KF.CONSTRAINT_NAME
	)
	SELECT RC.CONSTRAINT_NAME FK_Name
			--, RC.UNIQUE_CONSTRAINT_NAME PkName
			, RC.MATCH_OPTION MatchOption
			, RC.UPDATE_RULE UpdateRule
			, RC.DELETE_RULE DeleteRule
			, rc.UNIQUE_CONSTRAINT_SCHEMA , rc.UNIQUE_CONSTRAINT_NAME

			, KP.TABLE_SCHEMA PK_Schema
			, KP.Table_Name PK_Table
			, KP.COLUMN_NAME PK_Column

			, KF.TABLE_SCHEMA FK_Schema
			, KF.TABLE_NAME FK_Table
			, KF.COLUMN_NAME FK_Column

			--select *
	into #Fks
	FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS RC
	join constraint_columns kp on kp.TABLE_SCHEMA = rc.UNIQUE_CONSTRAINT_SCHEMA AND kp.CONSTRAINT_NAME = rc.UNIQUE_CONSTRAINT_NAME 
	join constraint_columns kF on kf.TABLE_SCHEMA = rc.CONSTRAINT_SCHEMA AND kf.CONSTRAINT_NAME = rc.CONSTRAINT_NAME 
	
	--drop FKs
	DECLARE fk_cursor CURSOR FAST_FORWARD FOR
		SELECT FK_Schema 
			,  FK_Table
			,  FK_Name
			,  FK_Column
			,  PK_Schema
			,  PK_Table
			,  PK_Column
		--select *
		FROM #Fks
	OPEN fk_cursor
	FETCH NEXT FROM fk_cursor INTO  @FK_Schema,  @FK_Table,  @FK_Name,  @FK_Column,  @PK_Schema,  @PK_Table,  @PK_Column
	WHILE @@FETCH_STATUS=0
	BEGIN 
		SET @sql= 'alter table ['+@FK_Schema+'].['+@FK_Table+'] drop constraint ['+@FK_Name+']'
		EXEC dbo.spExec @SQL, @debug
		FETCH NEXT FROM fk_cursor INTO  @FK_Schema,  @FK_Table,  @FK_Name,  @FK_Column,  @PK_Schema,  @PK_Table,  @PK_Column
	END 
	CLOSE fk_cursor
	DEALLOCATE fk_cursor

	--Truncate tables
	declare t_cursor cursor fast_forward for
		select Schema_Name(uid), Name 
		from sysobjects
		where xtype='u' and name not in ('servers','blocks','Environment'
			, 'Purpose','ServerAudit','DatabaseAudit'
			, 'ObjectTypes','ImportFile','ImportError','CustomerMismatch'
			, 'ImportLog','ExecErrors'
			, 'SecurityTypes', 'Applications', 'SecurityGroups', 'ApplicationServers','Command'
			, 'VolumesHist', 'DatabaseFilesHist', 'DatabasesHist', 'ServersHist')
		order by 1,2
	open t_cursor
	fetch next from t_cursor into @SchemaName, @TableName
	while @@FETCH_STATUS=0
	begin
		begin try
			set @sql = 'truncate table ['+@SchemaName+'].['+@TableName+']'
			EXEC dbo.spExec @SQL, @debug
		end try
		begin catch
			print  ERROR_MESSAGE()
			--if truncate fails then we atempt a delete, for instance if table is replicated or is used by views with schemabinding 
			set @sql = 'delete from ['+@SchemaName+'].['+@TableName+']'
			EXEC dbo.spExec @SQL, @debug
		end catch
		fetch next from t_cursor into @SchemaName, @TableName
	end
	close t_cursor
	deallocate t_cursor

	--Recreate FKs
	DECLARE fk_cursor CURSOR FAST_FORWARD FOR
		SELECT FK_Schema 
			,  FK_Table
			,  FK_Name
			,  FK_Column
			,  PK_Schema
			,  PK_Table
			,  PK_Column
		--select *
		FROM #Fks
	OPEN fk_cursor
	FETCH NEXT FROM fk_cursor INTO  @FK_Schema,  @FK_Table,  @FK_Name,  @FK_Column,  @PK_Schema,  @PK_Table,  @PK_Column
	WHILE @@FETCH_STATUS=0
	BEGIN 
		SET @sql= 'alter table ['+@FK_Schema+'].['+@FK_Table+'] with nocheck add constraint ['+@FK_Name + '] foreign key ('+@FK_Column+') references ['+@PK_Schema+'].['+@PK_Table+'] ('+@PK_Column+') '
		EXEC dbo.spExec @SQL, @debug
		FETCH NEXT FROM fk_cursor INTO  @FK_Schema,  @FK_Table,  @FK_Name,  @FK_Column,  @PK_Schema,  @PK_Table,  @PK_Column
	END 
	CLOSE fk_cursor
	DEALLOCATE fk_cursor



GO
/****** Object:  StoredProcedure [dbo].[spCleanupServer]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE proc [dbo].[spCleanupServer] @serverid int
as

/*
select 'delete from dbo.'+o.name+' where serverid = @serverid' 
from sysobjects o
join syscolumns c on o.id=c.id
where o.xtype='u'
and c.name='serverid' 
and o.name<>'servers'
*/

delete from dbo.DatabaseObjectColumns where serverid = @serverid
delete from dbo.DatabaseObjectPerms where serverid = @serverid
delete from dbo.IndexUsage where serverid = @serverid
delete from dbo.IndexFragmentation where serverid = @serverid
delete from dbo.DatabaseObjects where serverid = @serverid

delete from dbo.TopSql where serverid = @serverid
delete from dbo.Volumes where serverid = @serverid
delete from dbo.Services where serverid = @serverid
delete from dbo.RplPublicationTable where serverid = @serverid
delete from dbo.RplSubscriptionRoutine where serverid = @serverid
delete from dbo.AvailabilityGroups where serverid = @serverid
delete from dbo.DeadLockFiles where serverid = @serverid
delete from dbo.ErrorFiles where serverid = @serverid

delete from dbo.LongSqlFiles where serverid = @serverid
delete from dbo.ClusterNodes where serverid = @serverid
delete from dbo.PerfMonApp where serverid = @serverid
delete from dbo.PublicationPendingCommands where serverid = @serverid
delete from dbo.RplSubscriptionTable where serverid = @serverid
delete from dbo.RplSubscription where serverid = @serverid
delete from dbo.SubscriptionPendingCommands where serverid = @serverid
delete from dbo.DatabaseFiles where serverid = @serverid
delete from dbo.DatabasePerms where serverid = @serverid

delete from dbo.Publications where serverid = @serverid
delete from dbo.Articles where serverid = @serverid
delete from dbo.Subscriptions where serverid = @serverid
delete from dbo.RplImportLog where serverid = @serverid

delete from dbo.RplImportLogDetail where serverid = @serverid
delete from dbo.TopWait where serverid = @serverid
delete from dbo.RplDates where serverid = @serverid
delete from dbo.Sequences where serverid = @serverid
delete from dbo.Logins where serverid = @serverid

delete from dbo.Deadlocks where serverid = @serverid
delete from dbo.Errors where serverid = @serverid

delete from dbo.JobHistory where jobid in (select jobid  from dbo.Jobs where serverid = @serverid)
delete from dbo.JobSteps where  jobid in (select jobid  from dbo.Jobs where serverid = @serverid)

delete from dbo.Jobs where serverid = @serverid

delete from dbo.LongSql where serverid = @serverid
delete from dbo.MissingIndexes where serverid = @serverid
delete from dbo.PerfMon where serverid = @serverid
delete from dbo.Publishers where serverid = @serverid

delete from dbo.Databases where serverid = @serverid

delete from BackupFiles where serverid=@serverid
delete from BackupFolders where serverid=@serverid

GO
/****** Object:  StoredProcedure [dbo].[spCompositeCacheSwap]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



CREATE   proc [dbo].[spCompositeCacheSwap] 
	  @topn tinyint=10
	, @target varchar(4)='dev' --prod, rcl, pred, dev, tst 
	, @debug bit=0 --print commands if 1
	, @exec bit=1  -- execute commands if 1
as
begin
	raiserror ('this proc was retired, please run [spCompositeCacheCopyFetch] on QSService2 database directly.', 16,1)
	return(0)
	/*
	--validate @target
	if isnull(@target,'') not in ('prod','pred','dev','rcl','tst')
	begin
		raiserror('Invalid Target Value', 16,0)
		return(0)
	end
	if @topn <1 or @topn > 150--this is because remote proc output variables cant exceed 8k bytes
	begin
		raiserror('@topn must be between 1 and 150', 16,0)
		return(0)
	end
	
	--set connection variables
	declare @qsservice varchar(100), @qsservice2 varchar(100), @sql varchar(max)
	if @target = 'DEV'
	begin
		set @qsservice = '[uswdevdb.database.windows.net.DEV_QSService].DEV_QSService.'
		set @qsservice2 = '[uswdevdb.database.windows.net.DEV_QSService2].DEV_QSService2.'
	end
	else if @target = 'PRED'
	begin
		set @qsservice = '[uswdevdb.database.windows.net.PRED_QSService].PRED_QSService.'
		set @qsservice2 = '[uswdevdb.database.windows.net.PRED_QSService2].PRED_QSService2.'
	end
	else if @target = 'TST'
	begin
		set @qsservice = '[uswdevdb.database.windows.net.TST_QSService].TST_QSService.'
		set @qsservice2 = '[uswdevdb.database.windows.net.TST_QSService2].TST_QSService2.'
	end
	else if @target = 'RCL'
	begin
		set @qsservice = '[uswproddb.database.windows.net.RCL_QSService].RCL_QSService.'
		set @qsservice2 = '[uswproddb.database.windows.net.RCL_QSService2].RCL_QSService2.'
	end
	else if @target = 'PROD'
	begin
		set @qsservice = '[uswproddb.database.windows.net.PROD_QSService_20190708].PROD_QSService_20190708.'
		set @qsservice2 = '[uswproddb.database.windows.net.PROD_QSService2_20190708].PROD_QSService2_20190708.'
	end

	if object_id('tempdb..#list') is not null
		drop table #list
	
	create table #list (
		CompositeCacheId int
		, EvidenceId uniqueidentifier
	)

	set nocount on
	
	if @debug = 1
		print '
	if object_id(''tempdb..#list'') is not null
		drop table #list
	
	create table #list (
		CompositeCacheId int
		, EvidenceId uniqueidentifier
	)'

	--fetch topN rows
	--remote proc calls cannot take varchar(max) output variables :-(
	set @sql = '
	declare @list varchar(8000)
	exec '+@qsservice2+'dbo.spCompositeCacheCopyFetch @TopN='+cast(@TopN as varchar)+', @list = @list output
	
	insert into #list (CompositeCacheId, EvidenceId)
	select left(value, charindex('','', value)-1) , SUBSTRING(value, charindex('','', value)+1, 36) 
	from STRING_SPLIT (@list, char(13) )
	where charindex('','', value)>0
	' 
	exec [dbo].[spExec] @sql, @debug, @exec 
	

	--return data for repopulation
	set @sql = '
	select l.CompositeCacheId, e.EvidenceId, e.ParameterValue01
	from #list l
	join '+@qsservice2+'QuotaManagement.Evidence e on e.EvidenceId = l.EvidenceId
	'
	exec [dbo].[spExec] @sql, @debug, @exec 
	*/

end
GO
/****** Object:  StoredProcedure [dbo].[spCompositeCacheSwapPrep]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE   proc [dbo].[spCompositeCacheSwapPrep] 
	  @target varchar(4)='prod' --prod, rcl, pred, dev, tst 
	, @SinceDate date=null
	, @top int = null
	, @debug bit=0 --print commands if 1
	, @exec bit=1  -- execute commands if 1
as
begin
	if @SinceDate is null
		set @SinceDate = dateadd(dd,-10,getdate()) --default to 10 days prior
	if @top is null
		set @top = 10000000 --10 million

	--validate @target
	if isnull(@target,'') not in ('prod','pred','dev','rcl','tst')
	begin
		raiserror('Invalid Target Value', 16,0)
		return(0)
	end

	--set connection variables
	declare @qsservice varchar(100), @qsservice2 varchar(100), @sql varchar(max)
	if @target = 'DEV'
	begin
		set @qsservice = '[uswdevdb.database.windows.net.DEV_QSService].DEV_QSService.'
		set @qsservice2 = '[uswdevdb.database.windows.net.DEV_QSService2].DEV_QSService2.'
	end
	else if @target = 'PRED'
	begin
		set @qsservice = '[uswdevdb.database.windows.net.PRED_QSService].PRED_QSService.'
		set @qsservice2 = '[uswdevdb.database.windows.net.PRED_QSService2].PRED_QSService2.'
	end
	else if @target = 'TST'
	begin
		set @qsservice = '[uswdevdb.database.windows.net.TST_QSService].TST_QSService.'
		set @qsservice2 = '[uswdevdb.database.windows.net.TST_QSService2].TST_QSService2.'
	end
	else if @target = 'RCL'
	begin
		set @qsservice = '[uswproddb.database.windows.net.RCL_QSService].RCL_QSService.'
		set @qsservice2 = '[uswproddb.database.windows.net.RCL_QSService2].RCL_QSService2.'
	end
	else if @target = 'PROD'
	begin
		set @qsservice = '[uswproddb.database.windows.net.PROD_QSService].PROD_QSService.'
		set @qsservice2 = '[uswproddb.database.windows.net.PROD_QSService2_Copy].PROD_QSService2_Copy.'
	end

	--Cleanup CompositeCacheCopy
	set @sql = 'delete from '+@qsservice2+'dbo.CompositeCacheCopy
	'
	exec [dbo].[spExec] @sql, @debug, @exec 

	--Copy recent Predictive CompositeCache from QSService to QSService2
	set @sql = '
	insert into '+@qsservice2+'dbo.[CompositeCacheCopy] ([Id], [CompositeKey], [CompositeType], [CreateDate])
	select top ('+cast(@top as varchar)+') [Id], [CompositeKey], [CompositeType], [CreateDate]
	from  '+@qsservice+'[dbo].[CompositeCache] 
	where [CompositeKey] like ''UEIC.Business.Command.PredictiveDiscoveryJSONCommand%''
	and CreateDate >= '''+convert(varchar, @SinceDate, 110)+ '''
	order by CreateDate desc --need to limit the number of days or the evidencecopy will be too large
	'
	exec [dbo].[spExec] @sql, @debug, @exec 

	--Create copy of PredictiveEvidenceCopy with indexes to support matching
	set @sql = 'exec '+@qsservice2+'dbo.spPredictiveEvidenceCopy @SinceDate='''+convert(varchar, @SinceDate, 110)+ ''''
	exec [dbo].[spExec] @sql, @debug, @exec 

	--Remove cache entries without a possible match 
	set @sql = 'exec '+@qsservice2+'dbo.spPredictiveEvidenceCopyCleanOrphans'
	exec [dbo].[spExec] @sql, @debug, @exec 

	--Match entries
	set @sql = 'exec '+@qsservice2+'dbo.spCompositeCacheCopyMatch'
	exec [dbo].[spExec] @sql, @debug, @exec 

end
GO
/****** Object:  StoredProcedure [dbo].[spDailyChecks]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE proc [dbo].[spDailyChecks] (@Email varchar(100) ='', @CustomChecks bit=0, @ServerId int =0, @PrintSummary bit=1)
as
begin

if OBJECT_ID('tempdb..#t') is not null
	drop table #t

create table #t (
	Message varchar(255),
	Servername varchar(255),
	Subject varchar(2000),
	detail1 varchar(8000),
	detail2 varchar(8000),
	Sort int
)

set nocount on


insert into #t
select '00 - LoadDate:' Message, '' servername,'' subject,'' detail1, cast(max(date) as varchar) detail2, 1 Sort from ImportLog

if OBJECT_ID('tempdb..#FullyLoaded') is not null
	drop table #FullyLoaded

select distinct ServerId into #FullyLoaded from DatabaseObjectColumns

insert into #t
select '01 - Server Not Loaded Fully', servername, 'exec spLoad @ServerId = '+cast(serverid as varchar(255)), '', '',1
from servers s
where IsActive=1
and DailyChecks=1
--AND s.PurposeId=1
and serverid not in (select ServerId from dbo.tfServerFullyLoaded() )
and (@ServerId  = 0 or serverid=@ServerId)


insert into #t
select '02 - Server is disabled', servername, cast(serverid as varchar(255)), EnvironmentName, Error, 1 
from vwServers s
 where s.IsActive=0 and DailyChecks=1

 insert into #t
select '03 - Availability Group Not Healthy' Message, ServerName, AvailabiityGroup
	, replica_server_name
	, failover_mode_desc Cnt, 1 Sort
from vwAvailabilityGroups 
where synchronization_health_desc = 'NOT_HEALTHY'
--AND version NOT LIKE 'Microsoft SQL Server 2016 (SP1) (KB3182545)%'--this version has a bug showing groups not healthy, need sp1 CU3 to fix
and (@ServerId  = 0 or serverid=@ServerId)


insert into #t
select '03 - Database is in Suspect State' Message, ServerName, DatabaseName
	, 'PrimaryReplicaServerName: '+ PrimaryReplicaServerName
	, 'DataMB: '+cast(DataMB as varchar), 1 Sort
from vwDatabases 
where state_desc in ('suspect','RECOVERY_PENDING')
--AND version NOT LIKE 'Microsoft SQL Server 2016 (SP1) (KB3182545)%'--this version has a bug showing groups not healthy, need sp1 CU3 to fix
and (@ServerId  = 0 or serverid=@ServerId)

insert into #t
select '03 - Cluster node does not match extended event folder', ServerName, NodeName, ErrorEvents +char(13)+ DeadlockEvents +char(13)+ LongQueryEvents , '',1
from vwservers 
where DailyChecks=1 
and NodeName is not null
and IsActive=1
and (  (ErrorEvents not like '%' +NodeName +'%' and len(ErrorEvents)>1)
	or (DeadlockEvents not like '%' +NodeName +'%' and len(DeadlockEvents)>1)
	or (LongQueryEvents not like '%' +NodeName +'%' and len(LongQueryEvents)>1)
	)

/*
union all 
--low space volumes, moved to houly checks
select '02 - Volume low in free space' Message
	, servername
	, cast(v.volume_mount_point as varchar(200)) subject
	, 'PercentageFree: '+cast(v.PercentageFree as varchar) detail1
	, 'Size (Gbs) = '+ cast(TotalGB, 'N') as detail2
from [dbo].[Volumes] v 
join servers s on s.ServerId=v.serverid
where  [PercentageFree] < 10
--and DailyChecks=1
union all

select 'Strange sysadmin login' Message, servername, p.LoginName, 'isntuser: '+cast(p.isntuser as varchar) , 'isntgroup: '+cast(p.isntgroup as varchar)
from Logins p
join servers s on s.ServerId=p.serverid
where p.sysadmin=1
and loginname not in ('sa'

)
union all*/

insert into #t
select '03 - Very Large Log' Messsage, servername, DatabaseName+' / ' + FileName, 'TotalMbs: ' +cast(TotalMbs as varchar), 'AvailableMbs: ' + cast(AvailableMbs as varchar), 1
from vwdatabasefiles f
where filegroupname is null 
and TotalMbs > 30000
and IsActive=1
and DailyChecks=1
and (@ServerId  = 0 or f.serverid=@ServerId)

insert into #t
	select distinct '04 - Volume low in free space' Message
		, servername
		, cast(v.volume_mount_point as varchar(200)) subject
		, 'PercentageFree: '+cast(v.PercentageFree as varchar) detail1
		, 'Size (Gbs) = '+ cast(TotalGB as varchar) as detail2
		, 1
	from [dbo].[Volumes] v 
	join servers s on s.ServerId=v.serverid
	where  [PercentageFree] <= 20
	

insert into #t
select '04 - Old PageVerifyOption' Messsage, servername, DatabaseName, PageVerifyOption, null, 1
from [dbo].[Databases] f
join servers s on s.ServerId = f.ServerId
where [PageVerifyOption] <> 'CHECKSUM'
and ServerName not like 'test%'
and IsActive=1
and DailyChecks=1
AND s.PurposeId=1
and (@ServerId  = 0 or s.serverid=@ServerId)


insert into #t
select '05 - AutoShrink is On' Messsage, servername, DatabaseName, null, null, 1
from [dbo].[Databases] f
join servers s on s.ServerId = f.ServerId
where [is_auto_shrink_on] = 1
and ServerName not like 'test%'
and IsActive=1
and DailyChecks=1
AND s.PurposeId=1
and (@ServerId  = 0 or s.serverid=@ServerId)

insert into #t
select '05 - AutoClose is On' Messsage, servername, DatabaseName, null, null, 1
from [dbo].[Databases] f
join servers s on s.ServerId = f.ServerId
where [is_auto_close_on] = 1
and ServerName not like 'test%'
and IsActive=1
and DailyChecks=1
AND s.PurposeId=1
AND (@ServerId  = 0 or s.serverid=@ServerId)


insert into #t
select '06 - MSDB Backup Missing' Message
	,  ServerName, DatabaseName, 'LastBackup : ' + isnull(convert(varchar,Last_Backup,120),'over 30 days ago') LastFullDate
	, backup_type
	, BackupChecks
from vwMsdb_Backups b
where DaysAgo  > BackupChecks --all dbs should have a full backup at least once a week
and BackupChecks > 0 
and (@ServerId  = 0 or serverid=@ServerId)


insert into #t
select '07 - Job Failure' Messsage, e.servername, job_name, step_name, message, 1
--select *
from [dbo].[vwJobHistory] e
join servers s on s.ServerId = e.ServerId
where  run_status in (0,4)
--and job_name like '%Backup%'
and RunDateTime >= dateadd(dd, -1, getdate())
and e.ServerName not like 'test%'
and step_name <> 'Run agent.'
and len(rtrim(Message)) >5
and s.DailyChecks=1
AND s.PurposeId=1
and (@ServerId  = 0 or s.serverid=@ServerId)


/*
union all

select '07 - rpl Database Lagging ' Messsage, s.servername, DatabaseName, 'Last Update Minutes Behind: ' + cast(iHerReplication_Lag as varchar), 'Last Successfull Import: '+ cast(iHerReplication_LastSuccess as varchar)
from [vwDatabases] d
join servers s on s.ServerId = d.ServerId
where iHerReplication_Lag>60
and s.servername not like 'test%'
and IsActive=1
and DailyChecks=1
*/



insert into #t
select '08 - rpl Table With High Discrepancy' Message,  SubscriberServer, SubscriberDatabase, SubscriberSchemaName+'.'+SubscriberObjectName+ ' / PublisherRowCount = ' + cast(PublisherRowCount as  varchar)+ ' / SubscriberRowCount = ' + cast(SubscriberRowCount as varchar), 'Discrepancy %: '+ cast(Discrepancy as varchar), 1
from  [dbo].[vwRplSubscriptionTable] st
join RplSubscription s on s.rowid = st.RplSubscriptionRowId
join servers v on v.ServerId = st.serverid
where SubscriberServer not like 'test%' and Discrepancy > 20 and PublisherRowCount > 10
and st.IsActive=1
and s.IsActive=1
and SubscriberSchemaName <> 'rpl'
and v.DailyChecks = 1
and isnull(st.ExcludeFromChecks,0) = 0
and (@ServerId  = 0 or s.serverid=@ServerId)
and v.ServerName not like 'dbbackup%'

insert into #t
select distinct '09 - Replication Errors' Message,  l.ServerName, DatabaseName, 'count = ' + cast(count(*)as varchar), 'Message: '+ 
	case when Message like '%deadlocked%' then 'Deadlock'
		when Message like '%network-related%'  or Message like '%transport-level%' then 'Network dropped connection'
		else left(Message,175) end, count(*)
from vwRplImportLog l
join servers v on v.ServerId = l.serverid
where success =0 
and StartDate >= dateadd(dd, -1, getdate())
and Message is not null
and v.DailyChecks=1
and (@ServerId  = 0 or v.serverid=@ServerId)
group by  l.ServerName, DatabaseName
	, case when Message like '%deadlocked%' then 'Deadlock'
		when Message like '%network-related%' or Message like '%transport-level%' then 'Network dropped connection'
		else left(Message,175) end
having count(*) >= 10


insert into #t
	select distinct '10 - LongSql' Message, l.ServerName
		, left('Db: ' +database_name+ '/ User: ' +nt_username  + '/ App: ' +client_app_name, 75) as info
		, left(batch_text, 100) batch_text
		, 'Seconds: '+ cast(sum(Seconds)  as varchar) Seconds
		, sum(Seconds)
	from [vwLongSql] l
	where seconds > 600
	AND l.purposeid=1
	group by l.ServerName
		, left('Db: ' +database_name+ '/ User: ' +nt_username  + '/ App: ' +client_app_name, 75)
		, left(batch_text, 100) 


insert into #t
select '11 - Sql Errors' Message, e.ServerName, 'Login: ' +username
	, case when [errormessage] like '%deadlocked%' then 'Deadlock'
		else left([errormessage], 100) end
	, 'Error Count: '+ cast(count(*) as varchar) Cnt, count(*) Sort
from vwErrors e
join servers v on v.ServerId = e.serverid
where event_timestamp >= cast(dateadd(dd, -1, getdate()) as date)
and e.servername not like 'Test%'
--and errormessage NOT LIKE '%UNIQUE KEY%'
--and errormessage NOT LIKE '%duplicate key%'
--and errormessage NOT LIKE '%PRIMARY KEY%'
and v.DailyChecks=1
and (@ServerId  = 0 or v.serverid=@ServerId)
group by e.ServerName, username
 , case when [errormessage] like '%deadlocked%' then 'Deadlock'
		else left([errormessage], 100) end
having count(*) >= 10


--custom checks
if @CustomChecks=1
begin
	print 'insert custom checks here'
end

--return results
if @PrintSummary = 1
	select distinct Message, count(*) Cnt from #t where Message not like '00%' group by Message ORDER BY Message

select * from #t order by Message, Sort desc, ServerName, Subject

if @Email > '' and @@ROWCOUNT > 0
begin
	DECLARE @xml NVARCHAR(MAX)=''
	DECLARE @body NVARCHAR(MAX)=''

	--open body
	SET @body = @body + '
	<html>
		<H1>DBA Daily Checks</H1>
		<body bgcolor=white>'

	--get publication in error status
	set @xml = null

	--summary
	;with a as (
			SELECT  Message, count(*) Cnt 
			from #t
			where Message not like '00%'
			Group by Message
		)
	SELECT @xml = CAST(
		( SELECT  Message AS 'td' ,''
				 , Cnt    AS 'td' --last one has no comma
			from a
			order by Message
			FOR XML PATH('tr'), ELEMENTS ) 
		AS NVARCHAR(MAX))

	if @xml is not null
		SET @body = @body + '
			<table border = 2>
				<tr>
					<th>Message</th>
					<th>Cnt</th>
				</tr>
				' + @xml +'
			</table>'

	--detail

	SET @xml = CAST(
		( SELECT  Message AS 'td' ,''
				, Servername AS 'td' ,''
				, Subject AS 'td' ,''
				, Detail1 AS 'td',''
				, Detail2   AS 'td' --last one has no comma
			from #t
			order by Message, Sort desc, ServerName, Subject
			FOR XML PATH('tr'), ELEMENTS ) 
		AS NVARCHAR(MAX))

	if @xml is not null
		SET @body = @body + '
			<table border = 2>
				<tr>
					<th>Message</th>
					<th>Servername</th>
					<th>Subject</th>
					<th>Detail1</th>
					<th>Detail2</th>
				</tr>
				' + @xml +'
			</table>'


	--close body
	SET @body = @body + '
		</body>
	</html>'

	print @body

	if len (@body) > 100
		EXEC msdb.dbo.sp_send_dbmail 
		 @recipients =@Email
		,@body = @body
		,@body_format ='HTML'
		,@subject ='DBA Daily Checks'
		,@profile_name ='monitoring'
end

end

GO
/****** Object:  StoredProcedure [dbo].[spDbCompare]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

--select * from vwDatabases where databasename= 'shippingcenter' order by ServerName
CREATE proc [dbo].[spDbCompare]
 @sourceserver varchar(100)='',
 @sourcedb varchar(100)='', 
 @targetserver varchar(100)='',
 @targetdb varchar(100)='',
 @ObjectName varchar(100)='%'
as
begin 

if object_id('tempdb..#source_files') is not null
		 drop table #source_files

select * 
	into #source_files
	from vwDatabasefiles s
	where ServerName =  @sourceserver
	and DatabaseName = @sourcedb

if object_id('tempdb..#target_files') is not null
		 drop table #target_files

select * 
	into #target_files
	from vwDatabasefiles t
	where ServerName =  @targetserver
	and DatabaseName = @targetdb

------------------------------------
if object_id('tempdb..#source_objects') is not null
		 drop table #source_objects

select * 
	into #source_objects
	from vwDatabaseObjects s
	where ServerName =  @sourceserver
	and DatabaseName = @sourcedb
	and SchemaName <> 'rpl'
	and ObjectName not like 'sp_MS%'
	and ObjectName like @ObjectName

if object_id('tempdb..#target_objects') is not null
		 drop table #target_objects

select * 
	into #target_objects
	from vwDatabaseObjects t
	where ServerName =  @targetserver
	and DatabaseName = @targetdb
	and SchemaName <> 'rpl'
	and ObjectName not like 'sp_MS%'
	and ObjectName like @ObjectName

------------------------------------
if object_id('tempdb..#source_indexes') is not null
		 drop table #source_indexes

select table_schema, table_name, index_name, is_unique, is_disabled, sum(size_mbs) size_mbs , cols, included, filter_definition, create_cmd
	into #source_indexes
	from vwIndexUsage s
	where s.ServerName =  @sourceserver
	and DatabaseName = @sourcedb
	and table_schema <> 'rpl'
	and s.index_name not like '%rv'
	and exists (select * from #target_objects t where t.SchemaName = s.table_schema and t.ObjectName = s.table_name and t.Xtype='U')
	group by table_schema, table_name, index_name, is_unique, is_disabled, cols, included, filter_definition, create_cmd

if object_id('tempdb..#target_indexes') is not null
	drop table #target_indexes

select table_schema, table_name, index_name, is_unique, is_disabled, sum(size_mbs) size_mbs , cols, included, filter_definition, create_cmd
	into #target_indexes
	from vwIndexUsage t
	where ServerName =  @targetserver
	and DatabaseName = @targetdb
	and table_schema <> 'rpl'
	and index_name not like '%rv'
	and exists (select * from #source_objects s where s.SchemaName = t.table_schema and s.ObjectName = t.table_name and s.Xtype='U')
	group by table_schema, table_name, index_name, is_unique, is_disabled, cols, included, filter_definition, create_cmd

------------------------------------
if object_id('tempdb..#source_columns') is not null
	drop table #source_columns

select * 
	into #source_columns
	from vwDatabaseObjectColumns s
	where ServerName =  @sourceserver
	and DatabaseName = @sourcedb
	and table_schema <> 'rpl'
	and COLUMN_NAME not in ('rv', 'sourcerv')
	and DATA_TYPE <> 'sysname' 
	and exists (select * from #target_objects t where t.SchemaName = s.table_schema and t.ObjectName = s.table_name and t.Xtype='U')

if object_id('tempdb..#target_columns') is not null
	drop table #target_columns

select * 
	into #target_columns
	from vwDatabaseObjectColumns t
	where ServerName =  @targetserver
	and DatabaseName = @targetdb
	and table_schema <> 'rpl'
	and COLUMN_NAME not in ('rv', 'sourcerv')
	and DATA_TYPE <> 'sysname' 
	and exists (select * from #source_objects s where s.SchemaName = t.table_schema and s.ObjectName = t.table_name and s.Xtype='U')

select 'compare files'

	  if object_id('tempdb..#files') is not null
		 drop table #files

	select 'file missing in target' issue, s.FileName, s.PhysicalName, s.TotalMbs, s.filegroupname
	into #files --select *
	from #source_files s 
	left outer join #target_files t on  s.FileName = t.FileName
	where t.FileName is null

	insert into #files
	select 'file missing in source' issue, t.FileName, t.PhysicalName, t.TotalMbs, t.filegroupname
	from #source_files s 
	right outer join #target_files t on  s.FileName = t.FileName
	where s.FileName is null
		
	select * from #files order by 1,2,3

	select 'file location mismatch' issue, s.FileName, s.PhysicalName, t.PhysicalName
	from #source_files s 
	join #target_files t on s.FileName = t.FileName
	where s.PhysicalName <> t.PhysicalName

select 'compare objects (tables, procs, views, functions)'

	  if object_id('tempdb..#objects') is not null
		 drop table #objects

	select 'object missing in target' issue, s.SchemaName, s.ObjectName, s.Xtype, s.[RowCount]
	into #objects
	from #source_objects s 
	left outer join #target_objects t on  s.SchemaName = t.SchemaName
		and s.ObjectName = t.ObjectName
	where t.DatabaseName is null
	
	insert into #objects
	select 'object missing in source' issue, t.SchemaName, t.ObjectName, t.Xtype, t.[RowCount] 
	from #source_objects s 
	right outer join #target_objects t on s.SchemaName = t.SchemaName
		and s.ObjectName = t.ObjectName
	where s.DatabaseName is null
	
	select * from #objects order by 1,2,3

	select 'object definition mismatch' issue, t.SchemaName, t.ObjectName, t.Xtype, s.ROUTINE_DEFINITION Source_Definition, t.ROUTINE_DEFINITION Target_Definition, s.DatabaseObjectId SDatabaseObjectId, t.DatabaseObjectId TDatabaseObjectId
	from #source_objects s 
	join #target_objects t on s.SchemaName = t.SchemaName
		and s.ObjectName = t.ObjectName
		and s.xtype = t.Xtype 
	where checksum(ltrim(rtrim(replace(s.ROUTINE_DEFINITION,' ','')))) <> checksum(ltrim(rtrim(replace(t.ROUTINE_DEFINITION,' ',''))))
	
select 'compare indexes'
	if object_id('tempdb..#indexes') is not null
		 drop table #indexes

	select 'index missing in target' issue, s.*
	into #indexes
	from #source_indexes s 
	left outer join #target_indexes t on s.table_schema = t.table_schema
		and s.table_name = t.table_name
		and s.index_name = t.index_name
	where t.table_schema is null

	insert into #indexes
	select 'index missing in source' issue, t.*
	from #source_indexes s
	right outer join #target_indexes t on  s.table_schema = t.table_schema
		and s.table_name = t.table_name
		and s.index_name = t.index_name
	where s.table_schema is null
	
	select * from #indexes order by 1,2,3,4

select 'compare rowcounts'
	 if object_id('tempdb..#rows') is not null
		 drop table #rows
	 
	select 'rowcount mismatch' issue
		, s.SchemaName, s.ObjectName
		, s.[RowCount] SourceRows,  t.[RowCount] TargetRows,  abs(s.[RowCount]-  t.[RowCount]) diff
	 into #rows
	 from #source_objects s 
	 join #target_objects t on s.SchemaName = t.SchemaName
		and s.ObjectName = t.ObjectName
		and s.Xtype = 'U'
		and t.Xtype = 'U'
	where (isnull(s.[RowCount],0) <> isnull(t.[RowCount],0))
	order by diff desc

	select * from #rows order by 1,2

select 'compare columns'
	 if object_id('tempdb..#columns') is not null
		 drop table #columns

	select 'column missing in target' issue, s.TABLE_SCHEMA, s.TABLE_NAME, s.COLUMN_NAME, s.DATA_TYPE
	into #columns
	from #source_columns  s
	left outer join #target_columns t on --s.TABLE_CATALOG = t.TABLE_CATALOG
		 s.TABLE_SCHEMA = t.TABLE_SCHEMA
		and s.TABLE_NAME = t.TABLE_NAME
		and s.COLUMN_NAME = t.COLUMN_NAME
	where t.COLUMN_NAME is null

	insert into #columns
	select 'column missing in source' issue, t.TABLE_SCHEMA, t.TABLE_NAME, t.COLUMN_NAME, t.DATA_TYPE
	from #source_columns s
	right outer join #target_columns t on --s.TABLE_CATALOG = t.TABLE_CATALOG
		 s.TABLE_SCHEMA = t.TABLE_SCHEMA
		and s.TABLE_NAME = t.TABLE_NAME
		and s.COLUMN_NAME = t.COLUMN_NAME
	where s.COLUMN_NAME is null
	
	select * from #columns order by 1,2,3,4,5

	select 'column type mismatch' issue, t.TABLE_SCHEMA, t.TABLE_NAME, t.COLUMN_NAME, s.DATA_TYPE Source_Type, t.DATA_TYPE Target_Type
	from #source_columns s
	join #target_columns t on-- s.TABLE_CATALOG = t.TABLE_CATALOG
		 s.TABLE_SCHEMA = t.TABLE_SCHEMA
		and s.TABLE_NAME = t.TABLE_NAME
		and s.COLUMN_NAME = t.COLUMN_NAME
	where s.DATA_TYPE <> t.DATA_TYPE


end 

GO
/****** Object:  StoredProcedure [dbo].[spDisableConstraints]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

 CREATE proc [dbo].[spDisableConstraints] 
as
declare @table varchar(200), @sql varchar(max)
 
declare t_cursor cursor fast_forward for
	select name from sysobjects where xtype='u' and uid=1
open t_cursor
fetch next from t_cursor into @table
while @@fetch_Status=0
begin
	set @sql = '
	alter table '+@table+' nocheck constraint all
	alter table '+@table+' disable trigger all
	'
	exec (@sql)
	fetch next from t_cursor into @table
end
close t_cursor
deallocate t_cursor

GO
/****** Object:  StoredProcedure [dbo].[spDS_PublisherDatabase]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE proc [dbo].[spDS_PublisherDatabase] @ServerId int=0
as
select * from vwDatabases
where gSync_Published_Tables > 0
and ServerId = @ServerId
order by DatabaseName
GO
/****** Object:  StoredProcedure [dbo].[spDS_PublisherServer]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE proc [dbo].[spDS_PublisherServer]
as
select * from vwServers
where IsActive=1
and GSync_Published_Dbs > 0
order by ServerName

GO
/****** Object:  StoredProcedure [dbo].[spEnableConstraints]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
 CREATE proc [dbo].[spEnableConstraints] 
as
declare @table varchar(200), @sql varchar(max)
 
declare t_cursor cursor fast_forward for
	select name from sysobjects where xtype='u' and uid=1
open t_cursor
fetch next from t_cursor into @table
while @@fetch_Status=0
begin
	set @sql = '
	alter table '+@table+' check constraint all
	alter table '+@table+' enable trigger all
	'
	exec (@sql)
	fetch next from t_cursor into @table
end
close t_cursor
deallocate t_cursor
GO
/****** Object:  StoredProcedure [dbo].[spExec]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


/****** Object:  StoredProcedure [dbo].[spExecCommand]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create   proc [dbo].[spExecCommand] 
as
begin
	declare @sql varchar(max)
		, @message varchar(max)
		, @CommandId int
		, @ServerName varchar(100)
		, @Command varchar(max)

	declare @t table (CommandId int, ServerName varchar(100), command varchar(max) )

	update Command set StartDate = getdate()
	output deleted.CommandId, deleted.ServerName, deleted.Command into @t 
	where CommandId = ( 
		select top 1 CommandId from Command
		where StartDate is null
		order by Priority desc, CommandId
		)

	select @CommandId = t.CommandId
		, @ServerName = t.ServerName
		, @Command = t.Command
	from @t t

	begin  try
		set @sql = 'exec ('''+replace(@Command,'''','''''')+''') at ['+@ServerName+']'
		exec (@sql)
		update c set EndDate = getdate()
		from Command c
		join @t t on c.CommandId = t.CommandId
	end try
	begin catch
		select @CommandId CommandId, @Command Command, ERROR_MESSAGE() Message

		update c set EndDate = getdate()
			, Message = ERROR_MESSAGE()
		from Command c
		join @t t on c.CommandId = t.CommandId
	end catch

end
GO
/****** Object:  StoredProcedure [dbo].[spExportDeadlocks]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [dbo].[spExportDeadlocks]
as
DECLARE @id VARCHAR(10), @sql VARCHAR(MAX)
DECLARE T_cursor CURSOR FAST_FORWARD FOR	
	SELECT deadlockid 
	FROM dbo.vwDeadLocks  
	where event_timestamp > dateadd(dd, -30, getdate())
	-- and (isExported = 0 or isExported is null) 
open t_cursor
FETCH NEXT FROM t_cursor INTO @id
WHILE @@FETCH_STATUS=0
BEGIN
	SET @id = REPLICATE('0', 6-LEN(@id) )+@id 
	SET @sql = 'exec master..xp_cmdshell ''bcp "select top 1 event_data from dba..deadlocks where deadlockid='+@id+'" queryout "c:\inetpub\wwwroot\Servers\Deadlocks\deadlock_'+@id+'.xml" -c -T -S '+@@servername+''''
	EXEC (@sql)

	update Deadlocks set isExported = 1 where deadlockid = @id
	FETCH NEXT FROM t_cursor INTO @id
end
CLOSE t_cursor
DEALLOCATE t_cursor

GO
/****** Object:  StoredProcedure [dbo].[spExportLongSql]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE PROC [dbo].[spExportLongSql]
as
DECLARE @id VARCHAR(10), @sql VARCHAR(MAX)
DECLARE T_cursor CURSOR FAST_FORWARD FOR	
	SELECT LongSqlId FROM dbo.LongSql 
	where  event_timestamp > dateadd(dd, -5, getdate()) 
		and len(sql_text)>1000
		and (isExported = 0 or isExported is null )
open t_cursor
FETCH NEXT FROM t_cursor INTO @id
WHILE @@FETCH_STATUS=0
BEGIN
	SET @id = REPLICATE('0', 6-LEN(@id) )+@id 
	SET @sql = 'exec master..xp_cmdshell ''bcp "select top 1 sql_text from servers..LongSql where LongSqlid='+@id+'" queryout "c:\inetpub\wwwroot\Servers\longsql\longsql_'+@id+'.htm" -c -T -S '+@@servername+''''
	EXEC (@sql)

	update Deadlocks set isExported = 1 where deadlockid = @id
	FETCH NEXT FROM t_cursor INTO @id
end
CLOSE t_cursor
DEALLOCATE t_cursor


GO
/****** Object:  StoredProcedure [dbo].[spHourlyChecks]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE   proc [dbo].[spHourlyChecks] (@Email varchar(100) ='', @Minutes smallint=60)
as

set @Minutes = abs(@Minutes)


if OBJECT_ID('tempdb..#t') is not null
	drop table #t

create table #t (
	Message varchar(1000),
	Servername varchar(1000),
	Subject varchar(1000),
	detail1 varchar(8000),
	detail2 varchar(8000)
)

set nocount on

;
with base as (
/*Moved to ServerResponseChecks
	--low space volumes
	select '00 - Server is disabled' Message, servername, EnvironmentName subject, PurposeName detail1, Error detail2
	from vwServers s where s.IsActive=0 and DailyChecks=1
	union all
*/
	select distinct '01 - Volume low in free space' Message
		, servername
		, cast(v.volume_mount_point as varchar(200)) subject
		, 'PercentageFree: '+cast(v.PercentageFree as varchar) detail1
		, 'Size (Gbs) = '+ cast(TotalGB as varchar) as detail2
	from [dbo].[Volumes] v 
	join servers s on s.ServerId=v.serverid
	where  [PercentageFree] <= 15 and s.DailyChecks=1
	--AND s.PurposeId=1
	union all

	select '02 - Large File Growth' Message, f.ServerName, f.DatabaseName, f.PhysicalName, 'Growth = ' + cast(Growth as varchar) + ' Mb, NewSize = '+cast(f.TotalMbs as varchar)
		--, *
	from vwDatabaseFiles f
	cross apply (
		select top 1 *
			, f.TotalMbs - h.TotalMbs as Growth
			, 1- h.TotalMbs*1.0 / f.TotalMbs Ratio
		from DatabaseFilesHist h
		where h.ServerName = f.ServerName
		and h.DatabaseName = f.DatabaseName
		and h.PhysicalName = f.PhysicalName
		and Date <= dateadd(mi, -@Minutes, getdate())
		order by RowId desc
	) h
	where f.TotalMbs > 0
	and Growth > 5000
	--AND PurposeId=1
	
	union all
	select '03 - Job Stuck' Message
		, r.ServerName  
		, r.Job_Name
		, 'CurrentSeconds: '+ replace(cast(Seconds as varchar),'.00','') + ' / Since: '+ convert(varchar, start_execution_date, 109)
		, 'Average Seconds over last '+cast(RecentRunCount as varchar)+' runs: '+replace(cast(AvgSeconds as varchar),'.00','')
	from vwJobsRunning r
	where  Seconds > 2 * AvgSeconds
	and Seconds > 600

	union all
	
	select '04 Job Failure' Messsage, servername, job_name, step_name, message
	--select *
	from [dbo].[vwJobHistory] e
	where  run_status in (0,4)
	--and job_name like '%Backup%'
	and RunDateTime >= dateadd(mi, -@Minutes, getdate())
	and ServerName not like 'test%'
	and step_name <> 'Run agent.'
	and len(rtrim(Message)) >5
	--AND PurposeId=1
	
	union all

	select '07 - rpl Replication Lagging ' Messsage, s.servername, DatabaseName, 'Last Update Minutes Behind: ' + cast(GsyncReplication_Lag as varchar), 'Last Successfull Import: '+ cast(GsyncReplication_LastSuccess as varchar)
	from [vwDatabases] d
	join servers s on s.ServerId = d.ServerId
	where GsyncReplication_Lag>60
	and s.servername not like 'test%'
	and s.IsActive=1
	and s.DailyChecks=1
	
)
insert into #t
select * from base
order by 1,2,3,4

select * from #t	

if @Email > '' and @@ROWCOUNT > 0
begin
	DECLARE @xml NVARCHAR(MAX)=''
	DECLARE @body NVARCHAR(MAX)=''

	--open body
	SET @body = @body + '
	<html>
		<H1>DBA Hourly Checks</H1>
		<body bgcolor=white>'

	--get publication in error status
	set @xml = null
	SET @xml = CAST(
		( SELECT  Message AS 'td' ,''
				, Servername AS 'td' ,''
				, Subject AS 'td' ,''
				, Detail1 AS 'td',''
				, Detail2   AS 'td' --last one has no comma
			from #t
			--order by 1,2,3,4
			FOR XML PATH('tr'), ELEMENTS ) 
		AS NVARCHAR(MAX))

	if @xml is not null
		SET @body = @body + '
			<table border = 2>
				<tr>
					<th>Message</th>
					<th>Servername</th>
					<th>Subject</th>
					<th>Detail1</th>
					<th>Detail2</th>
				</tr>
				' + @xml +'
			</table>'


	--close body
	SET @body = @body + '
		</body>
	</html>'

	print @body

	if len (@body) > 100
		EXEC msdb.dbo.sp_send_dbmail 
		 @recipients = @Email
		,@body = @body
		,@body_format ='HTML'
		,@subject ='DBA Hourly Checks'
		,@profile_name ='monitoring'
	
end



GO
/****** Object:  StoredProcedure [dbo].[spInventory]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE proc [dbo].[spInventory]
as

set nocount on

if object_id('tempdb..#Inventory') is not null
drop table #Inventory

create table #Inventory
	(
	SchemaName varchar(50),
	TableName varchar(128),
	TotalRows int,		
	Reserved varchar(20),
	Data varchar(20),
	IndexSize varchar(20),
	Unused varchar(20),
	TotalColumns int,
	RowSize int,
	ReferencedBy int,
	ReferenceTo int
	)

declare @sql varchar(8000), @table varchar(255), @schema varchar(50)
declare t_cursor cursor fast_forward for
	select table_schema, table_name
	from information_schema.tables where table_type='base table' 
	order by 1,2
open t_cursor
fetch next from t_cursor into @schema, @table
while @@fetch_status = 0
begin
	set @sql = 'insert into #inventory (TableName , TotalRows, Reserved, Data, IndexSize,	Unused) 
	exec sp_spaceused ''' + @schema+'.' + @table+''''
	print @sql
	exec (@sql)
	update #Inventory set SchemaName = @schema where SchemaName is null
	
	fetch next from t_cursor into @schema, @table
end
close t_cursor
deallocate t_cursor

if object_id('tempdb..#Sys') is not null
drop table #Sys

select distinct o.name, count(*) TotalColumns, sum(length) RowSize
	into #sys
	from sysobjects o 
	inner join syscolumns c on o.id=c.id
	where o.type='u'
	group by o.name

update i set TotalColumns = s.TotalColumns, Rowsize = s.RowSize
from #Inventory i 
inner join #sys s on s.name=i.tablename


select * from #Inventory order by TotalRows desc



GO
/****** Object:  StoredProcedure [dbo].[spj_MonitorDeadlocks]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[spj_MonitorDeadlocks]
AS
BEGIN

    IF @@ROWCOUNT > 0
    BEGIN
        DECLARE @tableHTML NVARCHAR(MAX);
        DECLARE @DBA VARCHAR(100) = 'it-sqlalerts-dba@domain.com';
        DECLARE @today DATETIME = CAST(GETDATE() AS DATE);
        SET @tableHTML
            = N'<html><body><h2>Daily Count of Deadlocks by Server</h2>' + N'<table border="1" width="100%">'
              + N'<tr bgcolor="LightSkyBlue"><td>Server</td><td>Deadlocks</td></tr>'
              + CAST(
                (
                    SELECT td = servername,
                        '',
                        td = COUNT(*),
                        ''
                    FROM vwDeadLocks
                    WHERE event_timestamp > @today
                    GROUP BY servername
                    FOR XML PATH('tr'), TYPE
                ) AS NVARCHAR(MAX)) + N'</table></body></html>';

        EXEC msdb.dbo.sp_send_dbmail @recipients = @DBA,
            @subject = 'Deadlock Alert',
            @profile_name = 'DBAs',
            @body = @tableHTML,
            @body_format = 'HTML';
    END;

END;

--1/30/2018 Roger B. New SP to monitor deadlocks across servers
GO
/****** Object:  StoredProcedure [dbo].[spJob_Failure_Notification]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
create proc [dbo].[spJob_Failure_Notification] @days int = 1, @recipients varchar(255) = 'giraom@gmail.com'
as
DECLARE @tableHTML  NVARCHAR(MAX) 

if object_id ('tempdb..#t') is not null
	drop table #t

SELECT c.RunDateTime,      
       c.job_name, 
       c.step_name, 
       c.run_duration, 
       c.command, 
       a.[message]
into #t
FROM    msdb.dbo.sysjobhistory a ( NOLOCK )
INNER JOIN ( SELECT TOP 999999999
					j.name AS job_name
				  , jh.step_name
				  , jh.step_id
				  , jh.job_id
				  , js.subsystem
				  , LEFT(js.command, 4000) AS command
				  , js.output_file_name
				  , jh.run_date
				  , jh.run_time
				  , CAST(STUFF(STUFF(CAST(jh.run_date AS VARCHAR), 5, 0, '/'), 8, 0, '/') + ' ' + STUFF(STUFF(RIGHT('000000'
																													+ CAST(jh.run_time AS VARCHAR), 6),
																											  3, 0, ':'), 6, 0, ':') AS DATETIME) AS RunDateTime
				  , jh.run_duration
				  , jh.instance_id
				FROM   msdb.dbo.sysjobhistory jh ( NOLOCK )
				INNER JOIN msdb.dbo.sysjobs j ( NOLOCK ) ON jh.job_id = j.job_id
				INNER JOIN msdb.dbo.sysjobsteps js ( NOLOCK ) ON jh.job_id = js.job_id AND js.step_id = jh.step_id
				WHERE  run_status IN ( 0, 4 )
					AND jh.step_id > 0
					AND jh.run_date >= CAST(CONVERT(VARCHAR(8), DATEADD(DAY, -@days, GETDATE()), 112) AS INT)
				ORDER BY j.job_id
				  , jh.step_id
				  , jh.run_date
				  , jh.run_time
				) c ON a.step_id = c.step_id
				AND a.job_id = c.job_id
				AND a.run_date = c.run_date
				AND a.run_time = c.run_time
WHERE   a.run_status IN ( 0, 4 )
	AND a.step_id > 0
	AND a.run_date >= CAST(CONVERT(VARCHAR(8), DATEADD(DAY, -@days , GETDATE()), 112) AS INT)
	and c.subsystem not in ('Snapshot','Distribution')
order by 1,2,3;

if exists (select * from #t)
begin
	select RunDateTime,      
       job_name, 
       step_name, 
       run_duration, 
       '"'+command+'"' command,
       '"'+[message]+'"' message
	 from #t

	SET @tableHTML =
		N'<H1>Job Failure</H1>' +
		N'<table border="1">' +
		N'<tr><th>Date</th><th>Job</th><th>Step</th><th>Duration</th><th>Command</th><th>Message</th></tr>' +
		CAST ( ( SELECT td = RunDateTime,       '',
						td = job_name, '',
						td = step_name, '',
						td = run_duration, '',
						td = command, '',
						td = [message]
				  from #t
				  FOR XML PATH('tr'), TYPE 
		) AS NVARCHAR(MAX) ) +
		N'</table>' ;

	/*	
	   EXEC msdb.dbo.sp_send_dbmail  
		@profile_name = 'Email',  
		@recipients = @recipients,
		@subject = 'Job Failure' ,
		@body_format = 'HTML',
		@body = @tableHTML ;  
		*/
end



GO
/****** Object:  StoredProcedure [dbo].[spJobStart]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE proc [dbo].[spJobStart] @ServerName varchar(100)='%', @JobName varchar(100) ='gSyncReplicate - Catalog%', @debug	bit=0, @exec	bit=1
as

declare @sql varchar(max)=''

select 
	 @sql = @sql +  'EXEC ['+ServerName+'].msdb.dbo.sp_update_job @job_name='''+JobName+''', @enabled = 1'+char(13)
from vwjobs
where ServerName like @ServerName
and JobName like @JobName
order by JobName

exec spExec @sql, @debug, @exec

GO
/****** Object:  StoredProcedure [dbo].[spJobStop]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE proc [dbo].[spJobStop] @ServerName varchar(100)='%', @JobName varchar(100) ='gSyncReplicate - Catalog%', @debug	bit=0, @exec	bit=1
as

declare @sql1 varchar(max)='', @sql2 varchar(max)=''

select 
	@sql1 = @sql1 + 'EXEC ['+ServerName+'].msdb.dbo.sp_stop_job @job_name='''+JobName+''' '+char(13)
	, @sql2 = @sql2 +  'EXEC ['+ServerName+'].msdb.dbo.sp_update_job @job_name='''+JobName+''', @enabled = 0'+char(13)
from vwjobs
where ServerName like @ServerName
and JobName like @JobName
order by JobName

exec spExec @sql1, @debug, @exec
exec spExec @sql2, @debug, @exec

GO
/****** Object:  StoredProcedure [dbo].[spLoad]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



create table dbo.LinkedServers (
	[ServerId] [int] NULL,
	[ServerName] varchar(255), LinkedServer varchar(255), providername varchar(255), datasource varchar(255)
)
go

CREATE proc [dbo].[spLoadLinkedServers]  @serverid int=0
as
declare @sql nvarchar(max)

	set @sql='
		select '+cast(@serverid as varchar)+' , @@SERVERNAME, srvname, providername, datasource from master..sysservers
	   '
	begin try
		insert into LinkedServers ([ServerId], [ServerName], LinkedServer, providername, datasource)
		exec dbo.spExec @sql = @sql, @raiserror = 0
	end try
	begin catch
		print error_message()
		print @sql
	end catch


GO

GO


CREATE   proc [dbo].[spLoadADGroupMembers]  @serverid int=0, @ADGroup varchar(100)=''
as
declare @Group varchar(100), @Server varchar(100), @sql varchar(1000)

if @serverid = 0 and isnull(@ADGroup,'') = ''
	truncate table ADGroupMembers
else if isnull(@ADGroup,'') <> ''
	delete from ADGroupMembers where permission_path = @ADGroup

--in some cases xp_logininfo only works when called from the remote server, other times from local, probably due to the fact we have multiple domains without trust
--first we try to pull remote, then we try local, if we get the members from one server then we dont try again the same group on another server

declare t_cursor cursor fast_forward for
	select distinct LoginName, ServerName
	from vwLogins
	where isntgroup = 1
	and LoginName not like '%$%'
	and LoginName not in ('BUILTIN\Administrators')
	and (isnull(@ADGroup,'') = '' or LoginName=@ADGroup)
	and (ServerId = @serverid or @serverid = 0 )

open t_cursor
fetch next from t_cursor into @Group, @Server 
while @@FETCH_STATUS=0
begin
	--check if group was already loaded from another server
	if not exists (select * from ADGroupMembers where permission_path = @Group)
	begin
		begin try
			--PRINT @Group
			--try to get from remote server
			if @SERVER = @@SERVERNAME
				set @SERVER = '.' 
			--make sure local table exists on remote server
			set @sql='
			declare @sql varchar(max)
			set @sql = ''
				declare @sql varchar(max)
				if object_id (''''tempdb.dbo.ADGroupMembers'''') is null
					set @sql = ''''
							create table tempdb.dbo.ADGroupMembers(
								 account	varchar(100)
								, type	varchar(20)	
								, privilege	varchar(20)	
								, mapped_login varchar(100)
								, permission_path	varchar(100)
							)''''
				else 
					set @sql = ''''delete from tempdb.dbo.ADGroupMembers''''
				exec (@sql)
				''
			Exec (@sql) at ['+@SERVER+']'

			exec dbo.spExec @sql= @sql, @raiserror=1
	
			--load data into remote table
			set @sql='
			declare @sql varchar(max)
			set @sql = ''insert into tempdb..ADGroupMembers (account,type,privilege,mapped_login,permission_path)
			EXEC master.dbo.xp_logininfo '''''+@Group+''''', ''''members'''' ''
	
			Exec (@sql) at ['+@SERVER+']'
		
			exec (@sql)

			SET @SQL = 'SELECT account,type,privilege,mapped_login,permission_path
				FROM OPENQUERY(['+@SERVER+'], ''select * from tempdb.dbo.ADGroupMembers'') AS a;'
	
			insert into ADGroupMembers(account,type,privilege,mapped_login,permission_path)
			exec (@sql)
		end try
		begin catch	
			print @sql
			print error_message()
			--try pulling from local server
			begin try
				set @sql = 'EXEC master.dbo.xp_logininfo '''+@Group+''', ''members'' '
				insert into ADGroupMembers(account,type,privilege,mapped_login,permission_path)
				exec (@sql)
			end try
			begin catch
				print @sql
				print error_message()
			end catch
		end catch
	end
	fetch next from t_cursor into @Group, @Server 
end
close t_cursor
deallocate t_cursor

if @serverid = 0 and isnull(@ADGroup,'') = ''
begin
	declare t_cursor cursor fast_forward for
			--groups within groups
			SELECT distinct account FROM dbo.ADGroupMembers WHERE type='group' AND account NOT IN (SELECT DISTINCT permission_path FROM dbo.ADGroupMembers)
			union 
			--second try for sql ntgroup logins missing
			select Distinct LoginName adgroup from vwLogins where isntgroup=1 and LoginName not in (SELECT DISTINCT permission_path FROM dbo.ADGroupMembers)
	open t_cursor
	fetch next from t_cursor into @Group 
	while @@FETCH_STATUS=0
	BEGIN
		begin try
			--PRINT @Group
			set @sql='EXEC master.dbo.xp_logininfo '''+@Group+''', ''members'' '
	
			insert into ADGroupMembers(account,type,privilege,mapped_login,permission_path)
			exec (@sql)
		end try
		begin catch
			print @sql
			print error_message()
		end catch	
		fetch next from t_cursor into @Group 
	end
	close t_cursor
	deallocate t_cursor
end

--delete dupliates keeping only the first entry, in case the group was loaded multiple times
delete a from ADGroupMembers a
where exists (select * from ADGroupMembers b
	where b.account = a.account
	and b.permission_path = a.permission_path
	and b.rowid > a.rowid
	)

GO
/****** Object:  StoredProcedure [dbo].[spLoadArticles]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



CREATE proc [dbo].[spLoadArticles]   @serverid int=0
as
declare @sql nvarchar(max), @SERVER VARCHAR(100)
	, @publisherid int, @publisher varchar(100), @Distribution_db varchar(100)

--truncate table Articles


DECLARE server_cursor CURSOR FAST_FORWARD FOR
	SELECT SERVERNAME, serverid FROM vwServersNonAzureSql s
	where isActive=1 
	and (	s.ServerId = @serverid
			or (@serverid =0 and not exists(select * from Articles d where s.ServerId =d.serverid))
		)
OPEN server_cursor
FETCH NEXT FROM server_cursor INTO @SERVER, @serverid 
WHILE @@FETCH_STATUS=0
BEGIN
	declare publisher_cursor cursor fast_forward for
		select  publisherid, p.PublisherName, p.distribution_db from [Publishers] p where Active=1 and Serverid = @serverid
	open publisher_cursor
	FETCH NEXT FROM publisher_cursor INTO @publisherid, @publisher, @Distribution_db
	while @@FETCH_STATUS=0
	begin
		begin try

			set @sql='
			select PublisherId = '+cast(@publisherid as varchar)+', serverId= '+cast(@serverid as varchar)+', Publication_Id, Article_Id
				,article,destination_object,source_owner,source_object,description,destination_owner 
			from ['+@Distribution_db+'].[dbo].[MSArticles]'

			SET @SQL = 'SELECT a.*	FROM OPENQUERY(['+@SERVER+'], 
			'''+replace(@sql, '''', '''''')+'''
			) AS a;'

			insert into Articles (PublisherId,serverId, remote_publication_id, remote_article_id, article,destination_object,source_owner,source_object,description,destination_owner )
			exec dbo.spExec @sql
		end try
		begin catch
			print error_message()
			print @sql
		end catch
		FETCH NEXT FROM publisher_cursor INTO @publisherid, @publisher, @Distribution_db
	end
	close publisher_cursor
	deallocate publisher_cursor
	FETCH NEXT FROM server_cursor INTO @SERVER, @serverid
END
CLOSE server_cursor
DEALLOCATE server_cursor

update a set PublicationId = p.PublicationId
from Articles a 
join Publications p on a.remote_publication_id = p.remote_publication_id
	and a.ServerId = p.ServerId and a.PublisherId = p.PublisherId



GO
/****** Object:  StoredProcedure [dbo].[spLoadAvailabilityGroups]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE   proc [dbo].[spLoadAvailabilityGroups] @serverid int=0
as

declare @sql nvarchar(max), @SERVER VARCHAR(100)
DECLARE T_CURSOR CURSOR FAST_FORWARD FOR
	SELECT SERVERNAME, serverid FROM vwServersVms s
	where isActive=1 
	and Build >= '10'
	and (	s.ServerId = @serverid
			or (@serverid =0 and not exists(select * from AvailabilityGroups d where s.ServerId =d.serverid))
		)
	
OPEN T_CURSOR
FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid 
WHILE @@FETCH_STATUS=0
BEGIN
	set @sql='
	SELECT serverid = '+cast(@serverid as varchar)+',
		name as AGname,
		replica_server_name,
		CASE WHEN  (primary_replica  = replica_server_name) THEN  1	ELSE  0 END AS IsPrimaryServer,
		secondary_role_allow_connections AS ReadableSecondary,
		[availability_mode]  AS [Synchronous],
		failover_mode_desc,
		states.synchronization_health_desc--, *
	FROM master.sys.availability_groups Groups
	INNER JOIN master.sys.availability_replicas Replicas ON Groups.group_id = Replicas.group_id
	INNER JOIN sys.dm_hadr_availability_group_states gs ON gs.group_id = Groups.group_id
	INNER JOIN sys.dm_hadr_availability_replica_states states ON Replicas.replica_id = states.replica_id
	'
	
	SET @SQL = 'SELECT a.*	FROM OPENQUERY(['+@SERVER+'], 
		'''+replace(@sql, '''', '''''')+'''
		) AS a;'
	begin try
		insert into AvailabilityGroups (ServerId,AvailabiityGroup,replica_server_name,IsPrimaryServer,ReadableSecondary,Synchronous,failover_mode_desc,synchronization_health_desc)
		exec dbo.spExec @sql
	end try
	begin catch
		print error_message()
		print @sql
	end catch
	FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid
END
CLOSE T_CURSOR
DEALLOCATE T_CURSOR
--select * from jobs



GO
/****** Object:  StoredProcedure [dbo].[spLoadBackups]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE proc [dbo].[spLoadBackups]   @serverid int=0 --@folders varchar(8000)='', @separator char(1)=','
as
/* may need to enable xm_cmdshell
exec sp_configure 'show Advanced Options',1
Reconfigure

exec sp_configure 'xp_cmdshell',1
Reconfigure
*/

if @serverid=0
begin
	delete from dbo.BackupFiles
	delete from dbo.BackupFolders
end
else 
begin
	delete from dbo.BackupFiles where serverid = @serverid
	delete from dbo.BackupFolders where serverid = @serverid
end

declare @cmd varchar(8000)

--auxiliary tables
if object_id('tempdb..#dir') is not null
	drop table #dir

create table #dir (line varchar(8000), serverid int, rowid int identity)

if object_id('tempdb..#folders') is not null
	drop table #folders

create table #folders (folder varchar(8000), start int, lastsubfolder int, finish int, size bigint, subfolders int, files int, biggest_file_size bigint, serverid int)

CREATE UNIQUE INDEX  udx_folders_start ON #folders (start) include(finish)

if object_id('tempdb..#files') is not null
	drop table #files

create table #files (rowid int, name varchar(8000), date datetime, size bigint, folderid int, line varchar(8000), serverid int)

CREATE UNIQUE INDEX  udx_files_rowid ON #files (rowid)
CREATE INDEX  udx_files_folder ON #files (folderid)

declare t_cursor cursor fast_forward for
	--select 'dir '+value+' /s' from STRING_SPLIT (@folders, @separator ) 
	select 'dir '+backupfolder+' /s' , serverid
	from servers 
	where IsActive = 1 and version not like '%azure%'
	and backupfolder is not null 
	and backupfolder <> '' 
	and serverid = case when @serverid =0 then serverid else  @serverid end
open t_cursor
fetch next from t_cursor into @cmd , @serverid
while @@FETCH_STATUS=0
begin
	truncate table #dir
	truncate table #folders
	truncate table #files

	insert into #dir (line)
	exec xp_cmdshell @cmd
	--wrap up last dir
	insert into #dir values(' Directory of ENDOFTREEE',0)

	update #dir set serverid = @serverid where serverid is null or serverid=0

	
	insert into #folders (folder, start, serverid)
		select substring(d.line, 15, 8000) folder
			, rowid 
			, serverid 
		from #dir d
		where d.line like ' Directory of %'
		
	update f set lastsubfolder	= l.start
			, subfolders = cnt
		from #folders f	
		cross apply (select max(start) start, COUNT(*) cnt from #folders f2 where f2.start >= f.start and f2.folder like f.folder +'%') l
	
	update f set finish = l.start
			from #folders f		
			cross apply (select min(start) start from #folders f2 where f2.start > f.lastsubfolder) l

	update #folders set finish = lastsubfolder where finish is null
	 
	insert into #files (rowid, name, [date], size, folderid, line, serverid)
	select rowid
		, SUBSTRING(line, 40, 255) name
		, SUBSTRING(line, 1, 20)  [date]
		, replace(SUBSTRING(line, 21, 19),',','') size
		, null--f.start
		, line
		, serverid 
	from #dir d
	--outer apply (select MAX(start) start from #folders f where f.start < d.rowid) f
	where line not like '%<DIR>%'
	and line not like '%File(s)%bytes'
	and line not like ' Directory Of %'
	and rowid>2
	and ISDATE(SUBSTRING(line, 1, 20) )=1
	and isnumeric(SUBSTRING(line, 21, 19))=1
	order by rowid

	--this takes a long time and may not be necessary
	update fi set folderid = f.start
	from #files fi
	cross apply (select MAX(start) start from #folders f where f.start < fi.rowid) f


	 update f set size = fi.size
			, files = fi.cnt
			, biggest_file_size= fi.[max]
		from #folders f		
		outer apply (
			select SUM(size) size, COUNT(*) cnt, max(size) [max]
			from #files fi
			where fi.rowid between f.start and f.finish
			) fi


	insert into dbo.BackupFolders (FolderId,folder,size, subfolders, files,biggest_file,serverid)
	select start as FolderId, folder, size, subfolders, files, biggest_file_size/1024/1024/1024 biggest_file,serverid
	 from #folders

	;with base as (
		select  rowid as FileId, name, date, size, folderid, size/1024/1024/1024 Gbs
			,case when name like '%DIFF%.bak' then 'DIFF'
				when name like '%.trn' then 'LOG'
				when name like '%.bak' then 'FULL'
				end as Type
			, charindex('_201', name)-1 p1, ServerId
			--, charindex('_', name, charindex('_', name)+1) p2
		FROM #files f
		where name like '%_201%'
		)
	insert into dbo.BackupFiles (FileId, name, date, size, folderid, Gbs ,Type, p1, DatabaseName, serverid)
	select f.FileId,  f.name, f.date, f.size, f.folderid, f.Gbs, f.Type, f.p1
		, SUBSTRING(f.name, 1, case when isnumeric(f.p1)=1 and f.p1 > 0 then f.p1 else 0 end) DatabaseName
		, f.serverid
	from base f

	fetch next from t_cursor into @cmd , @serverid
end
close t_cursor
deallocate t_cursor

update b set databaseid = b.databaseid
from BackupFiles b
join databases d on b.serverid=d.serverid and b.DatabaseName = d.databasename
where b.databaseid is null

GO
/****** Object:  StoredProcedure [dbo].[spLoadClusterNodes]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



CREATE proc [dbo].[spLoadClusterNodes] @serverid int = 0
as
begin
if @serverid = 0
	delete from [ClusterNodes]

declare @sql nvarchar(max), @SERVER VARCHAR(100), @Build varchar(20)
DECLARE T_CURSOR CURSOR FAST_FORWARD FOR
	SELECT SERVERNAME, serverid, Build FROM vwServersVms s
	where isActive=1 
	and (	s.ServerId = @serverid
			or @serverid =0
		)
OPEN T_CURSOR
FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid , @Build
WHILE @@FETCH_STATUS=0
BEGIN
	if @Build >='10' 
		set @sql='SELECT  distinct serverid = '+cast(@serverid as varchar)+', NodeName, status, status_description, is_current_owner from master.sys.dm_os_cluster_nodes'
	else
		set @sql='SELECT  distinct serverid = '+cast(@serverid as varchar)+', NodeName, null status, null status_description, null is_current_owner from master.sys.dm_os_cluster_nodes'

	SET @SQL = 'SELECT a.*	FROM OPENQUERY(['+@SERVER+'], 
		'''+replace(@sql, '''', '''''')+'''
		) AS a;'
	begin try
		insert into [ClusterNodes] (ServerId, NodeName, status, status_description, is_current_owner)
		exec dbo.spExec @sql
	end try
	begin catch
		print error_message()
		print @sql
	end catch
	FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid, @Build
END
CLOSE T_CURSOR
DEALLOCATE T_CURSOR
--select * from jobs
end
GO
/****** Object:  StoredProcedure [dbo].[spLoadDataBaseFiles]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE proc [dbo].[spLoadDataBaseFiles] @serverid varchar(10)='0'
as

declare @linkedserver varchar(255)

/***********************
	DataBaseFiles
************************/
if @serverid = '0' 
	truncate table DatabaseFiles

declare @sql nvarchar(max), @SERVER VARCHAR(100), @DatabaseName varchar(100), @DatabaseId varchar(10), @version varchar(255), @EngineEdition varchar(50)

DECLARE T_CURSOR CURSOR FAST_FORWARD FOR
	SELECT SERVERNAME, serverid, version, EngineEdition FROM SERVERS s
	where isActive=1 
	and (	s.ServerId = @serverid
			or (@serverid =0 and not exists(select * from DatabaseFiles d where s.ServerId =d.serverid))
		)
		order by serverid
OPEN T_CURSOR
FETCH NEXT FROM T_CURSOR INTO @server, @serverid, @version, @EngineEdition
WHILE @@FETCH_STATUS=0
BEGIN
	--print @server
	declare d_cursor cursor fast_forward for
		select  databaseid, databasename 
		from vwDatabasesOnLine 
		where serverid=@serverid 
		
	open d_cursor
	FETCH NEXT FROM d_CURSOR INTO @databaseid, @databasename
	while @@FETCH_STATUS=0
	begin
		--print @databasename
		set @sql='
			SELECT f.name AS [File Name]
				  , f.physical_name AS [Physical Name]
				  , CAST(( f.size / 128.0 ) AS DECIMAL(10, 2)) AS [Total Size in MB]
				  , CAST(f.size / 128.0 - CAST(FILEPROPERTY(f.name, ''SpaceUsed'') AS INT) / 128.0 AS DECIMAL(10, 2)) AS [Available Space In MB]
				  , [file_id]
				  , fg.name AS [Filegroup Name]
			FROM    ['+@DatabaseName+'].sys.database_files AS f WITH ( NOLOCK )
					LEFT OUTER JOIN ['+@DatabaseName+'].sys.data_spaces AS fg WITH ( NOLOCK ) ON f.data_space_id = fg.data_space_id
			'
		if @DatabaseName = 'master' or @EngineEdition <> 'SQL Azure'
			set @linkedserver = @server
		else 
			set @linkedserver = @server+'.'+@DatabaseName

		SET @SQL = 'SELECT '+@serverid+', '+@databaseid+', a.*
			FROM OPENQUERY(['+@linkedserver+'], 
			'''+replace(@sql, '''', '''''')+'''
			) AS a
			;'
		begin try
			--print @sql
			insert into DatabaseFiles (Serverid,DatabaseId,FileName,PhysicalName,TotalMbs,AvailableMbs,fileid,filegroupname)
			exec (@sql)
			--exec spExec @sql=@SQL, @debug=@debug, @exec=@exec, @raiserror= @debug
		end try
		begin catch
			print @sql
			print error_message()
		end catch
		FETCH NEXT FROM d_CURSOR INTO @databaseid, @databasename
	end
	close d_cursor
	deallocate d_cursor

	FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid, @version, @EngineEdition

END
CLOSE T_CURSOR
DEALLOCATE T_CURSOR
GO
/****** Object:  StoredProcedure [dbo].[spLoadDataBaseObjectColums]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




CREATE proc [dbo].[spLoadDataBaseObjectColums] @serverid varchar(10)='0'
as

if @serverid = '0' 
	truncate table [DatabaseObjectColumns]

declare @sql nvarchar(max), @SERVER VARCHAR(100), @DatabaseName varchar(100), @DatabaseId varchar(10), @version varchar(255), @linkedserver varchar(255), @EngineEdition varchar(50)

DECLARE T_CURSOR CURSOR FAST_FORWARD FOR
	SELECT  SERVERNAME, serverid, version, EngineEdition from servers s
	where isActive=1 
	and (	s.ServerId = @serverid
			or (@serverid =0 /*and not exists(select * from DatabaseObjectColumns d where s.ServerId =d.serverid)*/)
		)
OPEN T_CURSOR
FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid, @version, @EngineEdition
WHILE @@FETCH_STATUS=0
BEGIN
	declare d_cursor cursor fast_forward for
		select databaseid, databasename 
		from vwDatabasesOnLineNonSystem d 
		where serverid=@serverid 
	 
	open d_cursor
	FETCH NEXT FROM d_CURSOR INTO @databaseid, @databasename
	while @@FETCH_STATUS=0
	begin
		if @SERVER = @@SERVERNAME
			set @SERVER = '.' 
		set @sql='
		select '''+@DatabaseName+''' TABLE_CATALOG
			, s.name TABLE_SCHEMA
			, t.name TABLE_NAME
			, c.name COLUMN_NAME
			, c.column_id ORDINAL_POSITION
			, df.definition COLUMN_DEFAULT
			, c.is_nullable IS_NULLABLE
			, case when ty.name in (''nvarchar'',''nchar'', ''varchar'', ''char'', ''varbinary'') and c.max_length = -1 then  ty.name + '' (max)''
					when ty.name in (''nvarchar'',''nchar'') then ty.name + '' (''+ cast(c.max_length / 2 as varchar) +'')''
					when ty.name in (''varchar'',''char'', ''varbinary'') then ty.name + '' (''+ cast(c.max_length as varchar) +'')''
					when ty.name in (''numeric'', ''decimal'') then ty.name + '' (''+ cast(c.precision as varchar)+ '',''+ cast(c.scale as varchar) +'')''
					when ty.name in (''timestamp'',''rowversion'') then ''varbinary(8)''
					else ty.name end Data_Type 
			, c.max_length CHARACTER_MAXIMUM_LENGTH
			, c.collation_name COLLATION_NAME
			, c.is_computed, c.is_identity
			, m.MASKING_FUNCTION
		FROM ['+@DatabaseName+'].sys.tables t with (nolock)
		INNER JOIN ['+@DatabaseName+'].sys.schemas s on s.schema_id = t.schema_id
		inner join ['+@DatabaseName+'].sys.columns c on c.object_id = t.object_id 
		inner join ['+@DatabaseName+'].sys.types ty on c.user_type_id = ty.user_type_id
		left outer join ['+@DatabaseName+'].sys.default_constraints df on df.object_id = c.default_object_id
		left outer join ['+@DatabaseName+'].sys.masked_columns m on m.[object_id] = t.[object_id] and m.column_id = c.column_id
		where t.type=''U''
	   '
	
		if @DatabaseName = 'master' or @EngineEdition <> 'SQL Azure' 
			set @linkedserver = @server
		else 
			set @linkedserver = @server+'.'+@DatabaseName

		SET @SQL = 'SELECT '+@serverid+' as serverid, '+@databaseid+' as databaseid, do.databaseObjectId, a.TABLE_CATALOG, a.TABLE_SCHEMA, a.TABLE_NAME, a.COLUMN_NAME, a.ORDINAL_POSITION, a.COLUMN_DEFAULT, a.IS_NULLABLE, a.DATA_TYPE, a.CHARACTER_MAXIMUM_LENGTH, a.COLLATION_NAME, is_computed, is_identity, MASKING_FUNCTION
			FROM OPENQUERY(['+@linkedserver+'], 
			'''+replace(@sql, '''', '''''')+'''
			) AS a
			JOIN DatabaseObjects do ON do.ObjectName = a.TABLE_NAME collate SQL_Latin1_General_CP1_CI_AS AND do.ServerId = '+@serverid+' and do.DatabaseId = '+@databaseid+' and do.SchemaName = a.TABLE_SCHEMA collate SQL_Latin1_General_CP1_CI_AS
			;'
		begin try
			insert into [DatabaseObjectColumns] (ServerId,DatabaseId,DatabaseObjectId, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME,ORDINAL_POSITION,COLUMN_DEFAULT,IS_NULLABLE,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,COLLATION_NAME, is_computed, is_identity, MASKING_FUNCTION)
			exec dbo.spExec @sql
		end try
		begin catch
				exec [dbo].[spPrintLongSql] @sql
				print error_message()
		end catch
		FETCH NEXT FROM d_CURSOR INTO @databaseid, @databasename
	end
	close d_cursor
	deallocate d_cursor
	FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid, @version, @EngineEdition
END
CLOSE T_CURSOR
DEALLOCATE T_CURSOR


GO
/****** Object:  StoredProcedure [dbo].[spLoadDataBaseObjectPerms]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE proc [dbo].[spLoadDataBaseObjectPerms] @serverid varchar(10)='0'
as

/***********************
	DataBaseObjectPerms
************************/
if @serverid = 0
	delete from DatabaseObjectPerms 
else 
	delete from DatabaseObjectPerms where ServerId=@serverid

declare @sql nvarchar(max), @SERVER VARCHAR(100), @DatabaseName varchar(100), @DatabaseId varchar(10), @version varchar(255), @linkedserver varchar(255), @EngineEdition varchar(50)

DECLARE T_CURSOR CURSOR FAST_FORWARD FOR
	SELECT SERVERNAME, serverid, version, EngineEdition FROM SERVERS s
	where isActive=1 
	and (	s.ServerId = @serverid
			or (@serverid =0 and not exists(select * from DatabaseObjectPerms d where s.ServerId =d.serverid))
		)
OPEN T_CURSOR
FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid , @version, @EngineEdition
WHILE @@FETCH_STATUS=0
BEGIN
	declare d_cursor cursor fast_forward for
		select databaseid, databasename 
		from vwDatabasesOnLineNonSystem 
		where serverid=@serverid 
	open d_cursor
	FETCH NEXT FROM d_CURSOR INTO @databaseid, @databasename
	while @@FETCH_STATUS=0
	begin
		set @sql='
		select  princ.name as UserName
		,       princ.type_desc
		,       perm.permission_name
		,       perm.state_desc
		,       perm.class_desc
		,      COALESCE(object_name(perm.major_id), SCHEMA_NAME(perm.major_id), DB_NAME() )
		from ['+@DatabaseName+'].sys.database_principals princ
		join ['+@DatabaseName+'].sys.database_permissions perm on perm.grantee_principal_id = princ.principal_id
	   '
		if @DatabaseName = 'master' or @EngineEdition <> 'SQL Azure' 
			set @linkedserver = @server
		else 
			set @linkedserver = @server+'.'+@DatabaseName
			
		SET @SQL = 'SELECT '+@serverid+', '+@databaseid+', l.LoginId, a.*
			FROM OPENQUERY(['+@linkedserver+'], 
			'''+replace(@sql, '''', '''''')+'''
			) AS a
			LEFT OUTER JOIN Logins l ON l.LoginName collate SQL_Latin1_General_CP1_CI_AS = a.UserName collate SQL_Latin1_General_CP1_CI_AS AND l.ServerId = '+@serverid+'
			;'
		begin try
			insert into DataBaseObjectPerms (Serverid,DatabaseId,LoginId,USERNAME,type_desc,perm_name,state_desc,class_desc,ObjectName)
			exec dbo.spExec @sql = @sql, @debug=0, @exec=1, @raiserror=1
		end try
		begin catch
			print error_message()
			print @sql
		end catch
		FETCH NEXT FROM d_CURSOR INTO @databaseid, @databasename
	end
	close d_cursor
	deallocate d_cursor
	FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid, @version, @EngineEdition
END
CLOSE T_CURSOR
DEALLOCATE T_CURSOR
GO
/****** Object:  StoredProcedure [dbo].[spLoadDataBaseObjects]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE proc [dbo].[spLoadDataBaseObjects] @debug bit=0, @exec bit=1, @serverid varchar(10)='0'
as
begin
set nocount on
/***********************
	DataBaseObjects
************************/
/*
delete from [dbo].[DatabaseObjectPerms]
delete from IndexUsage
delete from [dbo].[DatabaseObjectColumns]
delete from [dbo].[DatabaseObjects]
*/

declare @sql nvarchar(max), @SERVER VARCHAR(100), @DatabaseName varchar(100), @DatabaseId varchar(10), @Version	varchar	(255), @linkedserver varchar(255), @EngineEdition varchar(50)
	, @edition varchar(100)

DECLARE T_CURSOR CURSOR FAST_FORWARD FOR
	SELECT  SERVERNAME, serverid, Version, EngineEdition from Servers s
	where isActive=1 
	--and (	s.ServerId = @serverid
	--		or (@serverid ='0' and not exists(select * from DatabaseObjects d where s.ServerId =d.serverid))
	--	)
OPEN T_CURSOR
FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid , @Version, @EngineEdition
WHILE @@FETCH_STATUS=0
BEGIN
	--print @SERVER
	if @SERVER = @@SERVERNAME
		set @SERVER = '.' 
	declare d_cursor cursor fast_forward for
		select databaseid, databasename, edition 
		from vwDatabasesOnLineNonSystem 
		where serverid=@serverid
		order by 2
	open d_cursor
	FETCH NEXT FROM d_CURSOR INTO @databaseid, @databasename, @edition 
	while @@FETCH_STATUS=0
	begin 
		--print @databasename
		if isnull(@edition,'') = 'DataWarehouse'
		set @sql='
				select o.name, s.name sch, xtype, coalesce(/*ps.rows,*/ r.rows, 0)
					, RowLength, ColCount, ReplColumns, hasCr_Dt
					, case when o.xtype= ''v'' then ''READS'' else rt.SQL_DATA_ACCESS end as SQL_DATA_ACCESS
					, coalesce(rt.ROUTINE_DEFINITION, rv.VIEW_DEFINITION, ck.definition, fk.delete_referential_action_desc collate SQL_Latin1_General_CP1_CI_AS) ROUTINE_DEFINITION
					, isnull(st.is_published,0) is_mspublished			
					, c.is_rplpublished
					, c.is_rplsubscribed
					, coalesce (ck.is_disabled, fk.is_disabled) is_disabled
					, o.parent_obj
					, null start_value, null  current_value
					, parentSchema, parentTable, parentColumn
					, o.crdate
				from ['+@DatabaseName+'].sys.sysobjects o WITH ( NOLOCK )
				left join ['+@DatabaseName+'].sys.tables st on st.object_id = o.id
		
				join ['+@DatabaseName+'].sys.schemas s WITH ( NOLOCK ) on o.uid=s.schema_id
				outer apply 
				(SELECT  SUM(Rows) AS [rows]
					FROM ['+@DatabaseName+'].sys.partitions p WITH ( NOLOCK )
					WHERE p.index_id < 2 and p.object_id = o.id	
					and o.xtype=''u''
				) r
				--outer apply /*does not work for round robin tables :-( */
				--(select sum(nps.[row_count]) rows
				--	from  ['+@DatabaseName+'].sys.pdw_table_mappings tm 
				--	INNER JOIN ['+@DatabaseName+'].sys.pdw_nodes_tables nt  ON tm.[physical_name] = nt.[name] 
				--	INNER JOIN ['+@DatabaseName+'].sys.dm_pdw_nodes_db_partition_stats nps ON nt.[object_id] = nps.[object_id]    AND nt.[pdw_node_id] = nps.[pdw_node_id]    AND nt.[distribution_id] = nps.[distribution_id]
				--	where nps.index_id = 1
				--	and tm.[object_id] = o.id
				--) ps
				outer apply 
				(SELECT  SUM(max_length) AS RowLength
						, count(*) as [ColCount]
						, sum(case when Is_Replicated=1 then 1 else 0 end) ReplColumns
						, sum(case when c.name = ''DateCreated'' then 1 else 0 end) hasCr_Dt 
						, sum(case when c.name = ''rv'' then 1 else 0 end) is_rplpublished 
						, sum(case when c.name = ''sourcerv'' then 1 else 0 end) is_rplsubscribed
					FROM ['+@DatabaseName+'].sys.columns c WITH ( NOLOCK )
					WHERE c.object_id = o.id 
					and o.xtype in (''U'',''V'')
				) c
				outer apply (
					select SQL_DATA_ACCESS, ROUTINE_DEFINITION
					from ['+@DatabaseName+'].INFORMATION_SCHEMA.ROUTINES rt WITH ( NOLOCK )
					where rt.SPECIFIC_SCHEMA = s.name
					and rt.ROUTINE_NAME=o.name
					and o.xtype in (''P'', ''FN'', ''IF'', ''TF'')
				) rt
				outer apply (
					select VIEW_DEFINITION
					from ['+@DatabaseName+'].INFORMATION_SCHEMA.VIEWS rv WITH ( NOLOCK )
					where rv.TABLE_SCHEMA = s.name
					and rv.TABLE_NAME=o.name
					and o.xtype = ''V''
				) rv
				outer apply (
					select is_disabled, definition from ['+@DatabaseName+'].sys.check_constraints ck
					where ck.name = o.name
					and o.xtype=''C''
				) ck
				outer apply (
					select is_disabled, delete_referential_action_desc from ['+@DatabaseName+'].sys.foreign_keys fk
					where fk.name = o.name
					and o.xtype=''F''
				) fk
				outer apply (
					select top 1 st.name parentSchema, t.name parentTable, ct.name as parentColumn
						from ['+@DatabaseName+'].sys.tables t
						join ['+@DatabaseName+'].sys.schemas st on st.schema_id=t.schema_id
						outer apply (select top 1 name 
								from ['+@DatabaseName+'].sys.columns ct 
								where  ct.object_id = t.object_id 
								and ct.system_type_id in (52,56,127)--int variations
								and ct.name not like ''%OLD''
								) ct
						where t.name = replace(o.name,''_seq'','''')
						and o.xtype=''SO''
					) p
				where o.xtype in (''U'', ''V'', ''P'', ''FN'', ''IF'', ''TF'', ''C'', ''F'')
				and o.NAME not like ''syncobj%''
			   '
		ELSE if @version like 'Microsoft SQL Server 2008%' or @EngineEdition = 'SQL Azure' 
		set @sql='
				select o.name, s.name sch, xtype, [RowCount]
					, RowLength, ColCount, ReplColumns, hasCr_Dt
					, case when o.xtype= ''v'' then ''READS'' else rt.SQL_DATA_ACCESS end as SQL_DATA_ACCESS
					, coalesce(rt.ROUTINE_DEFINITION, rv.VIEW_DEFINITION, ck.definition, fk.delete_referential_action_desc collate SQL_Latin1_General_CP1_CI_AS) ROUTINE_DEFINITION
					, isnull(st.is_published,0) is_mspublished			
					, c.is_rplpublished
					, c.is_rplsubscribed
					, coalesce (ck.is_disabled, fk.is_disabled) is_disabled
					, o.parent_obj
					, null start_value, null  current_value
					, parentSchema, parentTable, parentColumn
					, o.crdate
				from ['+@DatabaseName+'].sys.sysobjects o WITH ( NOLOCK )
				left join ['+@DatabaseName+'].sys.tables st on st.object_id = o.id
		
				join ['+@DatabaseName+'].sys.schemas s WITH ( NOLOCK ) on o.uid=s.schema_id
				outer apply 
				(SELECT  SUM(Rows) AS [RowCount]
					FROM ['+@DatabaseName+'].sys.partitions p WITH ( NOLOCK )
					WHERE p.index_id < 2 and p.object_id = o.id	
					and o.xtype=''u''
				) r
				outer apply 
				(SELECT  SUM(max_length) AS RowLength
						, count(*) as [ColCount]
						, sum(case when Is_Replicated=1 then 1 else 0 end) ReplColumns
						, sum(case when c.name = ''DateCreated'' then 1 else 0 end) hasCr_Dt 
						, sum(case when c.name = ''rv'' then 1 else 0 end) is_rplpublished 
						, sum(case when c.name = ''sourcerv'' then 1 else 0 end) is_rplsubscribed
					FROM ['+@DatabaseName+'].sys.columns c WITH ( NOLOCK )
					WHERE c.object_id = o.id 
					and o.xtype in (''U'',''V'')
				) c
				outer apply (
					select SQL_DATA_ACCESS, ROUTINE_DEFINITION
					from ['+@DatabaseName+'].INFORMATION_SCHEMA.ROUTINES rt WITH ( NOLOCK )
					where rt.SPECIFIC_SCHEMA = s.name
					and rt.ROUTINE_NAME=o.name
					and o.xtype in (''P'', ''FN'', ''IF'', ''TF'')
				) rt
				outer apply (
					select VIEW_DEFINITION
					from ['+@DatabaseName+'].INFORMATION_SCHEMA.VIEWS rv WITH ( NOLOCK )
					where rv.TABLE_SCHEMA = s.name
					and rv.TABLE_NAME=o.name
					and o.xtype = ''V''
				) rv
				outer apply (
					select is_disabled, definition from ['+@DatabaseName+'].sys.check_constraints ck
					where ck.name = o.name
					and o.xtype=''C''
				) ck
				outer apply (
					select is_disabled, delete_referential_action_desc from ['+@DatabaseName+'].sys.foreign_keys fk
					where fk.name = o.name
					and o.xtype=''F''
				) fk
				outer apply (
					select top 1 st.name parentSchema, t.name parentTable, ct.name as parentColumn
						from ['+@DatabaseName+'].sys.tables t
						join ['+@DatabaseName+'].sys.schemas st on st.schema_id=t.schema_id
						outer apply (select top 1 name 
								from ['+@DatabaseName+'].sys.columns ct 
								where  ct.object_id = t.object_id 
								and ct.system_type_id in (52,56,127)--int variations
								and ct.name not like ''%OLD''
								) ct
						where t.name = replace(o.name,''_seq'','''')
						and o.xtype=''SO''
					) p
				where o.xtype in (''U'', ''V'', ''P'', ''FN'', ''IF'', ''TF'', ''C'', ''F'')
				and o.NAME not like ''syncobj%''
			   '
		ELSE 
		set @sql='
		select o.name, s.name sch, xtype, [RowCount]
			, RowLength, ColCount, ReplColumns, hasCr_Dt
			, case when o.xtype= ''v'' then ''READS'' else rt.SQL_DATA_ACCESS end as SQL_DATA_ACCESS
			, coalesce(rt.ROUTINE_DEFINITION, rv.VIEW_DEFINITION, ck.definition, fk.delete_referential_action_desc collate SQL_Latin1_General_CP1_CI_AS) ROUTINE_DEFINITION
			, isnull(st.is_published,0) is_mspublished			
			, c.is_rplpublished
			, c.is_rplsubscribed
			, coalesce (ck.is_disabled, fk.is_disabled) is_disabled
			, o.parent_obj
			, so.start_value, so.current_value
			, parentSchema, parentTable, parentColumn
			, o.crdate
		from ['+@DatabaseName+'].sys.sysobjects o WITH ( NOLOCK )
		left join ['+@DatabaseName+'].sys.tables st on st.object_id = o.id
		
		join ['+@DatabaseName+'].sys.schemas s WITH ( NOLOCK ) on o.uid=s.schema_id
		outer apply 
		(SELECT  SUM(Rows) AS [RowCount]
			FROM ['+@DatabaseName+'].sys.partitions p WITH ( NOLOCK )
			WHERE p.index_id < 2 and p.object_id = o.id	
			and o.xtype=''u''
		) r
		outer apply 
		(SELECT  SUM(max_length) AS RowLength
				, count(*) as [ColCount]
				, sum(case when Is_Replicated=1 then 1 else 0 end) ReplColumns
				, sum(case when c.name = ''DateCreated'' then 1 else 0 end) hasCr_Dt 
				, sum(case when c.name = ''rv'' then 1 else 0 end) is_rplpublished 
				, sum(case when c.name = ''sourcerv'' then 1 else 0 end) is_rplsubscribed
			FROM ['+@DatabaseName+'].sys.columns c WITH ( NOLOCK )
			WHERE c.object_id = o.id 
			and o.xtype in (''U'',''V'')
		) c
		outer apply (
			select SQL_DATA_ACCESS, ROUTINE_DEFINITION
			from ['+@DatabaseName+'].INFORMATION_SCHEMA.ROUTINES rt WITH ( NOLOCK )
			where rt.SPECIFIC_SCHEMA = s.name
			and rt.ROUTINE_NAME=o.name
			and o.xtype in (''P'', ''FN'', ''IF'', ''TF'')
		) rt
		outer apply (
			select VIEW_DEFINITION
			from ['+@DatabaseName+'].INFORMATION_SCHEMA.VIEWS rv WITH ( NOLOCK )
			where rv.TABLE_SCHEMA = s.name
			and rv.TABLE_NAME=o.name
			and o.xtype = ''V''
		) rv
		outer apply (
			select is_disabled, definition from ['+@DatabaseName+'].sys.check_constraints ck
			where ck.name = o.name
			and o.xtype=''C''
		) ck
		outer apply (
			select is_disabled, delete_referential_action_desc from ['+@DatabaseName+'].sys.foreign_keys fk
			where fk.name = o.name
			and o.xtype=''F''
		) fk
		outer apply (
			select cast(s.current_value as bigint) current_value
				, cast(start_value as bigint)  start_value
			from ['+@DatabaseName+'].sys.sequences s
			where s.object_id = o.id
			and o.xtype=''SO''
			) so
		outer apply (
			select top 1 st.name parentSchema, t.name parentTable, ct.name as parentColumn
				from ['+@DatabaseName+'].sys.tables t
				join ['+@DatabaseName+'].sys.schemas st on st.schema_id=t.schema_id
				outer apply (select top 1 name 
						from ['+@DatabaseName+'].sys.columns ct 
						where  ct.object_id = t.object_id 
						and ct.system_type_id in (52,56,127)--int variations
						and ct.name not like ''%OLD''
						) ct
				where t.name = replace(o.name,''_seq'','''')
				and o.xtype=''SO''
			) p
		where o.xtype in (''U'', ''V'', ''P'', ''FN'', ''IF'', ''TF'', ''C'', ''F'', ''SO'')
		and o.NAME not like ''syncobj%''
	   '
		if @DatabaseName = 'master' or @EngineEdition <> 'SQL Azure' 
			set @linkedserver = @server
		else 
			set @linkedserver = @server+'.'+@DatabaseName

		SET @SQL = 'SELECT '+@serverid+', '+@databaseid+', a.*
			FROM OPENQUERY(['+@linkedserver+'], 
			'''+replace(@sql, '''', '''''')+'''
			) AS a
			;'
		begin try
			insert into DatabaseObjects (Serverid,DatabaseId,ObjectName,SchemaName,Xtype,[RowCount], [RowLength], [ColCount], [ReplColumns], hasCr_Dt, SQL_DATA_ACCESS, ROUTINE_DEFINITION, is_mspublished, is_rplpublished, is_rplsubscribed, is_disabled, parent_object_id, start_value, current_value, parentSchema, parentTable, parentColumn, crdate)
			exec (@sql)
			--exec spExec @sql=@SQL, @debug=@debug, @exec=@exec, @raiserror= @debug
		end try
		begin catch
			print @sql
			print error_message()
		end catch
		FETCH NEXT FROM d_CURSOR INTO @databaseid, @databasename, @edition
	end
	close d_cursor
	deallocate d_cursor

	FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid, @Version, @EngineEdition
END
CLOSE T_CURSOR
DEALLOCATE T_CURSOR
end

GO
/****** Object:  StoredProcedure [dbo].[spLoadDataBasePerms]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE proc [dbo].[spLoadDataBasePerms] @serverid varchar(10)='0'
as

/***********************
	DataBasePerms
************************/
if @serverid = '0' 
	truncate table DataBasePerms

declare @sql nvarchar(max), @SERVER VARCHAR(100), @DatabaseName varchar(100), @DatabaseId varchar(10), @version varchar(255), @linkedserver varchar(255), @EngineEdition varchar(50)
DECLARE T_CURSOR CURSOR FAST_FORWARD FOR
	SELECT SERVERNAME, serverid, version, EngineEdition FROM SERVERS s
	where isActive=1 
	and (	s.ServerId = @serverid
			or (@serverid =0 and not exists(select * from DatabasePerms d where s.ServerId =d.serverid))
		)
OPEN T_CURSOR
FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid , @version , @EngineEdition
WHILE @@FETCH_STATUS=0
BEGIN
	declare d_cursor cursor fast_forward for
		select databaseid, databasename 
		from vwDatabasesOnLineNonSystem
		where  serverid=@serverid 
	open d_cursor
	FETCH NEXT FROM d_CURSOR INTO @databaseid, @databasename
	while @@FETCH_STATUS=0
	begin
		set @sql='
		SELECT USERNAME,
		   MAX(CASE ROLENAME WHEN ''DB_OWNER''         THEN 1 ELSE 0 END) AS DB_OWNER,
		   MAX(CASE ROLENAME WHEN ''DB_ACCESSADMIN ''   THEN 1 ELSE 0 END) AS DB_ACCESSADMIN ,
		   MAX(CASE ROLENAME WHEN ''DB_SECURITYADMIN''  THEN 1 ELSE 0 END) AS DB_SECURITYADMIN,
		   MAX(CASE ROLENAME WHEN ''DB_DDLADMIN''   THEN 1 ELSE 0 END) AS DB_DDLADMIN,
		   MAX(CASE ROLENAME WHEN ''DB_DATAREADER''        THEN 1 ELSE 0 END) AS DB_DATAREADER,
		   MAX(CASE ROLENAME WHEN ''DB_DATAWRITER''        THEN 1 ELSE 0 END) AS DB_DATAWRITER,
		   MAX(CASE ROLENAME WHEN ''DB_DENYDATAREADER'' THEN 1 ELSE 0 END) AS DB_DENYDATAREADER,
		   MAX(CASE ROLENAME WHEN ''DB_DENYDATAWRITER'' THEN 1 ELSE 0 END) AS DB_DENYDATAWRITER,
		  CREATEDATE,
		  UPDATEDATE 
		   FROM (SELECT B.NAME AS USERNAME, C.NAME AS ROLENAME, B.CREATEDATE, B.UPDATEDATE
				FROM ['+@DatabaseName+']..sysmembers A   
				JOIN ['+@DatabaseName+']..sysusers  B  ON A.MEMBERUID = B.UID
				JOIN ['+@DatabaseName+']..sysusers C ON A.GROUPUID = C.UID 
				 )S   
				   GROUP BY USERNAME, CREATEDATE, UPDATEDATE
			 ORDER BY USERNAME
			   '
		if @DatabaseName = 'master' or @EngineEdition <> 'SQL Azure' 
			set @linkedserver = @server
		else 
			set @linkedserver = @server+'.'+@DatabaseName
	
	SET @SQL = 'SELECT '+@serverid+', '+@databaseid+', l.LoginId, a.*
			FROM OPENQUERY(['+@linkedserver+'], 
			'''+replace(@sql, '''', '''''')+'''
			) AS a
			LEFT OUTER JOIN Logins l ON l.LoginName collate SQL_Latin1_General_CP1_CI_AS = a.UserName collate SQL_Latin1_General_CP1_CI_AS AND l.ServerId = '+@serverid+'
			;'
		begin try
			insert into DatabasePerms (Serverid,DatabaseId,LoginId,USERNAME,DB_OWNER,DB_ACCESSADMIN,DB_SECURITYADMIN,DB_DDLADMIN,DB_DATAREADER,DB_DATAWRITER,DB_DENYDATAREADER,DB_DENYDATAWRITER,CREATEDATE,UPDATEDATE)
			exec dbo.spExec @sql
		end try
		begin catch
			print error_message()
			print @sql
		end catch
		FETCH NEXT FROM d_CURSOR INTO @databaseid, @databasename
	end
	close d_cursor
	deallocate d_cursor
	FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid, @version , @EngineEdition
END
CLOSE T_CURSOR
DEALLOCATE T_CURSOR
GO
/****** Object:  StoredProcedure [dbo].[spLoadDatabases]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE proc [dbo].[spLoadDatabases] @serverid int=0 
as
begin
/**************
	DATABASES
**************/
if @serverid = '0'
	delete from databases
else 
	delete from databases where serverid=@serverid

declare @sql nvarchar(max)
	, @SERVER VARCHAR(100)--, @serverid int
	, @Version	varchar	(255)
	, @EngineEdition varchar(50)

DECLARE T_CURSOR CURSOR FAST_FORWARD FOR
	SELECT SERVERNAME, serverid, Version, EngineEdition
	FROM SERVERS s
	where isActive=1 
	and (	s.ServerId = @serverid
			or (@serverid =0 and not exists(select * from Databases d where s.ServerId =d.serverid))
		)
OPEN T_CURSOR
FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid, @Version, @EngineEdition
WHILE @@FETCH_STATUS=0
BEGIN
	if @SERVER = @@SERVERNAME
		set @SERVER = '.' 
	if @version like 'Microsoft SQL Server 2000%' 
		set @sql='SELECT  serverid = '+cast(@serverid as varchar)+'
				  , db.[name] 
				  , null recovery_model_desc
				  , null [Log Size (KB)]
				  , null [Log Used (KB)]
				  , null [Log Used %]
				  , null [DB Compatibility Level]
				  , null [Page Verify Option]
				  , null is_auto_create_stats_on
				  , null is_auto_update_stats_on
				  , null is_auto_update_stats_async_on
				  , null is_parameterization_forced
				  , null snapshot_isolation_state_desc
				  , null is_read_committed_snapshot_on
				  , null is_auto_close_on
				  , null is_auto_shrink_on
				  , null --target_recovery_time_in_seconds
				  , null Data
				  , null [Log] 
				  , null State_Desc
				  , null Create_Date
				  , null is_published
				  , null is_subscribed
				  , null Collation_name
				  , null [CachedSizeMBs]
				  , null AS [CPU_Time_Ms]
				  , null Is_Read_Only
				  , null delayed_durability_desc, null containment_desc, null is_cdc_enabled, null is_broker_enabled, null is_memory_optimized_elevate_to_snapshot_on	
				  , null AvailabilityGroup, null PrimaryReplicaServerName, null LocalReplicaRole, null SynchronizationState, null IsSuspended, null IsJoined
				  , null SourceDatabaseName, null owner
				  , null mirroring_state_desc, null mirroring_role_desc, null mirroring_safety_level_desc, null mirroring_partner_name, null mirroring_partner_instance
				  , null mirroring_witness_name, null mirroring_witness_state_desc, null mirroring_connection_timeout, null mirroring_redo_queue 
				  , null is_encrypted 	
				  , null edition, null service_objective, null elastic_pool_name, null is_query_store_on
				 --select *
			FROM    master..sysdatabases AS db
			   '	
	else if @version like 'Microsoft SQL Server 2005%' 
		set @sql='SELECT  serverid = '+cast(@serverid as varchar)+'
				  , db.[name] AS [DatabaseName]
				  , db.recovery_model_desc AS [Recovery Model]
				  , ls.cntr_value AS [Log Size (KB)]
				  , lu.cntr_value AS [Log Used (KB)]
				  , CAST(CAST(lu.cntr_value AS FLOAT) / CAST(ls.cntr_value AS FLOAT) AS DECIMAL(18, 2)) * 100 AS [Log Used %]
				  , db.[compatibility_level] AS [DB Compatibility Level]
				  , db.page_verify_option_desc AS [Page Verify Option]
				  , db.is_auto_create_stats_on
				  , db.is_auto_update_stats_on
				  , db.is_auto_update_stats_async_on
				  , db.is_parameterization_forced
				  , db.snapshot_isolation_state_desc
				  , db.is_read_committed_snapshot_on
				  , db.is_auto_close_on
				  , db.is_auto_shrink_on
				  , -1 --target_recovery_time_in_seconds
				  , (SELECT  sum(CONVERT(BIGINT, size / 128.0)) AS [Total Size in MB]
					FROM    sys.master_files f WITH ( NOLOCK )
					WHERE   f.[database_id] = db.database_id
					and type_desc = ''Rows''
					) Data
				 , (SELECT  sum(CONVERT(BIGINT, size / 128.0)) AS [Total Size in MB]
					FROM    sys.master_files f WITH ( NOLOCK )
					WHERE   f.[database_id] = db.database_id
					and type_desc = ''Log''
					) [Log] 
				  , db.State_Desc
				  , db.Create_Date
				  , db.is_published
				  , db.is_subscribed
				  , db.Collation_name
				  , (SELECT COUNT(*) * 8 / 1024 
					FROM    sys.dm_os_buffer_descriptors d WITH ( NOLOCK )
					WHERE  d.database_id = -1--db.database_id
					) [CachedSizeMBs]
				  , ( SELECT    SUM(total_worker_time) 
					FROM     sys.dm_exec_query_stats AS qs
					CROSS APPLY ( SELECT    CONVERT(INT, value) AS [DatabaseID]
								  FROM      sys.dm_exec_plan_attributes(qs.plan_handle)
								  WHERE     attribute = N''dbid''
								   ) AS F_DB
					   where F_DB.[DatabaseID] = -1--db.Database_ID
					 ) AS [CPU_Time_Ms]
					 , db.Is_Read_Only
					 , null delayed_durability_desc, null containment_desc, null is_cdc_enabled, db.is_broker_enabled, null is_memory_optimized_elevate_to_snapshot_on	
					 , null AvailabilityGroup, null PrimaryReplicaServerName, null LocalReplicaRole, null SynchronizationState, null IsSuspended, null IsJoined
					 , ss.name SourceDatabaseName, l.loginname owner
					 , m.mirroring_state_desc, m.mirroring_role_desc, m.mirroring_safety_level_desc, m.mirroring_partner_name, m.mirroring_partner_instance
					 , m.mirroring_witness_name, m.mirroring_witness_state_desc, m.mirroring_connection_timeout, m.mirroring_redo_queue 
					 , null is_encrypted
				  , null edition, null service_objective, null elastic_pool_name, null is_query_store_on
					 --select *
			FROM    master.sys.databases AS db
					LEFT JOIN master.sys.databases ss on ss.database_id = db.source_database_id
					LEFT JOIN master.sys.dm_os_performance_counters AS lu ON db.name = lu.instance_name and lu.counter_name LIKE N''Log File(s) Used Size (KB)%''
					LEFT JOIN master.sys.dm_os_performance_counters AS ls ON db.name = ls.instance_name AND ls.counter_name LIKE N''Log File(s) Size (KB)%'' AND ls.cntr_value > 0
					left join master..syslogins l on db.owner_sid = l.sid
					left join sys.database_mirroring m ON m.database_id = db.database_id
			OPTION  ( RECOMPILE );
			   '	
		else if @version like 'Microsoft SQL Server 2008%' 
		set @sql='SELECT  serverid = '+cast(@serverid as varchar)+'
				  , db.[name] AS [DatabaseName]
				  , db.recovery_model_desc AS [Recovery Model]
				  , ls.cntr_value AS [Log Size (KB)]
				  , lu.cntr_value AS [Log Used (KB)]
				  , CAST(CAST(lu.cntr_value AS FLOAT) / CAST(ls.cntr_value AS FLOAT) AS DECIMAL(18, 2)) * 100 AS [Log Used %]
				  , db.[compatibility_level] AS [DB Compatibility Level]
				  , db.page_verify_option_desc AS [Page Verify Option]
				  , db.is_auto_create_stats_on
				  , db.is_auto_update_stats_on
				  , db.is_auto_update_stats_async_on
				  , db.is_parameterization_forced
				  , db.snapshot_isolation_state_desc
				  , db.is_read_committed_snapshot_on
				  , db.is_auto_close_on
				  , db.is_auto_shrink_on
				  , -1 --target_recovery_time_in_seconds
				  , (SELECT  sum(CONVERT(BIGINT, size / 128.0)) AS [Total Size in MB]
					FROM    sys.master_files f WITH ( NOLOCK )
					WHERE   f.[database_id] = db.database_id
					and type_desc = ''Rows''
					) Data
				 , (SELECT  sum(CONVERT(BIGINT, size / 128.0)) AS [Total Size in MB]
					FROM    sys.master_files f WITH ( NOLOCK )
					WHERE   f.[database_id] = db.database_id
					and type_desc = ''Log''
					) [Log] 
				  , db.State_Desc
				  , db.Create_Date
				  , db.is_published
				  , db.is_subscribed
				  , db.Collation_name
				  , (SELECT COUNT(*) * 8 / 1024 
					FROM    sys.dm_os_buffer_descriptors d WITH ( NOLOCK )
					WHERE  d.database_id = -1--db.database_id
					) [CachedSizeMBs]
				  , ( SELECT    SUM(total_worker_time) 
					FROM     sys.dm_exec_query_stats AS qs
					CROSS APPLY ( SELECT    CONVERT(INT, value) AS [DatabaseID]
								  FROM      sys.dm_exec_plan_attributes(qs.plan_handle)
								  WHERE     attribute = N''dbid''
								   ) AS F_DB
					   where F_DB.[DatabaseID] = -1--db.Database_ID
					 ) AS [CPU_Time_Ms]
					 , db.Is_Read_Only
					 , null delayed_durability_desc, null containment_desc, db.is_cdc_enabled, db.is_broker_enabled, null is_memory_optimized_elevate_to_snapshot_on	
					 , null AvailabilityGroup, null PrimaryReplicaServerName, null LocalReplicaRole, null SynchronizationState, null IsSuspended, null IsJoined
					 , ss.name SourceDatabaseName, l.loginname owner
					 , m.mirroring_state_desc, m.mirroring_role_desc, m.mirroring_safety_level_desc, m.mirroring_partner_name, m.mirroring_partner_instance
					 , m.mirroring_witness_name, m.mirroring_witness_state_desc, m.mirroring_connection_timeout, m.mirroring_redo_queue 
					 , db.is_encrypted
				  , null edition, null service_objective, null elastic_pool_name, null is_query_store_on
					 --select *
			FROM    master.sys.databases AS db
					LEFT JOIN master.sys.databases ss on ss.database_id = db.source_database_id
					LEFT JOIN master.sys.dm_os_performance_counters AS lu ON db.name = lu.instance_name and lu.counter_name LIKE N''Log File(s) Used Size (KB)%''
					LEFT JOIN master.sys.dm_os_performance_counters AS ls ON db.name = ls.instance_name AND ls.counter_name LIKE N''Log File(s) Size (KB)%'' AND ls.cntr_value > 0
					left join master..syslogins l on db.owner_sid = l.sid
					left join sys.database_mirroring m ON m.database_id = db.database_id
			OPTION  ( RECOMPILE );
			   '	
	else if @version like 'Microsoft SQL Server 2012%' 
		set @sql='SELECT  serverid = '+cast(@serverid as varchar)+'
				, db.[name] AS [DatabaseName]
			  , db.recovery_model_desc AS [Recovery Model]
			  , ls.cntr_value AS [Log Size (KB)]
			  , lu.cntr_value AS [Log Used (KB)]
			  , CAST(CAST(lu.cntr_value AS FLOAT) / CAST(ls.cntr_value AS FLOAT) AS DECIMAL(18, 2)) * 100 AS [Log Used %]
			  , db.[compatibility_level] AS [DB Compatibility Level]
			  , db.page_verify_option_desc AS [Page Verify Option]
			  , db.is_auto_create_stats_on
			  , db.is_auto_update_stats_on
			  , db.is_auto_update_stats_async_on
			  , db.is_parameterization_forced
			  , db.snapshot_isolation_state_desc
			  , db.is_read_committed_snapshot_on
			  , db.is_auto_close_on
			  , db.is_auto_shrink_on
			  , -1 --target_recovery_time_in_seconds
			  , (SELECT  sum(CONVERT(BIGINT, size / 128.0)) AS [Total Size in MB]
					FROM    sys.master_files f WITH ( NOLOCK )
					WHERE   f.[database_id] = db.database_id
					and type_desc = ''Rows''
					) Data
			 , (SELECT  sum(CONVERT(BIGINT, size / 128.0)) AS [Total Size in MB]
					FROM    sys.master_files f WITH ( NOLOCK )
					WHERE   f.[database_id] = db.database_id
					and type_desc = ''Log''
					) [Log] 
			  , db.State_Desc
			  , db.Create_Date
			  , db.is_published
			  , db.is_subscribed
			  , db.Collation_name
			  , (SELECT COUNT(*) * 8 / 1024 
				FROM    sys.dm_os_buffer_descriptors d WITH ( NOLOCK )
				WHERE  d.database_id = -1--db.database_id
				) [CachedSizeMBs]
			  , ( SELECT    SUM(total_worker_time) 
				FROM     sys.dm_exec_query_stats AS qs
				CROSS APPLY ( SELECT    CONVERT(INT, value) AS [DatabaseID]
							  FROM      sys.dm_exec_plan_attributes(qs.plan_handle)
							  WHERE     attribute = N''dbid''
							   ) AS F_DB
				   where F_DB.[DatabaseID] = -1--db.Database_ID
				 ) AS [CPU_Time_Ms]
				 , db.Is_Read_Only
				 , null delayed_durability_desc, db.containment_desc, db.is_cdc_enabled, db.is_broker_enabled, null is_memory_optimized_elevate_to_snapshot_on	
				 , ag.[AvailabilityGroupName], ag.PrimaryReplicaServerName, ag.LocalReplicaRole, ag.SynchronizationState, ag.IsSuspended, ag.IsJoined
				 , ss.name SourceDatabaseName, l.loginname owner
				 , m.mirroring_state_desc, m.mirroring_role_desc, m.mirroring_safety_level_desc, m.mirroring_partner_name, m.mirroring_partner_instance
				 , m.mirroring_witness_name, m.mirroring_witness_state_desc, m.mirroring_connection_timeout, m.mirroring_redo_queue 
				 , db.is_encrypted
				  , null edition, null service_objective, null elastic_pool_name, null is_query_store_on
				 --select *
		FROM    master.sys.databases AS db
				LEFT JOIN master.sys.databases ss on ss.database_id = db.source_database_id
				LEFT JOIN master.sys.dm_os_performance_counters AS lu ON db.name = lu.instance_name and lu.counter_name LIKE N''Log File(s) Used Size (KB)%''
				LEFT JOIN master.sys.dm_os_performance_counters AS ls ON db.name = ls.instance_name AND ls.counter_name LIKE N''Log File(s) Size (KB)%'' AND ls.cntr_value > 0
				left join (
					SELECT
						AG.name AS [AvailabilityGroupName],
						agstates.primary_replica AS [PrimaryReplicaServerName],
						ISNULL(arstates.role, 3) AS [LocalReplicaRole],
						dbcs.database_name AS [DatabaseName],
						ISNULL(dbrs.synchronization_state, 0) AS [SynchronizationState],
						ISNULL(dbrs.is_suspended, 0) AS [IsSuspended],
						ISNULL(dbcs.is_database_joined, 0) AS [IsJoined]
					FROM master.sys.availability_groups AS AG
					LEFT OUTER JOIN master.sys.dm_hadr_availability_group_states as agstates   ON AG.group_id = agstates.group_id
					INNER JOIN master.sys.availability_replicas AS AR   ON AG.group_id = AR.group_id
					INNER JOIN master.sys.dm_hadr_availability_replica_states AS arstates   ON AR.replica_id = arstates.replica_id AND arstates.is_local = 1
					INNER JOIN master.sys.dm_hadr_database_replica_cluster_states AS dbcs   ON arstates.replica_id = dbcs.replica_id
					LEFT OUTER JOIN master.sys.dm_hadr_database_replica_states AS dbrs   ON dbcs.replica_id = dbrs.replica_id AND dbcs.group_database_id = dbrs.group_database_id
				) ag on ag.DatabaseName = db.name
				left join master..syslogins l on db.owner_sid = l.sid
				left join sys.database_mirroring m ON m.database_id = db.database_id
		OPTION  ( RECOMPILE );'
	else if @EngineEdition = 'SQL Azure'
		set @sql = 'SELECT  serverid = '+cast(@serverid as varchar)+'
			  , db.[name] AS [DatabaseName]
			  , db.recovery_model_desc AS [Recovery Model]
			  , null AS [Log Size (KB)]
			  , null AS [Log Used (KB)]
			  , null AS [Log Used %]
			  , db.[compatibility_level] AS [DB Compatibility Level]
			  , db.page_verify_option_desc AS [Page Verify Option]
			  , db.is_auto_create_stats_on
			  , db.is_auto_update_stats_on
			  , db.is_auto_update_stats_async_on
			  , db.is_parameterization_forced
			  , db.snapshot_isolation_state_desc
			  , db.is_read_committed_snapshot_on
			  , db.is_auto_close_on
			  , db.is_auto_shrink_on
			  , 0 --target_recovery_time_in_seconds
			  , rs.storage_in_megabytes
			  , 0 [Log] 
			  , db.State_Desc
			  , db.Create_Date
			  , db.is_published
			  , db.is_subscribed
			  , db.Collation_name
			  , null [CachedSizeMBs]
			  , null AS [CPU_Time_Ms]
				 , db.Is_Read_Only
				 , null delayed_durability_desc, db.containment_desc, db.is_cdc_enabled, db.is_broker_enabled, null is_memory_optimized_elevate_to_snapshot_on	
				 , null [AvailabilityGroupName], null PrimaryReplicaServerName, null LocalReplicaRole, null SynchronizationState, null IsSuspended, null IsJoined
				 , null SourceDatabaseName, null  owner
				 , null mirroring_state_desc, null mirroring_role_desc, null mirroring_safety_level_desc, null mirroring_partner_name, null mirroring_partner_instance
				 , null mirroring_witness_name, null mirroring_witness_state_desc, null mirroring_connection_timeout, null mirroring_redo_queue 
				 , db.is_encrypted
				 , dso.edition,	dso.service_objective,	dso.elastic_pool_name, db.is_query_store_on
				 --select *
		FROM    master.sys.databases AS db
		left join [sys].[database_service_objectives] dso on db.database_id = dso.database_id
		outer apply (SELECT TOP 1 storage_in_megabytes, sku, avg_instance_cpu_percent, avg_instance_memory_percent, dtu_limit
					FROM master.sys.resource_stats
					WHERE database_name = db.name
					ORDER BY end_time DESC) rs 
		OPTION  ( RECOMPILE );
		'
	else --latest versions
		set @sql='SELECT  serverid = '+cast(@serverid as varchar)+'
			  , db.[name] AS [DatabaseName]
			  , db.recovery_model_desc AS [Recovery Model]
			  , ls.cntr_value AS [Log Size (KB)]
			  , lu.cntr_value AS [Log Used (KB)]
			  , CAST(CAST(lu.cntr_value AS FLOAT) / CAST(ls.cntr_value AS FLOAT) AS DECIMAL(18, 2)) * 100 AS [Log Used %]
			  , db.[compatibility_level] AS [DB Compatibility Level]
			  , db.page_verify_option_desc AS [Page Verify Option]
			  , db.is_auto_create_stats_on
			  , db.is_auto_update_stats_on
			  , db.is_auto_update_stats_async_on
			  , db.is_parameterization_forced
			  , db.snapshot_isolation_state_desc
			  , db.is_read_committed_snapshot_on
			  , db.is_auto_close_on
			  , db.is_auto_shrink_on
			  , -1 --target_recovery_time_in_seconds
			  , (SELECT  sum(CONVERT(BIGINT, size / 128.0)) AS [Total Size in MB]
					FROM    sys.master_files f WITH ( NOLOCK )
					WHERE   f.[database_id] = db.database_id
					and type_desc = ''Rows''
					) Data
			 , (SELECT  sum(CONVERT(BIGINT, size / 128.0)) AS [Total Size in MB]
					FROM    sys.master_files f WITH ( NOLOCK )
					WHERE   f.[database_id] = db.database_id
					and type_desc = ''Log''
					) [Log] 
			  , db.State_Desc
			  , db.Create_Date
			  , db.is_published
			  , db.is_subscribed
			  , db.Collation_name
			  , (SELECT COUNT(*) * 8 / 1024 
				FROM    sys.dm_os_buffer_descriptors d WITH ( NOLOCK )
				WHERE  d.database_id = -1--db.database_id
				) [CachedSizeMBs]
			  ,  (SELECT    SUM(total_worker_time) 
				FROM     sys.dm_exec_query_stats AS qs
				CROSS APPLY ( SELECT    CONVERT(BIGINT, value) AS [DatabaseID]
							  FROM      sys.dm_exec_plan_attributes(qs.plan_handle)
							  WHERE     attribute = N''dbid''
							   ) AS F_DB
				   where F_DB.[DatabaseID] = -1 --db.Database_ID
				 )  AS [CPU_Time_Ms]
				 , db.Is_Read_Only
				 , db.delayed_durability_desc, db.containment_desc, db.is_cdc_enabled, db.is_broker_enabled, db.is_memory_optimized_elevate_to_snapshot_on	
				 , ag.[AvailabilityGroupName], ag.PrimaryReplicaServerName, ag.LocalReplicaRole, ag.SynchronizationState, ag.IsSuspended, ag.IsJoined
				 , ss.name SourceDatabaseName, l.loginname owner
				 , m.mirroring_state_desc, m.mirroring_role_desc, m.mirroring_safety_level_desc, m.mirroring_partner_name, m.mirroring_partner_instance
				 , m.mirroring_witness_name, m.mirroring_witness_state_desc, m.mirroring_connection_timeout, m.mirroring_redo_queue 
				 , db.is_encrypted
				  , null edition, null service_objective, null elastic_pool_name, db.is_query_store_on
				 --select *
		FROM    master.sys.databases AS db
				LEFT JOIN master.sys.databases ss on ss.database_id = db.source_database_id
				LEFT JOIN master.sys.dm_os_performance_counters AS lu ON db.name = lu.instance_name and lu.counter_name LIKE N''Log File(s) Used Size (KB)%''
				LEFT JOIN master.sys.dm_os_performance_counters AS ls ON db.name = ls.instance_name AND ls.counter_name LIKE N''Log File(s) Size (KB)%'' AND ls.cntr_value > 0
				left join (
					SELECT
						AG.name AS [AvailabilityGroupName],
						agstates.primary_replica AS [PrimaryReplicaServerName],
						ISNULL(arstates.role, 3) AS [LocalReplicaRole],
						dbcs.database_name AS [DatabaseName],
						ISNULL(dbrs.synchronization_state, 0) AS [SynchronizationState],
						ISNULL(dbrs.is_suspended, 0) AS [IsSuspended],
						ISNULL(dbcs.is_database_joined, 0) AS [IsJoined]
					FROM master.sys.availability_groups AS AG
					LEFT OUTER JOIN master.sys.dm_hadr_availability_group_states as agstates   ON AG.group_id = agstates.group_id
					INNER JOIN master.sys.availability_replicas AS AR   ON AG.group_id = AR.group_id
					INNER JOIN master.sys.dm_hadr_availability_replica_states AS arstates   ON AR.replica_id = arstates.replica_id AND arstates.is_local = 1
					INNER JOIN master.sys.dm_hadr_database_replica_cluster_states AS dbcs   ON arstates.replica_id = dbcs.replica_id
					LEFT OUTER JOIN master.sys.dm_hadr_database_replica_states AS dbrs   ON dbcs.replica_id = dbrs.replica_id AND dbcs.group_database_id = dbrs.group_database_id
				) ag on ag.DatabaseName = db.name
				left join master..syslogins l on db.owner_sid = l.sid
				left join sys.database_mirroring m ON m.database_id = db.database_id
		OPTION  ( RECOMPILE );
		   '
	
	SET @SQL = 'SELECT a.*	FROM OPENQUERY(['+@SERVER+'], 
		'''+replace(@sql, '''', '''''')+'''
		) AS a
		--where not exists (select * from databases d where d.serverid = '+cast(@serverid as varchar)+' and d.databasename=a.DatabaseName)
		;'
	begin try
		insert into Databases (ServerId  ,
				DatabaseName 
				, RecoveryModel
				  , LogSizeKB 
				  , LogUsedKB 
				  , LogUsedPercentage 
				  , [DBCompatibilityLevel]
				  , [PageVerifyOption] 
				  , is_auto_create_stats_on 
				  , is_auto_update_stats_on 
				  , is_auto_update_stats_async_on 
				  , is_parameterization_forced 
				  , snapshot_isolation_state_desc 
				  , is_read_committed_snapshot_on 
				  , is_auto_close_on 
				  , is_auto_shrink_on 
				  , target_recovery_time_in_seconds 
				  , DataMB, LogMB
  				  , State_Desc
				  , Create_Date
				  , is_published
				  , is_subscribed
				  , Collation
				  , CachedSizeMbs
				  , CPUTime
				  , Is_Read_Only
				  , delayed_durability_desc, containment_desc, is_cdc_enabled, is_broker_enabled, is_memory_optimized_elevate_to_snapshot_on	
				  , AvailabilityGroup, PrimaryReplicaServerName, LocalReplicaRole, SynchronizationState, IsSuspended, IsJoined
				  , SourceDatabaseName, owner
				  , mirroring_state, mirroring_role, mirroring_safety_level, mirroring_partner, mirroring_partner_instance
				  , mirroring_witness, mirroring_witness_state, mirroring_connection_timeout, mirroring_redo_queue
				  , is_encrypted
				  , edition, service_objective, elastic_pool_name, is_query_store_on
		 )
		exec(@sql)
	end try
	begin catch
		print error_message()
		print @sql
	end catch
	FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid, @Version, @EngineEdition
END
CLOSE T_CURSOR
DEALLOCATE T_CURSOR

--select * from Databases
end
GO
/****** Object:  StoredProcedure [dbo].[spLoadDeadLockFiles]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE proc [dbo].[spLoadDeadLockFiles]   @serverid int=0 --@folders varchar(8000)='', @separator char(1)=','
as

declare @cmd varchar(8000), @DeadLockEvents varchar(255), @folder  varchar(255)

if @serverid = 0
	truncate table DeadLockFiles
else
	delete DeadLockFiles where serverid = @serverid

if object_id('tempdb..#dir') is not null
	drop table #dir

create table #dir (line varchar(8000), serverid int, rowid int identity)

declare t_cursor cursor fast_forward for
	select DeadlockEvents, serverid 
		from servers s
		where isActive=1 
		and (	s.ServerId = @serverid
			or (@serverid =0 and not exists(select * from DeadLockFiles d where s.ServerId =d.serverid))
		)
		and isnull(DeadlockEvents,'') <> '' 
open t_cursor
fetch next from t_cursor into @DeadLockEvents, @serverid
while @@FETCH_STATUS=0
begin

	set @cmd = 'dir '+@DeadLockEvents
	set @folder = replace(@DeadLockEvents , 'DeadLocks*.xel','')

	truncate table #dir
	insert into #dir (line)
	exec xp_cmdshell @cmd

	insert into DeadLockFiles (foldername,  filename, [date], size, serverid)
	select @folder
		, SUBSTRING(line, 40, 255) name
		, SUBSTRING(line, 1, 20)  [date]
		, replace(SUBSTRING(line, 21, 19),',','') size
		, @serverid
	from #dir d
	where line not like '%<DIR>%'
	and line not like '%File(s)%bytes'
	and line not like ' Directory Of %'
	and rowid>2
	and ISDATE(SUBSTRING(line, 1, 20) )=1
	and isnumeric(SUBSTRING(line, 21, 19))=1
	order by rowid

	fetch next from t_cursor into @DeadLockEvents, @serverid
end
close t_cursor
deallocate t_cursor

--drop table DeadLockFiles
--create table DeadLockFiles (rowid int identity, foldername  varchar(255), filename varchar(255), date datetime, size bigint, serverid int)

GO
/****** Object:  StoredProcedure [dbo].[spLoadDeadlocks]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE proc [dbo].[spLoadDeadlocks] @serverid varchar(10)='0', @debug bit=0
as
if @serverid = '0'
	truncate table deadlocks
else 
	delete from Errors where serverid = @serverid

declare @sql nvarchar(max), @path VARCHAR(100), @servername varchar(100)
DECLARE T_CURSOR CURSOR FAST_FORWARD FOR
	SELECT CASE when NodeName IS NULL THEN DeadlockEvents ELSE  '\\'+NodeName+'\C$\PerfLogs\Deadlock*.xel' end DeadlockEvents
		, serverid 
	FROM VWSERVERS s
	where isActive=1 and version not like '%azure%'
	and isnull(DeadlockEvents,'')<>''
	and (	s.ServerId = @serverid
			or (@serverid =0 and not exists(select * from Deadlocks d where s.ServerId =d.serverid))
		)
OPEN T_CURSOR
FETCH NEXT FROM T_CURSOR INTO @path, @serverid 
WHILE @@FETCH_STATUS=0
BEGIN
	set @sql = 'exec (''alter EVENT SESSION [DeadLocks] ON SERVER state=stop'') at ['+@ServerName+']'
	exec spExec @sql, @debug, 1, 0

	set @sql='
		;WITH cte AS 
		( 
			SELECT 
				CAST(event_data AS XML) AS event_data 
			FROM 
				sys.fn_xe_file_target_read_file('''+@path+''', NULL, NULL, NULL) 
		), 
		cte2 AS 
		( 
			SELECT 	event_number = ROW_NUMBER() OVER (ORDER BY T.x) 
			,    event_name = T.x.value(''@name'', ''varchar(100)'') 
			,    event_timestamp = T.x.value(''@timestamp'', ''datetimeoffset'') 
			,    event_data 
			FROM 
				cte    
			CROSS APPLY 
				event_data.nodes(''/event'') T(x) 
		) 
		SELECT '+@serverid+' serverid, event_timestamp, event_data 
		FROM cte2 
		where not exists (select * from Deadlocks d where d.serverid = '+@serverid+' and d.event_timestamp = cte2.event_timestamp)
		--noisy false deadlocks
		and cast(event_data as varchar(max)) not like ''%SELECT DISTINCT base.order_guid, base.company_guid, base.catalog_guid, base.catalog_cd, base.catalog_name, base.site_guid, base.site_cd, base.site_name, base.provider_guid, base.provider_cd, base.provider_name, base.customer_guid, base.parent_order_guid%''   '

	insert into Deadlocks (ServerId,event_timestamp,event_data)
	exec dbo.spExec @sql, @debug, 1, 0
	
	set @sql = 'exec (''alter EVENT SESSION [DeadLocks] ON SERVER state=stop'') at ['+@ServerName+']'
	exec spExec @sql, @debug, 1, 0

	FETCH NEXT FROM T_CURSOR INTO @path, @serverid
END
CLOSE T_CURSOR
DEALLOCATE T_CURSOR
GO
/****** Object:  StoredProcedure [dbo].[spLoadErrorFiles]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE proc [dbo].[spLoadErrorFiles]   @serverid int=0 --@folders varchar(8000)='', @separator char(1)=','
as

declare @cmd varchar(8000), @ErrorEvents varchar(255), @folder  varchar(255)

if @serverid = 0
	truncate table ErrorFiles
else
	delete ErrorFiles where serverid = @serverid

if object_id('tempdb..#dir') is not null
	drop table #dir

create table #dir (line varchar(8000), serverid int, rowid int identity)

declare t_cursor cursor fast_forward for
	select ErrorEvents, serverid 
		from servers s
		where isActive=1 
		and (	s.ServerId = @serverid
			or (@serverid =0 and not exists(select * from ErrorFiles d where s.ServerId =d.serverid))
		)
		and isnull(ErrorEvents,'') <> '' 
open t_cursor
fetch next from t_cursor into @ErrorEvents, @serverid
while @@FETCH_STATUS=0
begin

	set @cmd = 'dir '+@ErrorEvents
	set @folder = replace(@ErrorEvents , 'Errors*.xel','')

	truncate table #dir
	insert into #dir (line)
	exec xp_cmdshell @cmd

	insert into ErrorFiles (foldername,  filename, [date], size, serverid)
	select @folder
		, SUBSTRING(line, 40, 255) name
		, SUBSTRING(line, 1, 20)  [date]
		, replace(SUBSTRING(line, 21, 19),',','') size
		, @serverid
	from #dir d
	where line not like '%<DIR>%'
	and line not like '%File(s)%bytes'
	and line not like ' Directory Of %'
	and rowid>2
	and ISDATE(SUBSTRING(line, 1, 20) )=1
	and isnumeric(SUBSTRING(line, 21, 19))=1
	order by rowid

	fetch next from t_cursor into @ErrorEvents, @serverid
end
close t_cursor
deallocate t_cursor

--drop table ErrorFiles
--create table ErrorFiles (rowid int identity, foldername  varchar(255), filename varchar(255), date datetime, size bigint, serverid int)


GO
/****** Object:  StoredProcedure [dbo].[spLoadErrors]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE proc [dbo].[spLoadErrors] @serverid varchar(10)='0'
as
if @serverid = '0'
	truncate table Errors
else 
	delete from Errors where serverid = @serverid

--print getdate()
declare @sql nvarchar(max), @path VARCHAR(100), @servername varchar(100)
DECLARE T_CURSOR CURSOR FAST_FORWARD FOR
	SELECT CASE when NodeName IS NULL THEN ErrorEvents ELSE  '\\'+NodeName+'\C$\PerfLogs\Error*.xel' end ErrorEvents
		, serverid, servername 
	FROM vwSERVERS s
	where isActive=1 and version not like '%azure%'
	and isnull(ErrorEvents,'') <> '' 
	and (	s.ServerId = @serverid
			or (@serverid =0 and not exists(select * from Errors d where s.ServerId =d.serverid))
		)
OPEN T_CURSOR
FETCH NEXT FROM T_CURSOR INTO @path, @serverid, @servername
WHILE @@FETCH_STATUS=0
BEGIN
		set @sql = 'exec(''alter EVENT SESSION [Errors] ON SERVER state=stop'') at ['+@servername+']'
		exec spExec @sql,0,1,0

		set @sql='
			;WITH cte AS 
			( 
				SELECT 
					CAST(event_data AS XML) AS event_data 
				FROM 
					sys.fn_xe_file_target_read_file('''+@path+''', NULL, NULL, NULL) 
			), 
			cte2 AS 
			( 
				SELECT 
					event_number = ROW_NUMBER() OVER (ORDER BY T.x) 
				,    event_name = T.x.value(''@name'', ''varchar(100)'') 
				,    event_timestamp = T.x.value(''@timestamp'', ''datetimeoffset'') 
				,    event_data 
				FROM 
					cte    
				CROSS APPLY 
					event_data.nodes(''/event'') T(x) 
			), 
			cte3 AS 
			( 
				SELECT 
					c.event_number, 
					c.event_timestamp, 
					--data_field = T2.x.value(''local-name(.)'', ''varchar(100)''), 
					data_name = T2.x.value(''@name'', ''varchar(100)''), 
					data_value = T2.x.value(''value[1]'', ''varchar(max)''), 
					data_text = T2.x.value(''text[1]'', ''varchar(max)'') 
				FROM 
					cte2 c 
				CROSS APPLY 
					c.event_data.nodes(''event/*'') T2(x) 
			), 
			cte4 AS 
			( 
				SELECT 
					* 
				FROM 
					cte3 
				WHERE 
					data_name IN (''error_number'', ''severity'', ''message'', ''sql_text'', ''database_name'', ''database_id'', ''client_hostname'', ''client_app_name'', ''collect_system_time'', ''username'') 
			) 
			SELECT '+@serverid+' ServerId,
				event_timestamp,
				error_number,
				severity,
				message,
				sql_text,
				database_name,
				username,
				client_hostname,
				client_app_name
			FROM 

				cte4 
			PIVOT 
				(MAX(data_value) FOR data_name IN ([error_number], [severity], [message], sql_text, database_name, database_id, username, client_hostname, client_app_name, collect_system_time)) T 
			where not exists (select * from Errors d where d.serverid = '+@serverid+' and d.event_timestamp = t.event_timestamp and d.database_name = t.database_name and d.username = t.username)
		   '
		begin try
			insert into Errors (ServerId,event_timestamp,errornumber,severity,errormessage,sql_text,database_name,username,client_hostname,client_app_name)
			--exec spExec @sql,0,1,0
			exec dbo.spExec @sql
		end try
		begin catch
			update servers set  Error = ERROR_MESSAGE() where serverid=@serverid
		end catch

		set @sql = 'exec(''alter EVENT SESSION [Errors] ON SERVER state=start'') at ['+@servername+']'
		exec spExec @sql,0,1,0
	
	FETCH NEXT FROM T_CURSOR INTO @path, @serverid, @servername
END
CLOSE T_CURSOR
DEALLOCATE T_CURSOR

--cleanup errors we dont care about
delete from errors 
where username ='NT AUTHORITY\SYSTEM'

--or (errormessage like 'Violation of PRIMARY KEY constraint ''PK_TBL_CustomerAgreement_NEW%')
--or (errormessage like 'Violation of PRIMARY KEY constraint ''PK_TBL_CustomsIDValidation%')


GO


CREATE proc [dbo].[spLoadIndexFragmentation] @serverid varchar(10)='0'
as

if @serverid = '0'
	truncate table IndexFragmentation

declare @sql nvarchar(max), @SERVER VARCHAR(100), @DatabaseName varchar(100), @DatabaseId varchar(10)
	, @Version	varchar	(255), @linkedserver varchar(255), @EngineEdition varchar(50)
DECLARE T_CURSOR CURSOR FAST_FORWARD FOR
	SELECT SERVERNAME, serverid, version, EngineEdition FROM SERVERS s
	where isActive=1 
	and (	s.ServerId = @serverid
			or (@serverid =0 and not exists(select * from IndexFragmentation d where s.ServerId =d.serverid))
		)
OPEN T_CURSOR
FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid, @Version, @EngineEdition
WHILE @@FETCH_STATUS=0
BEGIN
	declare d_cursor cursor fast_forward for
		select databaseid, databasename 
		from vwDatabasesOnLineNonSystem 
		where serverid=@serverid 
	open d_cursor
	FETCH NEXT FROM d_CURSOR INTO @databaseid, @databasename
	while @@FETCH_STATUS=0
	begin
		IF @EngineEdition = 'SQL Azure' 
			set @sql='
			SELECT  s.name [SchemaName], o.name [TableName]
				  , i.name AS [IndexName]
				  , ps.index_type_desc
				  , ps.avg_fragmentation_in_percent
				  , ps.fragment_count
				  , ps.page_count
			FROM    sys.dm_db_index_physical_stats( NULL, NULL, NULL, NULL, ''LIMITED'') AS ps
			INNER JOIN sys.indexes AS i WITH ( NOLOCK ) ON ps.[object_id] = i.[object_id]
																   AND ps.index_id = i.index_id
			inner join SYS.sysobjects o on  i.object_id = o.id
			inner join sys.schemas s on s.schema_id = o.uid
			where  page_count > 1000-->= 8 MB
			ORDER BY avg_fragmentation_in_percent DESC
			OPTION  ( RECOMPILE );
		'
		ELSE set @sql='
			SELECT s.name [SchemaName], o.name [TableName]
				  , i.name AS [IndexName]
				  , ps.index_type_desc
				  , ps.avg_fragmentation_in_percent
				  , ps.fragment_count
				  , ps.page_count
			FROM    ['+@DatabaseName+'].sys.dm_db_index_physical_stats((select dbid from master..sysdatabases where name='''+@DatabaseName+'''), NULL, NULL, NULL, ''LIMITED'') AS ps
			INNER JOIN ['+@DatabaseName+'].sys.indexes AS i WITH ( NOLOCK ) ON ps.[object_id] = i.[object_id]
																   AND ps.index_id = i.index_id
			inner join ['+@DatabaseName+'].sys.sysobjects o on  i.object_id = o.id
			inner join ['+@DatabaseName+'].sys.schemas s on s.schema_id = o.uid
			WHERE   database_id = (select dbid from master..sysdatabases where name='''+@DatabaseName+''')
					AND page_count > 1000-->= 8 MB
			ORDER BY avg_fragmentation_in_percent DESC
			OPTION  ( RECOMPILE );
		'
		
		if @DatabaseName = 'master' or @EngineEdition <> 'SQL Azure' 
			set @linkedserver = @server
		else 
			set @linkedserver = @server+'.'+@DatabaseName

		SET @SQL = 'SELECT '+@serverid+', '+@databaseid+', a.*
			FROM OPENQUERY(['+@linkedserver+'], 
			'''+replace(@sql, '''', '''''')+'''
			) AS a
			;'
			--print @sql
		begin try
			insert into IndexFragmentation (Serverid,DatabaseId,SchemaName,TableName,IndexName,index_type_desc,avg_fragmentation_in_percent,fragment_count,page_count)
			exec dbo.spExec @sql 
		end try
		begin catch
			print error_message()
			print @sql
		end catch
		FETCH NEXT FROM d_CURSOR INTO @databaseid, @databasename
	end
	close d_cursor
	deallocate d_cursor
	FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid, @Version , @EngineEdition
END
CLOSE T_CURSOR
DEALLOCATE T_CURSOR

GO
/****** Object:  StoredProcedure [dbo].[spLoadIndexUsage]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




CREATE proc [dbo].[spLoadIndexUsage]  @serverid varchar(10)='0'
as

if @serverid = '0'
	truncate table [IndexUsage]

declare @sql nvarchar(max), @SERVER VARCHAR(100), @DatabaseName varchar(100), @DatabaseId varchar(10), @version varchar(255), @linkedserver varchar(255), @EngineEdition varchar(50)

DECLARE T_CURSOR CURSOR FAST_FORWARD FOR
	SELECT SERVERNAME, serverid, version, EngineEdition FROM SERVERS s
	where isActive=1 
	and (	s.ServerId = @serverid
			or (@serverid =0 and not exists(select * from IndexUsage d where s.ServerId =d.serverid))
		)
OPEN T_CURSOR
FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid , @version, @EngineEdition
WHILE @@FETCH_STATUS=0
BEGIN
	declare d_cursor cursor fast_forward for
		select databaseid, databasename 
		from vwDatabasesOnLineNonSystem 
		where serverid=@serverid 
	open d_cursor
	FETCH NEXT FROM d_CURSOR INTO @databaseid, @databasename
	while @@FETCH_STATUS=0
	begin
		set @sql='		
with a as (
SELECT ds.name AS data_space 
      , au.type_desc AS allocation_desc 
      , au.total_pages / 128 AS size_mbs 
      , au.used_pages / 128 AS used_size 
      , au.data_pages / 128 AS data_size 
      , sch.name AS table_schema 
      , obj.type_desc AS object_type       
      , obj.name AS table_name 
      , idx.type_desc AS index_type 
      , idx.name AS index_name 
	 , idx.is_unique
	 , idx.is_disabled
	 , idx.filter_definition
	 , d.[physical_name] AS [database_file]
	 , [writes]
	 , reads
	 , idx.index_id, fill_factor
	 , 0  avg_fragmentation_in_percent
    , cols = stuff((select '', '' + name as [text()]
				from ['+@DatabaseName+'].sys.index_columns ic
				join ['+@DatabaseName+'].sys.columns c on ic.column_id = c.column_id AND c.object_id = ic.object_id
				 where ic.[object_id] = idx.object_id
					 and ic.[index_id] = idx.index_id
				and ic.is_included_column = 0
				order by ic.key_ordinal
			 for xml path('''')), 1, 2, '''')
    , included = stuff((select '', '' + name as [text()]
				from ['+@DatabaseName+'].sys.index_columns ic
				join ['+@DatabaseName+'].sys.columns c on ic.column_id = c.column_id AND c.object_id = ic.object_id
				 where ic.[object_id] = idx.object_id
					 and ic.[index_id] = idx.index_id
				and ic.is_included_column = 1
				order by index_column_id
			 for xml path('''')), 1, 2, '''')
	, pa.data_compression_desc
FROM ['+@DatabaseName+'].sys.objects AS obj ( NOLOCK ) 
    INNER JOIN ['+@DatabaseName+'].sys.schemas AS sch ( NOLOCK ) ON obj.schema_id = sch.schema_id 
    INNER JOIN ['+@DatabaseName+'].sys.indexes AS idx  ( NOLOCK ) ON obj.object_id = idx.object_id
	LEFT JOIN  ['+@DatabaseName+'].sys.filegroups f ON f.[data_space_id] = idx.[data_space_id]
	LEFT JOIN  ['+@DatabaseName+'].sys.partitions AS PA  ( NOLOCK ) ON PA.object_id = idx.object_id and PA.index_id = idx.index_id 
	LEFT JOIN  ['+@DatabaseName+'].sys.allocation_units AS au ( NOLOCK ) ON (au.type IN (1, 3)  AND au.container_id = PA.hobt_id) 
            OR  (au.type = 2  AND au.container_id = PA.partition_id) 
	LEFT JOIN  ['+@DatabaseName+'].sys.data_spaces AS ds  ( NOLOCK ) ON ds.data_space_id = au.data_space_id 
    LEFT JOIN  ['+@DatabaseName+'].sys.database_files d ON f.[data_space_id] = d.[data_space_id]
     outer apply (
	   select isnull(user_updates,0) AS writes
		  , isnull(user_seeks,0) + isnull(user_scans,0) + isnull(user_lookups,0) AS reads
	   from ['+@DatabaseName+'].sys.dm_db_index_usage_stats AS s WITH ( NOLOCK )
	   where s.[object_id] = idx.[object_id]
	   and idx.index_id = s.index_id
	   and idx.is_disabled = 0
    ) usage
	/* outer apply (
	   SELECT avg(indexstats.avg_fragmentation_in_percent ) avg_fragmentation_in_percent
	   FROM sys.dm_db_index_physical_stats(DB_ID('''+@DatabaseName+'''), obj.object_id, idx.index_id , NULL, NULL) indexstats 
	   WHERE indexstats.object_id = obj.object_id 
		  and indexstats.index_id = idx.index_id 
		  and idx.is_disabled = 0
		  and 1=2--disabled for to speed up load
    ) frag*/
WHERE obj.type_desc in (''USER_TABLE'',''VIEW'')
), b as (
select data_space, allocation_desc, sum(size_mbs) size_mbs, sum(used_size) used_size, sum(data_size) data_size
	, table_schema
	, object_type, table_name, index_type, index_name, is_unique, is_disabled, filter_definition, min(database_file) database_file
	, sum(writes) writes, sum(reads) reads, index_id, fill_factor, avg(avg_fragmentation_in_percent) avg_fragmentation_in_percent, cols, included, data_compression_desc
from a
group by data_space, allocation_desc, table_schema
	, object_type, table_name, index_type, index_name, is_unique, is_disabled, filter_definition
	, index_id, fill_factor, cols, included, data_compression_desc
)
select * 
	, ''drop index ['+@DatabaseName+'].[''+table_name+''].[''+index_name+'']''  drop_cmd
	, ''alter index [''+index_name+''] on ['+@databasename+'].[''+table_schema+''].[''+table_name+''] disable '' disable_cmd
	, ''create ''+ case when is_unique = 1 then ''unique'' else '''' end +'' index ''+index_name+'' on [''+table_schema+''].[''+table_name+''] ('' +cols + '')''
	+ case when included is not null then '' include (''+included+'')'' else '''' end
	+ '' WITH (FILLFACTOR=''+cast(fill_factor as varchar)+'', ONLINE=OFF, SORT_IN_TEMPDB=ON, PAD_INDEX = ON, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, DROP_EXISTING=ON )'' create_cmd
from b
order by table_name, index_name 					  					
'
	
		if @DatabaseName = 'master' or @EngineEdition <> 'SQL Azure' 
			set @linkedserver = @server
		else 
			set @linkedserver = @server+'.'+@DatabaseName

		SET @SQL = 'SELECT '+@serverid+' as serverid, '+@databaseid+' as databaseid, data_space,allocation_desc,table_schema,object_type,table_name,index_type,index_name,is_unique,is_disabled,database_file,size_mbs,used_size,data_size,writes,reads,index_id,fill_factor,avg_fragmentation_in_percent,cols,included,filter_definition,drop_cmd,disable_cmd,create_cmd, data_compression_desc
			FROM OPENQUERY(['+@linkedserver+'], 
			'''+replace(@sql, '''', '''''')+'''
			) AS a;'
		begin try
			insert into [IndexUsage] (ServerId,DatabaseId, data_space,allocation_desc,table_schema,object_type,table_name,index_type,index_name,is_unique,is_disabled,database_file,size_mbs,used_size,data_size,writes,reads,index_id,fill_factor,avg_fragmentation_in_percent,cols,included,filter_definition,drop_cmd,disable_cmd,create_cmd, data_compression_desc)
			exec (@sql)
		end try
		begin catch
			print @sql
			print error_message()
		end catch
		FETCH NEXT FROM d_CURSOR INTO @databaseid, @databasename
	end
	close d_cursor
	deallocate d_cursor
	FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid, @version, @EngineEdition
END
CLOSE T_CURSOR
DEALLOCATE T_CURSOR



upDATE IU SET DatabaseObjectId = (
	SELECT TOP 1 DatabaseObjectId 
	FROM DatabaseObjects DO
	WHERE DO.ServerId = IU.ServerId AND DO.DatabaseId = IU.DatabaseId
	AND IU.table_schema = DO.SchemaName AND IU.table_name =  do.ObjectName
	and do.Xtype='u'
	)
FROM [IndexUsage] IU 



GO
/****** Object:  StoredProcedure [dbo].[spLoadJobHistory]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



CREATE proc [dbo].[spLoadJobHistory]  @serverid varchar(10)='0'
as

/**********************************************
	JOB ERRORS
***********************************************/
declare @sql nvarchar(max), @SERVER VARCHAR(100), @instance_id int

if @serverid = '0'
	truncate table JobHistory
else 
	delete from JobHistory where serverid=@serverid

DECLARE T_CURSOR CURSOR FAST_FORWARD FOR
	SELECT SERVERNAME, serverid, isnull(e.instance_id,0) instance_id 
	FROM vwServersNonAzureSql s 
	outer apply (
		select max(e.instance_id) instance_id
		from JobHistory e
		join Jobs j on j.JobId = e.JobId
		where j.ServerId = s.ServerId
	) e
	where s.isActive=1 
	and (	s.ServerId = @serverid
			or (@serverid = '0')
		)
OPEN T_CURSOR
FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid , @instance_id
WHILE @@FETCH_STATUS=0
BEGIN
	if @SERVER = @@SERVERNAME
		set @SERVER = '.' 
	set @sql='
			SELECT	serverid = '+cast(@serverid as varchar)+', j.name
				  , js.step_name
				  , jh.[message]
				  , jh.step_id
				  , js.subsystem
				  , LEFT(js.command, 4000) AS command
				  , js.output_file_name
				  , CAST(STUFF(STUFF(CAST(jh.run_date AS VARCHAR), 5, 0, ''/''), 8, 0, ''/'') + '' '' + STUFF(STUFF(RIGHT(''000000''+ CAST(jh.run_time AS VARCHAR), 6),3, 0, '':''), 6, 0, '':'') AS DATETIME) AS RunDateTime
				  , jh.run_duration
				  , jh.instance_id
				  , jh.job_id
				  , jh.run_status 
				  , js.database_name
				  , js.database_user_name
				  , msdb.dbo.udf_schedule_description(sch.freq_type,
											 sch.freq_interval,
											 sch.freq_subday_type,
											 sch.freq_subday_interval,
											 sch.freq_relative_interval,
											 sch.freq_recurrence_factor,
											 sch.active_start_date,
											 sch.active_end_date,
											 sch.active_start_time,
											 sch.active_end_time) AS ScheduleDscr 
			FROM    msdb.dbo.sysjobhistory jh ( NOLOCK )
			INNER JOIN msdb.dbo.sysjobs j ( NOLOCK ) ON jh.job_id = j.job_id
			INNER JOIN msdb.dbo.sysjobsteps js ( NOLOCK ) ON jh.job_id = js.job_id AND js.step_id = jh.step_id
			outer apply(select top 1 * from  [msdb].[dbo].[sysjobschedules] AS jsch where j.[job_id] = jsch.[job_id] ) jsch
			LEFT JOIN [msdb].[dbo].[sysschedules] AS sch ON jsch.[schedule_id] = sch.[schedule_id]
			WHERE  1=1
				AND jh.run_date >= cast(CONVERT(varchar(10),GetDate()-10,112) as int)--last 10 days
				AND jh.instance_id > '+cast(@instance_id as varchar)+'
			   '
	
		SET @SQL = '
		SELECT j.jobid, a.*
			FROM OPENQUERY(['+@SERVER+'], 
			'''+replace(@sql, '''', '''''')+'''
			) AS a
		left join jobs j on j.serverid = '+@serverid+' and j.jobidentifier = a.job_id
			;'
	begin try
			insert into jobHistory (JobId,serverid,job_name,step_name,message,step_id,subsystem,command,output_file_name,RunDateTime,run_duration,instance_id,jobidentifier, run_status, database_name, database_user_name,ScheduleDscr )
			exec (@sql)
	end try
	begin catch
		print error_message()
		print @sql
	end catch
	FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid, @instance_id
END
CLOSE T_CURSOR
DEALLOCATE T_CURSOR
GO
/****** Object:  StoredProcedure [dbo].[spLoadJobs]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE proc [dbo].[spLoadJobs] @serverid int = 0
as

/***********************************************
		JOBS
***********************************************/

if @serverid = '0'
begin
	delete from JobHistory
	delete from JobSteps
	delete from Jobs
end
else 
begin
	delete from JobHistory where serverid=@serverid
	delete from JobSteps where serverid=@serverid
	delete from Jobs where serverid=@serverid
end

declare @sql nvarchar(max), @SERVER VARCHAR(100)
DECLARE T_CURSOR CURSOR FAST_FORWARD FOR
	SELECT SERVERNAME, serverid FROM vwServersNonAzureSql s
	where isActive=1 
	and (	s.ServerId = @serverid
			or (@serverid =0 and not exists(select * from jobs d where s.ServerId =d.serverid))
		)
OPEN T_CURSOR
FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid 
WHILE @@FETCH_STATUS=0
BEGIN
	if @SERVER = @@SERVERNAME
		set @SERVER = '.' 

	set @sql='
		SELECT  distinct serverid = '+cast(@serverid as varchar)+',
				j.[name]  ,
				j.[description] ,
				j.[enabled],
				msdb.dbo.udf_schedule_description(sch.freq_type,
											 sch.freq_interval,
											 sch.freq_subday_type,
											 sch.freq_subday_interval,
											 sch.freq_relative_interval,
											 sch.freq_recurrence_factor,
											 sch.active_start_date,
											 sch.active_end_date,
											 sch.active_start_time,
											 sch.active_end_time) AS ScheduleDscr ,

				o.name AS Operator ,
				o.enabled AS OperatorEnabled ,
				o.email_address AS Operator_email_address ,
				l.loginname AS owner,

				st.[step_name] AS [JobStartStepName] ,
				case when sch.[schedule_uid] is not null then 1 else 0 end AS [IsScheduled] ,
				sch.[name] AS [JobScheduleName] ,
				''Frequency'' = CASE WHEN sch.freq_type = 1
															 THEN ''Once''
															 WHEN sch.freq_type = 4
															 THEN ''Daily''
															 WHEN sch.freq_type = 8
															 THEN ''Weekly''
															 WHEN sch.freq_type = 16
															 THEN ''Monthly''
															 WHEN sch.freq_type = 32
															 THEN ''Monthly relative''
															 WHEN sch.freq_type = 32
															 THEN ''Execute when SQL Server Agent starts''
														END ,
				''Units'' = CASE WHEN sch.freq_subday_type = 1
															THEN ''At the specified time''
															WHEN sch.freq_subday_type = 2
															THEN ''Seconds''
															WHEN sch.freq_subday_type = 4
															THEN ''Minutes''
															WHEN sch.freq_subday_type = 8
															THEN ''Hours''
													   END ,
				CAST(CAST(sch.active_start_date AS VARCHAR(15)) AS DATETIME) AS active_start_date ,
				CAST(CAST(sch.active_end_date AS VARCHAR(15)) AS DATETIME) AS active_end_date ,
				STUFF(STUFF(RIGHT(''000000'' + CAST(jsch.next_run_time AS VARCHAR), 6),
							3, 0, '':''), 6, 0, '':'') AS Run_Time ,
				CONVERT(VARCHAR(24), sch.date_created) AS Created_Date,
				j.job_id
		FROM    [msdb].[dbo].[sysjobs] AS j
				LEFT JOIN [msdb].[sys].[servers] AS s ON j.[originating_server_id] = s.[server_id]
				LEFT JOIN [msdb].[dbo].[syscategories] AS c ON j.[category_id] = c.[category_id]
				LEFT JOIN [msdb].[dbo].[sysjobsteps] AS st ON j.[job_id] = st.[job_id]
															  AND j.[start_step_id] = st.[step_id]
				LEFT JOIN [msdb].[sys].[database_principals] AS prin ON j.[owner_sid] = prin.[sid]
				outer apply(select top 1 * from  [msdb].[dbo].[sysjobschedules] AS jsch where j.[job_id] = jsch.[job_id] ) jsch
				LEFT JOIN [msdb].[dbo].[sysschedules] AS sch ON jsch.[schedule_id] = sch.[schedule_id]
		
				LEFT OUTER JOIN msdb.dbo.sysoperators AS o WITH ( NOLOCK ) ON j.notify_email_operator_id = o.id
				LEFT OUTER JOIN master.sys.syslogins AS l WITH ( NOLOCK ) ON j.owner_sid = l.sid
		ORDER BY 1
		   '
	
	SET @SQL = 'SELECT a.*	FROM OPENQUERY(['+@SERVER+'], 
		'''+replace(@sql, '''', '''''')+'''
		) AS a;'
	begin try
		insert into jobs (ServerId,Jobname,Description,IsEnabled,ScheduleDscr,Operator,OperatorEnabled,Operator_email_address,Owner,JobStartStepName,IsScheduled,JobScheduleName,Frequency,Units,Active_start_date,Active_end_date,Run_Time,Created_Date, [jobidentifier])
		exec (@sql)
	end try
	begin catch
		print error_message()
		print @sql
	end catch
	FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid
END
CLOSE T_CURSOR
DEALLOCATE T_CURSOR
--select * from jobs

GO
/****** Object:  StoredProcedure [dbo].[spLoadJobsRunning]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE proc [dbo].[spLoadJobsRunning]  @serverid int=0
as
if @serverid = '0'
	truncate table JobsRunning
else 
	delete from JobsRunning where serverid=@serverid


/**************
	Volumes
**************/
declare @sql nvarchar(max), @SERVER VARCHAR(100), @Version	varchar	(255)

DECLARE T_CURSOR CURSOR FAST_FORWARD FOR
	SELECT SERVERNAME, serverid, version 
	FROM vwServersNonAzureSql s
	where isActive=1 
	and (	s.ServerId = @serverid
			or (@serverid =0 and not exists(select * from Volumes d where s.ServerId =d.serverid))
		)
OPEN T_CURSOR
FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid, @Version 
WHILE @@FETCH_STATUS=0
BEGIN
	if @SERVER = @@SERVERNAME
		set @SERVER = '.' 
	set @sql='
		SELECT  distinct serverid = '+cast(@serverid as varchar)+'
			 , j.job_id as jobidentifier
			 , j.name
			 , start_execution_date
			, DATEDIFF(SECOND,ja.start_execution_date,GetDate()) AS Seconds
		FROM msdb..sysjobactivity ja
		JOIN msdb..sysjobs j on j.job_id = ja.job_id
		WHERE ja.stop_execution_date IS NULL 
		AND ja.start_execution_date IS NOT NULL 
		and not exists( select 1
			from msdb..sysjobactivity new
			where new.job_id = ja.job_id
			and new.start_execution_date > ja.start_execution_date
			)
		   '
	
	SET @SQL = 'SELECT a.*	FROM OPENQUERY(['+@SERVER+'], 
		'''+replace(@sql, '''', '''''')+'''
		) AS a;'
	begin try
		insert into JobsRunning (ServerId,jobidentifier,job_name,start_execution_date,seconds)
		exec dbo.spExec @sql = @sql, @raiserror = 0
	end try
	begin catch
		print error_message()
		print @sql
	end catch
	FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid, @Version
END
CLOSE T_CURSOR
DEALLOCATE T_CURSOR

update r  set JobId = j.JobId
from JobsRunning r
join jobs j on r.jobidentifier = j.jobidentifier and r.ServerId = j.ServerId
where r.JobId is null 

GO
/****** Object:  StoredProcedure [dbo].[spLoadJobSteps]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE proc [dbo].[spLoadJobSteps] @serverid varchar(10)='0'
as
if @serverid = '0'
	truncate table JobSteps
else 
	delete from JobSteps where serverid=@serverid

/**********************************************
	JOB StepS
***********************************************/
declare @sql nvarchar(max), @SERVER VARCHAR(100)
DECLARE T_CURSOR CURSOR FAST_FORWARD FOR
	SELECT SERVERNAME, serverid 
	FROM vwServersNonAzureSql s
	where isActive=1 
	and (	s.ServerId = @serverid
			or (@serverid =0 and not exists(select * from JobSteps js join Jobs j on j.JobId = js.JobId where s.ServerId =j.serverid))
		)
OPEN T_CURSOR
FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid 
WHILE @@FETCH_STATUS=0
BEGIN
	if @SERVER = @@SERVERNAME
		set @SERVER = '.' 

	set @sql='
		SELECT 	serverid = '+cast(@serverid as varchar)+', j.[name] job_name ,
			msdb.dbo.udf_schedule_description(sch.freq_type,
											 sch.freq_interval,
											 sch.freq_subday_type,
											 sch.freq_subday_interval,
											 sch.freq_relative_interval,
											 sch.freq_recurrence_factor,
											 sch.active_start_date,
											 sch.active_end_date,
											 sch.active_start_time,
											 sch.active_end_time) AS ScheduleDscr ,
			j.[enabled],
			st.step_id,
			st.[step_name] ,
			st.database_name,
			st.command,
			j.job_id
		FROM [msdb].[dbo].[sysjobs] AS j
		LEFT JOIN [msdb].[dbo].[sysjobsteps] AS st ON j.[job_id] = st.[job_id]  
		outer apply(select top 1 * from  [msdb].[dbo].[sysjobschedules] AS jsch where j.[job_id] = jsch.[job_id] ) jsch
		LEFT JOIN [msdb].[dbo].[sysschedules] AS sch ON jsch.[schedule_id] = sch.[schedule_id]
		ORDER BY j.[name], st.step_id
		OPTION (ROBUST PLAN)
			   '
	
		SET @SQL = '
		SELECT serverid = '+cast(@serverid as varchar)+', j.jobid, a.job_name, a.ScheduleDscr,a.enabled,a.step_id,a.step_name,a.database_name,a.command 
			FROM OPENQUERY(['+@SERVER+'], 
			'''+replace(@sql, '''', '''''')+'''
			) AS a
		join jobs j on j.serverid = '+@serverid+' and j.jobidentifier = a.job_id
			;'
	begin try
		--print @sql
			insert into jobSteps (serverid,JobId,job_name,ScheduleDscr,enabled,step_id,step_name,database_name,command )
			exec (@sql)
	end try
	begin catch
		print error_message()
		print @sql
	end catch
	FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid
END
CLOSE T_CURSOR
DEALLOCATE T_CURSOR
--select * from jobSteps

GO
/****** Object:  StoredProcedure [dbo].[spLoadLogins]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



CREATE proc [dbo].[spLoadLogins] @serverid varchar(10)='0'
as

/**************
	Logins
**************/

declare @sql nvarchar(max), @SERVER VARCHAR(100), @version varchar(255), @EngineEdition varchar(50)

DECLARE T_CURSOR CURSOR FAST_FORWARD FOR
	SELECT SERVERNAME, serverid, version, EngineEdition 
	FROM vwServersNonAzureSql s
	where isActive=1 
	and (	s.ServerId = @serverid
			or (@serverid =0 /*and not exists(select * from Logins d where s.ServerId =d.serverid)*/)
		)

OPEN T_CURSOR
FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid , @version , @EngineEdition
WHILE @@FETCH_STATUS=0
BEGIN
	if @SERVER = @@SERVERNAME
		set @SERVER = '.' 

	set @sql='
		SELECT LoginName,denylogin,hasaccess,isntname,isntgroup,isntuser,sysadmin,securityadmin,serveradmin,setupadmin,processadmin,diskadmin,dbcreator,bulkadmin 
		FROM master..syslogins 
		where name not like ''#%'' and name not like ''NT SERVICE%'' and name not like ''NT SERVICE%''
		'
	
	SET @SQL = 'SELECT serverid = '+@serverid +', a.*, 0,0,0,0,0
		FROM OPENQUERY(['+@SERVER+'], 
		'''+replace(@sql, '''', '''''')+'''
		) AS a;'
	begin try
		insert into Logins (ServerId,LoginName,denylogin,hasaccess,isntname,isntgroup,isntuser,sysadmin,securityadmin,serveradmin,setupadmin,processadmin,diskadmin,dbcreator,bulkadmin, SQLAgentOperatorRole, SQLAgentReaderRole, SQLAgentUserRole, db_ssisadmin, db_ssisltduser)
		exec dbo.spExec @sql

		set @sql='
		SELECT USERNAME,
			   MAX(CASE ROLENAME WHEN ''SQLAgentOperatorRole''         THEN 1 ELSE 0 END) AS SQLAgentOperatorRole,
			   MAX(CASE ROLENAME WHEN ''SQLAgentReaderRole ''   THEN 1 ELSE 0 END) AS SQLAgentReaderRole ,
			   MAX(CASE ROLENAME WHEN ''SQLAgentUserRole''  THEN 1 ELSE 0 END) AS SQLAgentUserRole,
			   MAX(CASE ROLENAME WHEN ''db_ssisadmin''   THEN 1 ELSE 0 END) AS db_ssisadmin,
			   MAX(CASE ROLENAME WHEN ''db_ssisltduser''        THEN 1 ELSE 0 END) AS db_ssisltduser
		   FROM (SELECT B.NAME AS USERNAME, C.NAME AS ROLENAME, B.CREATEDATE, B.UPDATEDATE
				FROM [msdb].dbo.sysmembers A   
				JOIN [msdb].dbo.sysusers  B  ON A.MEMBERUID = B.UID
				JOIN [msdb].dbo.sysusers C ON A.GROUPUID = C.UID 
				 )S   
				   GROUP BY USERNAME, CREATEDATE, UPDATEDATE
			 ORDER BY USERNAME
		   '

		SET @SQL = 'update l set 
			SQLAgentOperatorRole = a.SQLAgentOperatorRole,
			SQLAgentReaderRole = a.SQLAgentReaderRole,
			SQLAgentUserRole = a.SQLAgentUserRole,
			db_ssisadmin = a.db_ssisadmin,
			db_ssisltduser = a.db_ssisltduser
		from logins l 
		join (SELECT a.* FROM OPENQUERY(['+@SERVER+'], 
				'''+replace(@sql, '''', '''''')+'''
				) a
			 ) a on a.USERNAME collate SQL_Latin1_General_CP1_CI_AS = l.LoginName collate SQL_Latin1_General_CP1_CI_AS
		where l.Serverid = '+@serverid 
		
		exec dbo.spExec @sql
		--print @sql
	end try
	begin catch
		print error_message()
		print @sql
	end catch
	FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid, @version , @EngineEdition
END
CLOSE T_CURSOR
DEALLOCATE T_CURSOR

GO
/****** Object:  StoredProcedure [dbo].[spLoadLongSql]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE proc [dbo].[spLoadLongSql] @serverid varchar(10)='0', @debug	bit	=0
as
if @serverid = '0'
	truncate table LongSql
else 
	delete from Errors where serverid = @serverid

if @debug=1
	SELECT LongQueryEvents, serverid, servername FROM vwServersVms s
	where isActive=1 
	and isnull(LongQueryEvents,'')<>''
	and (	s.ServerId = @serverid
			or (@serverid =0 and not exists(select * from LongSql d where s.ServerId =d.serverid))
		)

declare @sql nvarchar(max), @path VARCHAR(100), @servername varchar(100)
DECLARE T_CURSOR CURSOR FAST_FORWARD FOR
	SELECT LongQueryEvents, serverid, servername FROM vwServersVms s
	where isActive=1 
	and isnull(LongQueryEvents,'')<>''
	and (	s.ServerId = @serverid
			or (@serverid =0 and not exists(select * from LongSql d where s.ServerId =d.serverid))
		)
OPEN T_CURSOR
FETCH NEXT FROM T_CURSOR INTO @path, @serverid, @servername
WHILE @@FETCH_STATUS=0
BEGIN
	set @sql = 'exec(''alter EVENT SESSION [LongSql] ON SERVER state=stop'') at ['+@servername+']'
		exec spExec @sql,@debug,1,@debug

	set @sql='
		;WITH cte AS 
		( 
			SELECT 
				CAST(event_data AS XML) AS event_data 
			FROM 
				sys.fn_xe_file_target_read_file('''+@path+''', NULL, NULL, NULL) 
		), 
		cte2 AS 
		( 
			SELECT 
				event_number = ROW_NUMBER() OVER (ORDER BY T.x) 
			,    event_name = T.x.value(''@name'', ''varchar(100)'') 
			,    event_timestamp = T.x.value(''@timestamp'', ''datetimeoffset'') 
			,    event_data 
			FROM 
				cte    
			CROSS APPLY 
				event_data.nodes(''/event'') T(x) 
		), 
		cte3 AS 
		( 
			SELECT 
				c.event_number, 
				c.event_timestamp, 
				--data_field = T2.x.value(''local-name(.)'', ''varchar(100)''), 
				data_name = T2.x.value(''@name'', ''varchar(100)''), 
				data_value = T2.x.value(''value[1]'', ''varchar(max)''), 
				data_text = T2.x.value(''text[1]'', ''varchar(max)'') 
				--,    event_data 
			FROM 
				cte2 c 
			CROSS APPLY 
				c.event_data.nodes(''event/*'') T2(x) 
		), 
		cte4 AS 
		( 
			SELECT 
				* 
			FROM 
				cte3 
			WHERE 
				data_name IN (''cpu_time'', ''duration'', ''physical_reads'', ''logical_reads'', ''writes'', ''row_count'', ''batch_text'', ''client_app_name'', ''client_hostname'', ''database_name'', ''nt_username'', ''sql_text'') 
		) 
		SELECT
			'+@serverid+' serverid, event_timestamp,
			cpu_time, duration, physical_reads, logical_reads, writes, row_count, batch_text, client_app_name, client_hostname, database_name, nt_username, sql_text
		FROM 
			cte4 
		PIVOT 
			(MAX(data_value) FOR data_name IN (cpu_time, duration, physical_reads, logical_reads, writes, row_count, batch_text, client_app_name, client_hostname, database_name, nt_username, sql_text)) T
		WHERE batch_text IS NOT NULL
		and not exists (select * from LongSql d where d.serverid = '+@serverid+' and d.event_timestamp = t.event_timestamp)
		   '
	begin try
		insert into LongSql (ServerId,event_timestamp,cpu_time, duration, physical_reads, logical_reads, writes, row_count, batch_text, client_app_name, client_hostname, database_name, nt_username, sql_text)
		exec dbo.spExec @sql, @debug,1,@debug
	end try
	begin catch
		update servers set  Error = ERROR_MESSAGE() where serverid=@serverid
	end catch
	
	set @sql = 'exec(''alter EVENT SESSION [LongSql] ON SERVER state=start'') at ['+@servername+']'
	exec spExec @sql,@debug,1,@debug

	FETCH NEXT FROM T_CURSOR INTO @path, @serverid, @servername
END
CLOSE T_CURSOR
DEALLOCATE T_CURSOR

--select * from LongSql order by row_count desc

delete from LongSql 
where batch_text like 'EXEC %spx_Application_SetBlockedIPAddresses%' 
or batch_text like 'EXEC %SPJ_BackupDatabases%'
or batch_text like 'EXEC %SPJ_BackupTransactionLogs%'
or batch_text like 'BACKUP %' 
or batch_text like '%@BackupPath%'

GO
/****** Object:  StoredProcedure [dbo].[spLoadLongSqlFiles]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE   proc [dbo].[spLoadLongSqlFiles]   @serverid int=0 --@folders varchar(8000)='', @separator char(1)=','
as

declare @cmd varchar(8000), @LongQueryEvents varchar(255), @folder  varchar(255)

if @serverid = 0
	truncate table LongSqlFiles
else
	delete LongSqlFiles where serverid = @serverid

if object_id('tempdb..#dir') is not null
	drop table #dir

create table #dir (line varchar(8000), serverid int, rowid int identity)

declare t_cursor cursor fast_forward for
	select LongQueryEvents, serverid 
		from servers s
		where isActive=1 and version not like '%azure%'
		and isnull(LongQueryEvents,'') <> '' 
		and (	s.ServerId = @serverid
			or (@serverid =0 and not exists(select * from LongSqlFiles d where s.ServerId =d.serverid))
		)
open t_cursor
fetch next from t_cursor into @LongQueryEvents, @serverid
while @@FETCH_STATUS=0
begin

	set @cmd = 'dir '+@LongQueryEvents
	set @folder = replace(@LongQueryEvents , 'LongSql*.xel','')

	truncate table #dir
	insert into #dir (line)
	exec xp_cmdshell @cmd

	insert into LongSqlFiles (foldername,  filename, [date], size, serverid)
	select @folder
		, SUBSTRING(line, 40, 255) name
		, SUBSTRING(line, 1, 20)  [date]
		, replace(SUBSTRING(line, 21, 19),',','') size
		, @serverid
	from #dir d
	where line not like '%<DIR>%'
	and line not like '%File(s)%bytes'
	and line not like ' Directory Of %'
	and rowid>2
	and ISDATE(SUBSTRING(line, 1, 20) )=1
	and isnumeric(SUBSTRING(line, 21, 19))=1
	order by rowid

	fetch next from t_cursor into @LongQueryEvents, @serverid
end
close t_cursor
deallocate t_cursor
GO
/****** Object:  StoredProcedure [dbo].[spLoadMissingIndexes]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




CREATE proc [dbo].[spLoadMissingIndexes] @serverid varchar(10)='0'
as

/***********************
	MissingIndexes
************************/
if @serverid = '0'
	truncate table MissingIndexes

declare @sql nvarchar(max), @SERVER VARCHAR(100), @DatabaseName varchar(100), @DatabaseId varchar(10), @version varchar(255), @linkedserver varchar(255), @EngineEdition varchar(50)
DECLARE T_CURSOR CURSOR FAST_FORWARD FOR
	SELECT SERVERNAME, serverid, version, EngineEdition FROM SERVERS s
	where isActive=1 
	and (	s.ServerId = @serverid
			or (@serverid =0 and not exists(select * from MissingIndexes d where s.ServerId =d.serverid))
		)
OPEN T_CURSOR
FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid , @version, @EngineEdition
WHILE @@FETCH_STATUS=0
BEGIN
	declare d_cursor cursor fast_forward for
		select databaseid, databasename 
		from vwDatabasesOnLineNonSystem
		where serverid=@serverid 
	open d_cursor
	FETCH NEXT FROM d_CURSOR INTO @databaseid, @databasename
	while @@FETCH_STATUS=0
	begin
		set @sql='
		SELECT TOP ( 20 ) user_seeks * avg_total_user_cost * ( avg_user_impact * 0.01 ) AS [index_advantage]
			  , migs.last_user_seek
			  , mid.[statement] AS [TableName]
			  , mid.equality_columns
			  , mid.inequality_columns
			  , mid.included_columns
			  , migs.unique_compiles
			  , migs.user_seeks
			  , migs.avg_total_user_cost
			  , migs.avg_user_impact
		FROM    ['+@DatabaseName+'].sys.dm_db_missing_index_group_stats AS migs WITH ( NOLOCK )
				INNER JOIN ['+@DatabaseName+'].sys.dm_db_missing_index_groups AS mig WITH ( NOLOCK ) ON migs.group_handle = mig.index_group_handle
				INNER JOIN ['+@DatabaseName+'].sys.dm_db_missing_index_details AS mid WITH ( NOLOCK ) ON mig.index_handle = mid.index_handle
		WHERE   mid.database_id = DB_ID('''+@DatabaseName+''') -- Remove this to see for entire instance
		ORDER BY index_advantage DESC
		OPTION  ( RECOMPILE );
		'
		
		if @DatabaseName = 'master' or @EngineEdition <> 'SQL Azure'
			set @linkedserver = @server
		else 
			set @linkedserver = @server+'.'+@DatabaseName

		SET @SQL = 'SELECT '+@serverid+', '+@databaseid+', a.*
			FROM OPENQUERY(['+@linkedserver+'], 
			'''+replace(@sql, '''', '''''')+'''
			) AS a
			;'
		begin try
			insert into MissingIndexes (Serverid,DatabaseId,index_advantage,last_user_seek,TableName,equality_columns,inequality_columns,included_columns,unique_compiles,user_seeks,avg_total_user_cost,avg_user_impact)
			exec dbo.spExec @sql
		end try
		begin catch
			print error_message()
			print @sql
		end catch
		FETCH NEXT FROM d_CURSOR INTO @databaseid, @databasename
	end
	close d_cursor
	deallocate d_cursor
	FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid, @version, @EngineEdition
END
CLOSE T_CURSOR
DEALLOCATE T_CURSOR
GO
/****** Object:  StoredProcedure [dbo].[spLoadMsdb_Backups]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE proc [dbo].[spLoadMsdb_Backups] @serverid int = 0
as

/***********************************************
		Msdb_Backups
***********************************************/
if @serverid = 0
	truncate table Msdb_Backups
else 
	delete from Msdb_Backups where ServerId = @serverid

declare @sql nvarchar(max), @SERVER VARCHAR(100)
DECLARE T_CURSOR CURSOR FAST_FORWARD FOR
	SELECT SERVERNAME, serverid FROM vwServersVms s
	where isActive=1 
	and (	s.ServerId = @serverid
			or (@serverid =0 and not exists(select * from Msdb_Backups d where s.ServerId =d.serverid))
		)
OPEN T_CURSOR
FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid 
WHILE @@FETCH_STATUS=0
BEGIN
	set @sql='
		SELECT  distinct serverid = '+cast(@serverid as varchar)+',
				bs.database_name, 
				bs.backup_start_date, 
				bs.backup_finish_date, 
				bs.expiration_date, 
				bs.type, 
				sum(bs.backup_size) backup_size, 
				min(mf.logical_device_name) logical_device_name, 
				min(mf.physical_device_name) physical_device_name, 
				min(bs.name) backupset_name,  
				min(bs.description)  description
			FROM msdb.dbo.backupmediafamily mf
			INNER JOIN msdb.dbo.backupset bs ON mf.media_set_id = bs.media_set_id 
			WHERE (CONVERT(datetime, bs.backup_start_date, 102) >= GETDATE() - 30) 
			group by bs.database_name, 
				bs.backup_start_date, 
				bs.backup_finish_date, 
				bs.expiration_date, 
				bs.type
			
		   '
	SET @SQL = 'SELECT a.*	FROM OPENQUERY(['+@SERVER+'], 
		'''+replace(@sql, '''', '''''')+'''
		) AS a;'
	begin try
		insert into Msdb_Backups (ServerId,database_name,backup_start_date,backup_finish_date,expiration_date,backup_type,backup_size,logical_device_name,physical_device_name,backupset_name,description)
		exec dbo.spExec @sql
	end try
	begin catch
		print error_message()
		print @sql
	end catch
	FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid
END
CLOSE T_CURSOR
DEALLOCATE T_CURSOR

update b set databaseid = d.databaseid
from Msdb_Backups b
join databases d on b.serverid=d.serverid and b.database_name = d.databasename
where b.databaseid is null

GO
/****** Object:  StoredProcedure [dbo].[spLoadPerMon]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE proc [dbo].[spLoadPerMon] @serverid varchar(10)='0'
as

declare @sql nvarchar(max), @folder VARCHAR(255),  @file varchar(255), @rows int, @importFileId int, @today varchar(4)

if month(getdate()) < 10
	set @today = '0'+ cast(month(getdate()) as varchar)
else 
	set @today = cast(month(getdate()) as varchar)

if day(getdate()) < 10
	set @today = @today+'0'+ cast(day(getdate()) as varchar)
else 
	set @today = @today+cast(day(getdate()) as varchar)

if OBJECT_ID('tempdb..#dir') is not null
	drop table #dir

create table #dir (line varchar(255))

DECLARE T_CURSOR CURSOR FAST_FORWARD FOR
	SELECT perfMonLogs, serverid
	FROM vwServersVms s
	where isActive=1  
	and (	s.ServerId = @serverid
			or (@serverid =0 and not exists(select * from DatabaseObjects d where s.ServerId =d.serverid))
		)
	and isnull(perfMonLogs,'') <> '' 
OPEN T_CURSOR
FETCH NEXT FROM T_CURSOR INTO @folder, @serverid 
WHILE @@FETCH_STATUS=0
BEGIN
	truncate table #dir 

	set @sql = '
	insert into #dir
	exec xp_cmdshell ''dir "'+@folder+'"''
	'
	exec dbo.spExec @sql

	delete from #dir where line is null 
	delete from #dir where line not like '%.csv'

	update #dir set line = SUBSTRING (line, 40, 255)

	declare f_cursor cursor fast_forward for
		select line from #dir
	open f_cursor
	fetch next from f_cursor into @file
	while @@FETCH_STATUS = 0
	begin
		if not exists(select * from ImportFile where FileName= @file)
		and @file not like '%'+@today+'____.csv'
			--and not exists(select * from ImportError where FileName= @file and Message not like '%Operating system error code 32(The process cannot access the file because it is being used by another process%')
		begin try
			print @file	
			truncate table PerfMonStg
		
			set @sql = '	
			bulk insert Servers.dbo.PerfMonStg
			from '''+@folder+@file+'''
			with (FIRSTROW =2, FIELDTERMINATOR ='','')
			'
			exec dbo.spExec @sql
			set @rows = @@ROWCOUNT
				
			update PerfMonStg set 
				MetricDate = replace(MetricDate ,'"',''),	
				MemoryAvailableMBytes = replace(MemoryAvailableMBytes ,'"',''),
				PercentageProcessorTime = replace(PercentageProcessorTime ,'"',''),
				ForwardedRecordsPerSec = replace(ForwardedRecordsPerSec ,'"',''),
				FullScansPerSec = replace(FullScansPerSec ,'"',''),
				IndexSearchesPerSec = replace(IndexSearchesPerSec ,'"',''),
				PageLifeExpectancy = replace(PageLifeExpectancy ,'"',''),
				PageReadsPerSec = replace(PageReadsPerSec ,'"',''),
				PageWritesPerSec = replace(PageWritesPerSec ,'"',''),
				LazyWritesPerSec = replace(LazyWritesPerSec ,'"',''),
				C_AvgDiskBytesPerRead = replace(C_AvgDiskBytesPerRead ,'"',''),
				C_AvgDiskBytesPerWrite = replace(C_AvgDiskBytesPerWrite ,'"',''),
				C_AvgDiskQueueLength = replace(C_AvgDiskQueueLength ,'"',''),
				C_AvgDiskSecPerRead = replace(C_AvgDiskSecPerRead ,'"',''),
				C_AvgDiskSecPerWrite = replace(C_AvgDiskSecPerWrite ,'"',''),
				D_AvgDiskBytesPerRead = replace(D_AvgDiskBytesPerRead ,'"',''),
				D_AvgDiskBytesPerWrite = replace(D_AvgDiskBytesPerWrite ,'"',''),
				D_AvgDiskQueueLength = replace(D_AvgDiskQueueLength ,'"',''),
				D_AvgDiskSecPerRead = replace(D_AvgDiskSecPerRead ,'"',''),
				D_AvgDiskSecPerWrite = replace(D_AvgDiskSecPerWrite ,'"','')
			
			UPDATE PerfMOnStg set MemoryAvailableMBytes	=0 where MemoryAvailableMBytes = ''	
			UPDATE PerfMOnStg set PercentageProcessorTime	=0 where PercentageProcessorTime  = ''	
			UPDATE PerfMOnStg set ForwardedRecordsPerSec	=0 where ForwardedRecordsPerSec = ''	
			UPDATE PerfMOnStg set FullScansPerSec	=0 where FullScansPerSec = ''			
			UPDATE PerfMOnStg set IndexSearchesPerSec	=0 where IndexSearchesPerSec = ''		
			UPDATE PerfMOnStg set PageLifeExpectancy	=0 where PageLifeExpectancy  = ''	
			UPDATE PerfMOnStg set PageReadsPerSec	=0 where PageReadsPerSec = ''			
			UPDATE PerfMOnStg set PageWritesPerSec	=0 where PageWritesPerSec = ''			
			UPDATE PerfMOnStg set LazyWritesPerSec	=0 where LazyWritesPerSec = ''			
			UPDATE PerfMOnStg set C_AvgDiskBytesPerRead	=0 where C_AvgDiskBytesPerRead = ''				
			UPDATE PerfMOnStg set C_AvgDiskBytesPerWrite	=0 where C_AvgDiskBytesPerWrite = ''		
			UPDATE PerfMOnStg set C_AvgDiskQueueLength	=0 where C_AvgDiskQueueLength = ''		
			UPDATE PerfMOnStg set C_AvgDiskSecPerRead	=0 where C_AvgDiskSecPerRead = ''
			UPDATE PerfMOnStg set C_AvgDiskSecPerWrite	=0 where C_AvgDiskSecPerWrite = ''
			UPDATE PerfMOnStg set D_AvgDiskBytesPerRead	=0 where  D_AvgDiskBytesPerRead= ''
			UPDATE PerfMOnStg set D_AvgDiskBytesPerWrite	=0 where D_AvgDiskBytesPerWrite = ''
			UPDATE PerfMOnStg set D_AvgDiskQueueLength	=0 where D_AvgDiskQueueLength = ''
			UPDATE PerfMOnStg set D_AvgDiskSecPerRead	=0 where D_AvgDiskSecPerRead = ''
			UPDATE PerfMOnStg set D_AvgDiskSecPerWrite	=0 where D_AvgDiskSecPerWrite = ''

			insert into ImportFile (FileName,ImportDate,Rows)
			values (@file, GETDATE(), @rows)
		
			set @importFileId = SCOPE_IDENTITY()

			insert into PerfMon (ServerId, ImportFileId, MetricDate, MetricTime, MemoryAvailableMBytes, PercentageProcessorTime, ForwardedRecordsPerSec, FullScansPerSec, IndexSearchesPerSec, PageLifeExpectancy, PageReadsPerSec, PageWritesPerSec, LazyWritesPerSec, C_AvgDiskBytesPerRead, C_AvgDiskBytesPerWrite, C_AvgDiskQueueLength, C_AvgDiskSecPerRead, C_AvgDiskSecPerWrite, D_AvgDiskBytesPerRead, D_AvgDiskBytesPerWrite, D_AvgDiskQueueLength, D_AvgDiskSecPerRead, D_AvgDiskSecPerWrite)
			select @serverid, @importFileId,  cast(MetricDate as date), cast(MetricDate as time), MemoryAvailableMBytes, PercentageProcessorTime, ForwardedRecordsPerSec, FullScansPerSec, IndexSearchesPerSec, PageLifeExpectancy, PageReadsPerSec, PageWritesPerSec, LazyWritesPerSec, C_AvgDiskBytesPerRead, C_AvgDiskBytesPerWrite, C_AvgDiskQueueLength, C_AvgDiskSecPerRead, C_AvgDiskSecPerWrite, D_AvgDiskBytesPerRead, D_AvgDiskBytesPerWrite, D_AvgDiskQueueLength, D_AvgDiskSecPerRead, D_AvgDiskSecPerWrite
			from PerfMonStg

			--select COUNT(*) from CES_SAE where importFileId = @importFileId
		end try	
		begin catch
			declare @message varchar(255)
			set @message = ERROR_MESSAGE()
			--if @message='Error converting data type varchar to numeric.'
				--select * from PerfMonStg
			
			insert into ImportError (FileName,ImportDate,Message)
			values (@file, GETDATE(), @message)
			print @message
		end catch
	
		fetch next from f_cursor into @file
	end
	close f_cursor
	deallocate f_cursor

	FETCH NEXT FROM T_CURSOR INTO @folder, @serverid
END
CLOSE T_CURSOR
DEALLOCATE T_CURSOR





GO
/****** Object:  StoredProcedure [dbo].[spLoadPerMonApp]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE proc [dbo].[spLoadPerMonApp] @serverid varchar(10)='0'
as

declare @sql nvarchar(max), @folder VARCHAR(255),  @file varchar(255), @rows int, @importFileId int, @today varchar(4)

if month(getdate()) < 10
	set @today = '0'+ cast(month(getdate()) as varchar)
else 
	set @today = cast(month(getdate()) as varchar)

if day(getdate()) < 10
	set @today = @today+'0'+ cast(day(getdate()) as varchar)
else 
	set @today = @today+cast(day(getdate()) as varchar)


if OBJECT_ID('tempdb..#dir') is not null
	drop table #dir

create table #dir (line varchar(255))

DECLARE T_CURSOR CURSOR FAST_FORWARD FOR
	SELECT perfMonLogs, serverid FROM vwServersVms s
	where isActive=1  
	and (	s.ServerId = @serverid
			or (@serverid =0 and not exists(select * from DatabaseObjects d where s.ServerId =d.serverid))
		)
	and isnull(perfMonLogs,'') <> '' 
OPEN T_CURSOR
FETCH NEXT FROM T_CURSOR INTO @folder, @serverid 
WHILE @@FETCH_STATUS=0
BEGIN
	truncate table #dir 

	set @sql = '
	insert into #dir
	exec xp_cmdshell ''dir "'+@folder+'"''
	'
	exec dbo.spExec @sql


	delete from #dir where line is null 
	delete from #dir where line not like '%.csv'

	update #dir set line = SUBSTRING (line, 40, 255)

	declare f_cursor cursor fast_forward for
		select line from #dir
	open f_cursor
	fetch next from f_cursor into @file
	while @@FETCH_STATUS = 0
	begin
		if not exists(select * from ImportFile where FileName= @file)
			and @file not like '%'+@today+'____.csv'
			--and not exists(select * from ImportError where FileName= @file and Message not like '%Operating system error code 32(The process cannot access the file because it is being used by another process%')
		begin try
			print @file	
			truncate table PerfMonAppStg
		
			set @sql = '	
			bulk insert Servers.dbo.PerfMonAppStg
			from '''+@folder+@file+'''
			with (FIRSTROW =2, FIELDTERMINATOR ='','')
			'
			exec dbo.spExec @sql
			set @rows = @@ROWCOUNT
				
			update PerfMonAppStg set 
				MetricDate = replace(MetricDate ,'"',''),	
				TotalCommittedBytes = replace(TotalCommittedBytes ,'"',''),
				ApplicationRestarts = replace(ApplicationRestarts ,'"',''),
				RequestWaitTime = replace(RequestWaitTime ,'"',''),
				RequestsQueued = replace(RequestsQueued ,'"',''),
				RequestsPerSec = replace(RequestsPerSec ,'"',''),
				C_PencentageDiskTime = replace(C_PencentageDiskTime ,'"',''),
				D_PercentageDiskTime = replace(D_PercentageDiskTime ,'"',''),
				MemoryAvailableMBytes = replace(MemoryAvailableMBytes ,'"',''),
				MemoryPagesPerSec = replace(MemoryPagesPerSec ,'"',''),
				PhisicalPercentageDiskTime = replace(PhisicalPercentageDiskTime ,'"',''),
				ProcessorQueueLength = replace(ProcessorQueueLength ,'"',''),
				PostRequestsPerSec = replace(PostRequestsPerSec ,'"',''),
				CurrentConnections = replace(CurrentConnections ,'"',''),
				NetworkBytesTotalPerSec = replace(NetworkBytesTotalPerSec ,'"',''),
				PercentageProcessorTime = replace(PercentageProcessorTime ,'"','')
			
			UPDATE PerfMOnAppStg set TotalCommittedBytes	=0 where TotalCommittedBytes = ''	
			UPDATE PerfMOnAppStg set ApplicationRestarts	=0 where ApplicationRestarts  = ''	
			UPDATE PerfMOnAppStg set RequestWaitTime	=0 where RequestWaitTime = ''	
			UPDATE PerfMOnAppStg set RequestsQueued	=0 where RequestsQueued = ''			
			UPDATE PerfMOnAppStg set RequestsPerSec	=0 where RequestsPerSec = ''		
			UPDATE PerfMOnAppStg set C_PencentageDiskTime	=0 where C_PencentageDiskTime  = ''	
			UPDATE PerfMOnAppStg set D_PercentageDiskTime	=0 where D_PercentageDiskTime = ''			
			UPDATE PerfMOnAppStg set MemoryAvailableMBytes	=0 where MemoryAvailableMBytes = ''			
			UPDATE PerfMOnAppStg set MemoryPagesPerSec	=0 where MemoryPagesPerSec = ''				
			UPDATE PerfMOnAppStg set PhisicalPercentageDiskTime	=0 where PhisicalPercentageDiskTime = ''		
			UPDATE PerfMOnAppStg set ProcessorQueueLength	=0 where ProcessorQueueLength = ''		
			UPDATE PerfMOnAppStg set PostRequestsPerSec	=0 where PostRequestsPerSec = ''
			UPDATE PerfMOnAppStg set CurrentConnections	=0 where CurrentConnections = ''
			UPDATE PerfMOnAppStg set NetworkBytesTotalPerSec	=0 where  NetworkBytesTotalPerSec= ''
			UPDATE PerfMOnAppStg set PercentageProcessorTime	=0 where PercentageProcessorTime = ''

			insert into ImportFile (FileName,ImportDate,Rows)
			values (@file, GETDATE(), @rows)
		
			set @importFileId = SCOPE_IDENTITY()

			insert into PerfMonApp (ServerId, ImportFileId, MetricDate, MetricTime, TotalCommittedBytes,ApplicationRestarts,RequestWaitTime,RequestsQueued,RequestsPerSec,C_PencentageDiskTime,D_PercentageDiskTime,MemoryAvailableMBytes,MemoryPagesPerSec,PhisicalPercentageDiskTime,ProcessorQueueLength,PostRequestsPerSec,CurrentConnections,NetworkBytesTotalPerSec,PercentageProcessorTime)
			select @serverid, @importFileId,  cast(MetricDate as date), cast(MetricDate as time), TotalCommittedBytes,ApplicationRestarts,RequestWaitTime,RequestsQueued,RequestsPerSec,C_PencentageDiskTime,D_PercentageDiskTime,MemoryAvailableMBytes,MemoryPagesPerSec,PhisicalPercentageDiskTime,ProcessorQueueLength,PostRequestsPerSec,CurrentConnections,NetworkBytesTotalPerSec,PercentageProcessorTime
			from PerfMonAppStg

			--select COUNT(*) from CES_SAE where importFileId = @importFileId
		end try	
		begin catch
			declare @message varchar(255)
			set @message = ERROR_MESSAGE()
			--if @message='Error converting data type varchar to numeric.'
			--	select * from PerfMonStg

			insert into ImportError (FileName,ImportDate,Message)
			values (@file, GETDATE(), @message)
		end catch
	
		fetch next from f_cursor into @file
	end
	close f_cursor
	deallocate f_cursor

	FETCH NEXT FROM T_CURSOR INTO @folder, @serverid
END
CLOSE T_CURSOR
DEALLOCATE T_CURSOR






GO
/****** Object:  StoredProcedure [dbo].[spLoadPublications]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE proc [dbo].[spLoadPublications] @serverid int=0
as
declare @sql nvarchar(max), @SERVER VARCHAR(100)
	, @publisherid int, @publisher varchar(100), @Distribution_db varchar(100)

--truncate table Publications

DECLARE server_cursor CURSOR FAST_FORWARD FOR
	SELECT SERVERNAME, serverid FROM vwServersNonAzureSql s
	where isActive=1  
	and (	s.ServerId = @serverid
			or (@serverid =0 and not exists(select * from Publications d where s.ServerId =d.serverid))
		)
OPEN server_cursor
FETCH NEXT FROM server_cursor INTO @SERVER, @serverid 
WHILE @@FETCH_STATUS=0
BEGIN
	declare publisher_cursor cursor fast_forward for
		select  publisherid, p.PublisherName, p.distribution_db from [Publishers] p where Active=1 and Serverid = @serverid
	open publisher_cursor
	FETCH NEXT FROM publisher_cursor INTO @publisherid, @publisher, @Distribution_db
	while @@FETCH_STATUS=0
	begin
		begin try

			set @sql='
			select PublisherId = '+cast(@publisherid as varchar)+', serverId= '+cast(@serverid as varchar)+'
				,publisher_db,publication,publication_type,thirdparty_flag,independent_agent,immediate_sync,allow_push,allow_pull,allow_anonymous,description,vendor_name,retention,sync_method,allow_subscription_copy,thirdparty_options,allow_queued_tran,options,retention_period_unit,allow_initialize_from_backup, publication_id
			from ['+@Distribution_db+'].[dbo].[MSpublications]'

			SET @SQL = 'SELECT a.*	FROM OPENQUERY(['+@SERVER+'], 
			'''+replace(@sql, '''', '''''')+'''
			) AS a;'

			insert into Publications (PublisherId,serverId,publisher_db,publication,publication_type,thirdparty_flag,independent_agent,immediate_sync,allow_push,allow_pull,allow_anonymous,description,vendor_name,retention,sync_method,allow_subscription_copy,thirdparty_options,allow_queued_tran,options,retention_period_unit,allow_initialize_from_backup, remote_publication_id )
			exec dbo.spExec @sql
		end try
		begin catch
			print error_message()
			print @sql
		end catch
		FETCH NEXT FROM publisher_cursor INTO @publisherid, @publisher, @Distribution_db
	end
	close publisher_cursor
	deallocate publisher_cursor
	FETCH NEXT FROM server_cursor INTO @SERVER, @serverid
END
CLOSE server_cursor
DEALLOCATE server_cursor




GO
/****** Object:  StoredProcedure [dbo].[spLoadPublishers]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE proc [dbo].[spLoadPublishers] @serverid varchar(10) = '0'
as
declare @sql nvarchar(max), @SERVER VARCHAR(100) 

--truncate table Publishers

DECLARE server_cursor CURSOR FAST_FORWARD FOR
	SELECT SERVERNAME, serverid 
	FROM vwServersNonAzureSql s
	where isActive=1 
	and (	s.ServerId = @serverid
			or (@serverid =0 and not exists(select * from Publishers d where s.ServerId =d.serverid))
		)
	and exists (select * from vwDatabases do where do.DatabaseName like 'distribution%' and do.ServerId = s.ServerId )
OPEN server_cursor
FETCH NEXT FROM server_cursor INTO @SERVER, @serverid 
WHILE @@FETCH_STATUS=0
BEGIN
		
	begin try

		set @SQL = 'select serverid = '+cast(@serverid as varchar)+', name, distribution_db, working_directory, active, publisher_type from msdb.[dbo].[MSdistpublishers]'
		
		SET @SQL = 'SELECT a.*	FROM OPENQUERY(['+@SERVER+'], 
		'''+replace(@sql, '''', '''''')+'''
		) AS a;'

		insert into Publishers ([Serverid], PublisherName, distribution_db, working_directory, active, publisher_type)
		exec dbo.spExec @sql
		
	end try
	begin catch
		--print error_message()
		--print @sql
	end catch
	FETCH NEXT FROM server_cursor INTO @SERVER, @serverid
END
CLOSE server_cursor
DEALLOCATE server_cursor


GO
/****** Object:  StoredProcedure [dbo].[spLoadQueryStoreLongestAvgTimes]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO





CREATE   proc [dbo].[spLoadQueryStoreLongestAvgTimes] @serverid varchar(10)='0'
as

declare @linkedserver varchar(255)

/***********************
	QueryStoreLongestAvgTimes
************************/
if @serverid = '0' 
	truncate table QueryStoreLongestAvgTimes

declare @sql nvarchar(max), @SERVER VARCHAR(100), @DatabaseName varchar(100), @DatabaseId varchar(10), @version varchar(255), @EngineEdition varchar(50)

DECLARE T_CURSOR CURSOR FAST_FORWARD FOR
	SELECT SERVERNAME, serverid, version, EngineEdition FROM SERVERS s
	where isActive=1 
	and (	s.ServerId = @serverid
			or (@serverid =0 and not exists(select * from QueryStoreLongestAvgTimes d where s.ServerId =d.serverid))
		)
		order by serverid
OPEN T_CURSOR
FETCH NEXT FROM T_CURSOR INTO @server, @serverid, @version, @EngineEdition
WHILE @@FETCH_STATUS=0
BEGIN
	--print @server
	declare d_cursor cursor fast_forward for
		select  databaseid, databasename 
		from vwDatabasesOnLine 
		where is_query_store_on = 1
		and serverid=@serverid 
		
	open d_cursor
	FETCH NEXT FROM d_CURSOR INTO @databaseid, @databasename
	while @@FETCH_STATUS=0
	begin
		--print @databasename
		set @sql='
			SELECT TOP 10 rs.avg_duration, qt.query_sql_text, q.query_id,
				qt.query_text_id, p.plan_id
			FROM ['+@DatabaseName+'].sys.query_store_query_text AS qt
			JOIN ['+@DatabaseName+'].sys.query_store_query AS q ON qt.query_text_id = q.query_text_id
			JOIN ['+@DatabaseName+'].sys.query_store_plan AS p ON q.query_id = p.query_id
			JOIN ['+@DatabaseName+'].sys.query_store_runtime_stats AS rs ON p.plan_id = rs.plan_id
			WHERE rs.last_execution_time > DATEADD(hour, -24, GETUTCDATE())
			ORDER BY rs.avg_duration DESC;
	
			'
		if @DatabaseName = 'master' or @EngineEdition <> 'SQL Azure'
			set @linkedserver = @server
		else 
			set @linkedserver = @server+'.'+@DatabaseName

		SET @SQL = 'SELECT '+@serverid+', '+@databaseid+', a.*
			FROM OPENQUERY(['+@linkedserver+'], 
			'''+replace(@sql, '''', '''''')+'''
			) AS a
			;'
		begin try
			--print @sql
			insert into QueryStoreLongestAvgTimes (Serverid,DatabaseId,avg_duration,query_sql_text,query_id,query_text_id,plan_id)
			exec (@sql)
		end try
		begin catch
			print @sql
			print error_message()
		end catch
		FETCH NEXT FROM d_CURSOR INTO @databaseid, @databasename
	end
	close d_cursor
	deallocate d_cursor

	FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid, @version, @EngineEdition

END
CLOSE T_CURSOR
DEALLOCATE T_CURSOR
GO
/****** Object:  StoredProcedure [dbo].[spLoadQueryStoreTopExecCounts]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO





CREATE   proc [dbo].[spLoadQueryStoreTopExecCounts] @serverid varchar(10)='0'
as

declare @linkedserver varchar(255)

/***********************
	QueryStoreTopExecCounts
************************/
if @serverid = '0' 
	truncate table QueryStoreTopExecCounts

declare @sql nvarchar(max), @SERVER VARCHAR(100), @DatabaseName varchar(100), @DatabaseId varchar(10), @version varchar(255), @EngineEdition varchar(50)

DECLARE T_CURSOR CURSOR FAST_FORWARD FOR
	SELECT SERVERNAME, serverid, version, EngineEdition FROM SERVERS s
	where isActive=1 
	and (	s.ServerId = @serverid
			or (@serverid =0 and not exists(select * from QueryStoreTopExecCounts d where s.ServerId =d.serverid))
		)
		order by serverid
OPEN T_CURSOR
FETCH NEXT FROM T_CURSOR INTO @server, @serverid, @version, @EngineEdition
WHILE @@FETCH_STATUS=0
BEGIN
	--print @server
	declare d_cursor cursor fast_forward for
		select  databaseid, databasename 
		from vwDatabasesOnLine 
		where is_query_store_on = 1
		and serverid=@serverid 
		
	open d_cursor
	FETCH NEXT FROM d_CURSOR INTO @databaseid, @databasename
	while @@FETCH_STATUS=0
	begin
		--print @databasename
		set @sql='
			SELECT top 10 q.query_id, qt.query_text_id, qt.query_sql_text,
				SUM(rs.count_executions) AS total_execution_count
			FROM ['+@DatabaseName+'].sys.query_store_query_text AS qt
			JOIN ['+@DatabaseName+'].sys.query_store_query AS q ON qt.query_text_id = q.query_text_id
			JOIN ['+@DatabaseName+'].sys.query_store_plan AS p ON q.query_id = p.query_id
			JOIN ['+@DatabaseName+'].sys.query_store_runtime_stats AS rs ON p.plan_id = rs.plan_id
			GROUP BY q.query_id, qt.query_text_id, qt.query_sql_text
			ORDER BY total_execution_count DESC;
			'
		if @DatabaseName = 'master' or @EngineEdition <> 'SQL Azure'
			set @linkedserver = @server
		else 
			set @linkedserver = @server+'.'+@DatabaseName

		SET @SQL = 'SELECT '+@serverid+', '+@databaseid+', a.*
			FROM OPENQUERY(['+@linkedserver+'], 
			'''+replace(@sql, '''', '''''')+'''
			) AS a
			;'
		begin try
			--print @sql
			insert into QueryStoreTopExecCounts (Serverid,DatabaseId,query_id,query_text_id,query_sql_text,total_execution_count)
			exec (@sql)
		end try
		begin catch
			print @sql
			print error_message()
		end catch
		FETCH NEXT FROM d_CURSOR INTO @databaseid, @databasename
	end
	close d_cursor
	deallocate d_cursor

	FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid, @version, @EngineEdition

END
CLOSE T_CURSOR
DEALLOCATE T_CURSOR
GO
/****** Object:  StoredProcedure [dbo].[spLoadRplDates]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE proc [dbo].[spLoadRplDates]
as
truncate table RplDates

declare @sql nvarchar(max)
	, @SERVER VARCHAR(100)
	, @serverid varchar(10)=0
	, @DatabaseName varchar(100)
	, @DatabaseId varchar(10)
	, @Version	varchar	(255), @linkedserver varchar(255), @EngineEdition varchar(50)

DECLARE T_CURSOR CURSOR FAST_FORWARD FOR
	SELECT SERVERNAME, serverid, Version, EngineEdition FROM SERVERS s
	where isActive=1 
	and (	s.ServerId = @serverid
			or (@serverid =0 and not exists(select * from RplDates d where s.ServerId =d.serverid))
		)
OPEN T_CURSOR
FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid , @Version, @EngineEdition
WHILE @@FETCH_STATUS=0
BEGIN
	--print @SERVER
	declare d_cursor cursor fast_forward for
		select  databaseid, databasename 
		from vwDatabasesOnLineNonSystem 
		where GSync_Subscribed_Tables > 0
		and serverid=@serverid 
	open d_cursor
	FETCH NEXT FROM d_CURSOR INTO @databaseid, @databasename
	while @@FETCH_STATUS=0
	begin
		if @DatabaseName = 'master' or @EngineEdition <> 'SQL Azure' 
			set @linkedserver = @server
		else 
			set @linkedserver = @server+'.'+@DatabaseName
		
		SET @SQL = 'SELECT '+@serverid+', '+@DatabaseId+', *
				FROM OPENQUERY(['+@linkedserver+'], 
				''select Date from ['+@databasename+'].rpl.DatesFromSubscription_1'') AS a
				;'
				--print @databasename
		begin try
			insert into RplDates (Serverid,DatabaseId,Date)
			exec dbo.spExec @sql
		end try
		begin catch
			print error_message()
			print @sql
		end catch
		FETCH NEXT FROM d_CURSOR INTO @databaseid, @databasename
	end
	close d_cursor
	deallocate d_cursor
	FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid, @Version, @EngineEdition
END
CLOSE T_CURSOR
DEALLOCATE T_CURSOR



GO
/****** Object:  StoredProcedure [dbo].[spLoadRplImportLog]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE proc [dbo].[spLoadRplImportLog] @serverid varchar(10)='0'
as

declare @sql nvarchar(max)
	, @SERVER VARCHAR(100)
	, @DatabaseName varchar(100)
	, @DatabaseId varchar(10)
	, @ImportLogId int
	, @Version	varchar	(255), @linkedserver varchar(255), @EngineEdition varchar(50)

DECLARE T_CURSOR CURSOR FAST_FORWARD FOR
	SELECT SERVERNAME, serverid, Version, EngineEdition  FROM SERVERS s
	where isActive=1 
	and (	s.ServerId = @serverid
			or (@serverid =0 and not exists(select * from RplImportLog d where s.ServerId =d.serverid))
		)
OPEN T_CURSOR
FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid , @Version, @EngineEdition
WHILE @@FETCH_STATUS=0
BEGIN
	declare d_cursor cursor fast_forward for
		select  databaseid
			, databasename
			, isnull(ImportLogId, 0) ImportLogId
		from vwDatabasesOnLineNonSystem d
		outer apply (select max(ImportLogId) ImportLogId 
			from RplImportLog l 
			where l.databaseid = d.DatabaseId
			) l
		where GSync_Subscribed_Tables > 0
		and serverid=@serverid 
	open d_cursor
	FETCH NEXT FROM d_CURSOR INTO @databaseid, @databasename, @ImportLogId
	while @@FETCH_STATUS=0
	begin
		if @DatabaseName = 'master' or @EngineEdition <> 'SQL Azure'
			set @linkedserver = @server
		else 
			set @linkedserver = @server+'.'+@DatabaseName
		SET @SQL = 'SELECT '+@serverid+', '+@DatabaseId+', *
				FROM OPENQUERY(['+@linkedserver+'], 
				''select ImportLogId,SubscriptionId,RvFrom,RvTo,StartDate,EndDate,Success,TotalRows,RvTotalRows,Threads,UseStage,message, totalKbs from ['+@databasename+'].rpl.ImportLog where ImportLogId > '+cast(@ImportLogId as varchar)+' '') AS a
				;'
		begin try
			--print @SQL
			insert into RplImportLog (Serverid,DatabaseId,ImportLogId,SubscriptionId,RvFrom,RvTo,StartDate,EndDate,Success,TotalRows,RvTotalRows,Threads,UseStage,message, totalKbs)
			exec dbo.spExec @sql
		end try
		begin catch
			print error_message()
			print @sql
		end catch
		FETCH NEXT FROM d_CURSOR INTO @databaseid, @databasename, @ImportLogId
	end
	close d_cursor
	deallocate d_cursor
	FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid, @Version, @EngineEdition
END
CLOSE T_CURSOR
DEALLOCATE T_CURSOR

update t set RplSubscriptionRowId =  (select top 1 rowid from [RplSubscription] s
where s.databaseid = t.databaseid and s.SubscriptionId = t.SubscriptionId)
from [RplImportLog] t
where RplSubscriptionRowId is null





GO
/****** Object:  StoredProcedure [dbo].[spLoadRplImportLogDetail]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



CREATE proc [dbo].[spLoadRplImportLogDetail] @serverid varchar(10)= '0'
as

declare @sql nvarchar(max)
	, @SERVER VARCHAR(100)
	, @DatabaseName varchar(100)
	, @DatabaseId varchar(10)
	, @ImportLogDetailId int
	, @Version	varchar	(255), @linkedserver varchar(255), @EngineEdition varchar(50)

DECLARE T_CURSOR CURSOR FAST_FORWARD FOR
	SELECT SERVERNAME, serverid, Version, EngineEdition  FROM SERVERS s
	where isActive=1 
	and (	s.ServerId = @serverid
			or (@serverid =0 and not exists(select * from RplImportLogDetail d where s.ServerId =d.serverid))
		)
OPEN T_CURSOR
FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid , @Version, @EngineEdition
WHILE @@FETCH_STATUS=0
BEGIN
	declare d_cursor cursor fast_forward for
		select  databaseid, databasename , isnull(ImportLogDetailId,0) ImportLogDetailId
		from vwDatabasesOnLineNonSystem d
		outer apply (select max(ImportLogDetailId) ImportLogDetailId 
			from RplImportLogDetail l 
			where l.databaseid = d.DatabaseId
			) l
		where GSync_Subscribed_Tables > 0
		and serverid=@serverid 
	open d_cursor
	FETCH NEXT FROM d_CURSOR INTO @databaseid, @databasename, @ImportLogDetailId
	while @@FETCH_STATUS=0
	begin
		if @DatabaseName = 'master' or @EngineEdition <> 'SQL Azure'
			set @linkedserver = @server
		else 
			set @linkedserver = @server+'.'+@DatabaseName
		SET @SQL = 'SELECT '+@serverid+', '+@DatabaseId+', *
				FROM OPENQUERY(['+@linkedserver+'], 
				''select  ImportLogDetailId, ImportLogId, SchemaName, TableName, TotalRows, totalKbs from ['+@databasename+'].rpl.ImportLogDetail where totalRows>0 and ImportLogDetailId > '+cast(@ImportLogDetailId as varchar)+' '') AS a
				;'
		begin try
			insert into RplImportLogDetail (Serverid,DatabaseId,ImportLogDetailId, ImportLogId, SchemaName, TableName, TotalRows, totalKbs)
			exec dbo.spExec @sql
		end try
		begin catch
			print error_message()
			print @sql
		end catch
		FETCH NEXT FROM d_CURSOR INTO @databaseid, @databasename, @ImportLogDetailId
	end
	close d_cursor
	deallocate d_cursor
	FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid, @Version, @EngineEdition
END
CLOSE T_CURSOR
DEALLOCATE T_CURSOR

update t set RplImportLogRowId =  (select top 1 rowid from RplImportLog s
where s.databaseid = t.databaseid and s.ImportLogId = t.ImportLogId)
from [RplImportLogDetail] t
where RplImportLogRowId is null
GO
/****** Object:  StoredProcedure [dbo].[spLoadRplPublicationTable]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




CREATE proc [dbo].[spLoadRplPublicationTable] @serverid varchar(10)= '0'
as
if @serverid = '0'
	truncate table RplPublicationTable

declare @sql nvarchar(max)
	, @SERVER VARCHAR(100)
	, @DatabaseName varchar(100)
	, @DatabaseId varchar(10)
	, @Version	varchar	(255), @linkedserver varchar(255), @EngineEdition varchar(50)

DECLARE T_CURSOR CURSOR FAST_FORWARD FOR
	SELECT SERVERNAME, serverid, Version, EngineEdition  FROM SERVERS s
	where isActive=1 
	and (	s.ServerId = @serverid
			or (@serverid =0 and not exists(select * from RplPublicationTable d where s.ServerId =d.serverid))
		)
OPEN T_CURSOR
FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid , @Version, @EngineEdition
WHILE @@FETCH_STATUS=0
BEGIN
	declare d_cursor cursor fast_forward for
		select  databaseid, databasename 
		from vwDatabasesOnLineNonSystem d
		where exists(select * from DatabaseObjects do where do.DatabaseId = d.DatabaseId and do.ObjectName='PublicationTable' and do.SchemaName='rpl')
		and serverid=@serverid 
	open d_cursor
	FETCH NEXT FROM d_CURSOR INTO @databaseid, @databasename
	while @@FETCH_STATUS=0
	begin
		if @DatabaseName = 'master' or @EngineEdition <> 'SQL Azure'
			set @linkedserver = @server
		else 
			set @linkedserver = @server+'.'+@DatabaseName
		SET @SQL = 'SELECT '+@serverid+', '+@DatabaseId+', *
				FROM OPENQUERY(['+@linkedserver+'], 
				''select TableId,SchemaName,TableName,PkName,KeyCount,has_identity from ['+@databasename+'].rpl.PublicationTable'') AS a;'
		begin try
			insert into RplPublicationTable (Serverid,DatabaseId,TableId,SchemaName,TableName,PkName,KeyCount,has_identity)
			exec dbo.spExec @sql
		end try
		begin catch
			print error_message()
			print @sql
		end catch
		FETCH NEXT FROM d_CURSOR INTO @databaseid, @databasename
	end
	close d_cursor
	deallocate d_cursor
	FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid, @Version, @EngineEdition
END
CLOSE T_CURSOR
DEALLOCATE T_CURSOR



GO
/****** Object:  StoredProcedure [dbo].[spLoadRplSubscription]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE proc [dbo].[spLoadRplSubscription] @serverid varchar(10)= '0'
as
if @serverid = '0'
begin	
	delete from RplImportLogDetail
	delete from RplImportLog
	delete from RplSubscriptionTable
	delete from RplSubscription
end

declare @sql nvarchar(max)
	, @SERVER VARCHAR(100)
	, @DatabaseName varchar(100)
	, @DatabaseId varchar(10)
	, @Version	varchar	(255), @linkedserver varchar(255), @EngineEdition varchar(50)

DECLARE T_CURSOR CURSOR FAST_FORWARD FOR
	SELECT SERVERNAME, serverid, Version, EngineEdition FROM SERVERS s
	where isActive=1 
	and (	s.ServerId = @serverid
			or (@serverid =0 and not exists(select * from RplSubscription d where s.ServerId =d.serverid))
		)
OPEN T_CURSOR
FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid , @Version, @EngineEdition
WHILE @@FETCH_STATUS=0
BEGIN

	declare d_cursor cursor fast_forward for
		select  databaseid, databasename 
		from vwDatabasesOnLineNonSystem 
		where GSync_Subscribed_Tables > 0
		and serverid=@serverid 
	open d_cursor
	FETCH NEXT FROM d_CURSOR INTO @databaseid, @databasename
	while @@FETCH_STATUS=0
	begin
		if @DatabaseName = 'master' or @EngineEdition <> 'SQL Azure'
			set @linkedserver = @server
		else 
			set @linkedserver = @server+'.'+@DatabaseName

		SET @SQL = 'SELECT '+@serverid+', '+@DatabaseId+', *
				FROM OPENQUERY(['+@linkedserver+'], 
				''select SubscriptionId,ServerName,DatabaseName,IsActive,FrequencyInMinutes,Initialize, SubscriptionName, PriorityGroup, Login, Pass, DoubleReadRVRange, DelayAlertInMinutes, SubscriptionSequence from ['+@databasename+'].rpl.Subscription'') AS a;'
		begin try
			insert into RplSubscription (Serverid,DatabaseId,SubscriptionId,ServerName,DatabaseName,IsActive,FrequencyInMinutes,Initialize, SubscriptionName, PriorityGroup, Login, Pass, DoubleReadRVRange, DelayAlertInMinutes, SubscriptionSequence)
			exec dbo.spExec @sql
		end try
		begin catch
			print error_message()
			print @sql
		end catch
		FETCH NEXT FROM d_CURSOR INTO @databaseid, @databasename
	end
	close d_cursor
	deallocate d_cursor
	FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid, @Version, @EngineEdition
END
CLOSE T_CURSOR
DEALLOCATE T_CURSOR
GO
/****** Object:  StoredProcedure [dbo].[spLoadRplSubscriptionRoutine]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE proc [dbo].[spLoadRplSubscriptionRoutine] @serverid varchar(10)= '0'
as
if @serverid = '0'
	truncate table RplSubscriptionRoutine

declare @sql nvarchar(max)
	, @SERVER VARCHAR(100)
	, @DatabaseName varchar(100)
	, @DatabaseId varchar(10)
	, @Version	varchar	(255), @linkedserver varchar(255), @EngineEdition varchar(50)

DECLARE T_CURSOR CURSOR FAST_FORWARD FOR
	SELECT SERVERNAME, serverid, Version, EngineEdition  FROM SERVERS s
	where isActive=1 
	and (	s.ServerId = @serverid
			or (@serverid =0 and not exists(select * from RplSubscriptionRoutine d where s.ServerId =d.serverid))
		)
OPEN T_CURSOR
FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid , @Version, @EngineEdition
WHILE @@FETCH_STATUS=0
BEGIN
	declare d_cursor cursor fast_forward for
		select  databaseid, databasename 
		from vwDatabasesOnLineNonSystem 
		where GSync_Subscribed_Tables > 0
		and serverid=@serverid 
	open d_cursor
	FETCH NEXT FROM d_CURSOR INTO @databaseid, @databasename
	while @@FETCH_STATUS=0
	begin
		if @DatabaseName = 'master' or @EngineEdition <> 'SQL Azure'
			set @linkedserver = @server
		else 
			set @linkedserver = @server+'.'+@DatabaseName
		SET @SQL = 'SELECT '+@serverid+', '+@DatabaseId+', *
				FROM OPENQUERY(['+@linkedserver+'], 
				''select RoutineId,SubscriptionId,RoutineName,IsActive,RoutineSequence  from ['+@databasename+'].rpl.SubscriptionRoutine'') AS a;'
		begin try
			insert into RplSubscriptionRoutine (Serverid,DatabaseId,RoutineId,SubscriptionId,RoutineName,IsActive,RoutineSequence)
			exec dbo.spExec @sql
		end try
		begin catch
			print error_message()
			print @sql
		end catch
		FETCH NEXT FROM d_CURSOR INTO @databaseid, @databasename
	end
	close d_cursor
	deallocate d_cursor
	FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid, @Version, @EngineEdition
END
CLOSE T_CURSOR
DEALLOCATE T_CURSOR

update t set RplSubscriptionRowId =  (select top 1 rowid from [RplSubscription] s
where s.databaseid = t.databaseid and s.SubscriptionId = t.SubscriptionId)
from [RplSubscriptionRoutine] t


GO
/****** Object:  StoredProcedure [dbo].[spLoadRplSubscriptionTable]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE proc [dbo].[spLoadRplSubscriptionTable] @serverid varchar(10)= '0'
as
if @serverid = '0'
	truncate table RplSubscriptionTable

declare @sql nvarchar(max)
	, @SERVER VARCHAR(100)
	, @DatabaseName varchar(100)
	, @DatabaseId varchar(10)
	, @Version	varchar	(255), @linkedserver varchar(255), @EngineEdition varchar(50)

DECLARE T_CURSOR CURSOR FAST_FORWARD FOR
	SELECT SERVERNAME, serverid, Version, EngineEdition  FROM SERVERS s
	where isActive=1 
	and (	s.ServerId = @serverid
			or (@serverid =0 and not exists(select * from RplSubscriptionTable d where s.ServerId =d.serverid))
		)
OPEN T_CURSOR
FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid , @Version, @EngineEdition
WHILE @@FETCH_STATUS=0
BEGIN
	declare d_cursor cursor fast_forward for
		select  databaseid, databasename 
		from vwDatabasesOnLineNonSystem 
		where Gsync_Subscribed_Tables > 0
		and serverid=@serverid 
	open d_cursor
	FETCH NEXT FROM d_CURSOR INTO @databaseid, @databasename
	while @@FETCH_STATUS=0
	begin
		if @DatabaseName = 'master' or @EngineEdition <> 'SQL Azure'
			set @linkedserver = @server
		else 
			set @linkedserver = @server+'.'+@DatabaseName
		SET @SQL = 'SELECT '+@serverid+', '+@DatabaseId+', *
				FROM OPENQUERY(['+@linkedserver+'], 
				''select TableId,SubscriptionId,SchemaName,TableName,PublisherSchemaName,PublisherTableName,IsActive,PkName,KeyCount,has_identity,Initialize, InitialRowCount, IsCustom,GetProcName, ExcludeFromChecks  from ['+@databasename+'].rpl.SubscriptionTable'') AS a;'
		begin try
			insert into RplSubscriptionTable (Serverid,DatabaseId,TableId,SubscriptionId,SchemaName,TableName,PublisherSchemaName,PublisherTableName,IsActive,PkName,KeyCount,has_identity,Initialize, InitialRowCount, IsCustom,GetProcName, ExcludeFromChecks)
			exec dbo.spExec @sql
		end try
		begin catch
			print error_message()
			print @sql
		end catch
		FETCH NEXT FROM d_CURSOR INTO @databaseid, @databasename
	end
	close d_cursor
	deallocate d_cursor
	FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid, @Version, @EngineEdition
END
CLOSE T_CURSOR
DEALLOCATE T_CURSOR

update t set RplSubscriptionRowId =  (select top 1 rowid from [RplSubscription] s
where s.databaseid = t.databaseid and s.SubscriptionId = t.SubscriptionId)
from [RplSubscriptionTable] t

GO
/****** Object:  StoredProcedure [dbo].[spLoadSequences]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE proc [dbo].[spLoadSequences] @serverid int=0
as
set nocount on
truncate table Sequences

declare @Server varchar(100), @DatabaseName varchar(100), @SchemaName varchar(100), @ObjectName varchar(100),
		 @ParentSchema varchar(100), @ParentTable varchar(100), @ParentColumn varchar(100), @current_value bigint,
		  @databaseid int, @version varchar(255), @linkedserver varchar(255), @EngineEdition varchar(50)

declare @sql nvarchar(max)

DECLARE T_CURSOR CURSOR FAST_FORWARD FOR
	select ServerName, DatabaseName, SchemaName, ObjectName,
		 ParentSchema, ParentTable, ParentColumn, current_value
		 , s.serverid, d.databaseid , s.version, s.EngineEdition
	 from DatabaseObjects do
	 join databases d on d.databaseid=do.databaseid
	 join servers s on s.serverid=d.serverid
	where xtype in ('SO')
	and s.serverid = case when @serverid = 0 then s.serverid else @serverid end
open T_CURSOR
fetch next from T_CURSOR into @Server, @DatabaseName, @SchemaName, @ObjectName, @ParentSchema, @ParentTable, @ParentColumn, @current_value , @serverid, @databaseid , @version, @EngineEdition
while @@FETCH_STATUS=0
begin

	set @sql='SELECT '+cast(@serverid as varchar)+' as serverid
		, '+cast(@DatabaseId as varchar)+' as DatabaseId
		, '''+@SchemaName+'.'+@ObjectName+''' assequenceName
		, current_value as current_value
		, '''+@parentSchema+'.'+@parentTable+''' as parentTable
		, '''+@parentColumn+''' as parentColumn
		, (select max(['+@parentColumn+']) from ['+ @DatabaseName+'].['+ @parentSchema+'].['+@parentTable+']) as maxExisting
		, (select min(['+@parentColumn+']) from ['+ @DatabaseName+'].['+ @parentSchema+'].['+@parentTable+'] where ['+@parentColumn+'] > s.current_value) as NextInUse
	from (
		select cast(current_value as bigint) current_value 
		from ['+ @DatabaseName+'].sys.sequences s 
		where s.name = '''+@ObjectName+'''
		) s
	'
	
	if @DatabaseName = 'master' or @EngineEdition <> 'SQL Azure'
			set @linkedserver = @server
		else 
			set @linkedserver = @server+'.'+@DatabaseName

	SET @SQL = 'SELECT a.*	FROM OPENQUERY(['+@linkedserver+'], 
		'''+replace(@sql, '''', '''''')+'''
		) AS a;'
	print @sql
	begin try
		insert into Sequences (ServerId,DatabaseId,SequenceName,Current_value,ParentTable,ParentColumn,maxExisting,NextInUse)
		exec (@sql)
	end try
	begin catch
		print error_message()
		print @sql
	end catch

	fetch next from T_CURSOR into @Server, @DatabaseName, @SchemaName, @ObjectName, @ParentSchema, @ParentTable, @ParentColumn, @current_value , @serverid, @databaseid , @version, @EngineEdition

end
close T_CURSOR
deallocate T_CURSOR

update Sequences set IsMax = case when current_value < maxExisting then 1 else 0 end 
	, Gap =  NextInUse - current_value 

GO
/****** Object:  StoredProcedure [dbo].[spLoadServerperms]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE proc [dbo].[spLoadServerperms] @serverid varchar(10)='0'
as

/**************
	ServerPerms
**************/
if @serverid = '0'
	truncate table ServerPerms
declare @sql nvarchar(max), @SERVER VARCHAR(100)
DECLARE T_CURSOR CURSOR FAST_FORWARD FOR
	SELECT SERVERNAME, serverid FROM SERVERS s
	where isActive=1 and EngineEdition <> 'SQL Azure'
	and (	s.ServerId = @serverid
			or (@serverid =0 and not exists(select * from ServerPerms p join Logins d  on p.LoginId = d.LoginId where s.ServerId =d.serverid))
		)
OPEN T_CURSOR
FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid 
WHILE @@FETCH_STATUS=0
BEGIN
	if @SERVER = @@SERVERNAME
		set @SERVER = '.' 
	set @sql='
		;with ServerPermsAndRoles as
		(
			select
				spr.name as principal_name,
				spr.type_desc as principal_type,
				spm.permission_name collate SQL_Latin1_General_CP1_CI_AS as security_entity,
				''permission'' as security_type,
				spm.state_desc
			from sys.server_principals spr
			inner join sys.server_permissions spm
			on spr.principal_id = spm.grantee_principal_id
			where spr.type in (''s'', ''u'')

			union all

			select
				sp.name as principal_name,
				sp.type_desc as principal_type,
				spr.name as security_entity,
				''role membership'' as security_type,
				null as state_desc
			from sys.server_principals sp
			inner join sys.server_role_members srm
			on sp.principal_id = srm.member_principal_id
			inner join sys.server_principals spr
			on srm.role_principal_id = spr.principal_id
			where sp.type in (''s'', ''u'')
		)
		select *
		from ServerPermsAndRoles
		where principal_name not like ''#%'' and principal_name not like ''NT SERVICE%'' and principal_name not like ''NT SERVICE%''
		order by principal_name
		   '
	
	SET @SQL = 'SELECT '+@serverid+' as ServerId, l.LoginId, a.Principal_Type, a.Security_Entity, a.Security_type, a.state_desc
			FROM OPENQUERY(['+@SERVER+'], 
		'''+replace(@sql, '''', '''''')+'''
		) AS a
		join Logins l on l.serverid = '+@serverid+' and a.principal_name collate SQL_Latin1_General_CP1_CI_AS = l.LoginName collate SQL_Latin1_General_CP1_CI_AS
		;'
	begin try
		insert into ServerPerms (ServerId, LoginId,Principal_Type,Security_Entity,Security_type,state_desc)
		exec dbo.spExec @sql=@sql, @raiserror=1
	end try
	begin catch
		print error_message()
		print @sql
	end catch
	FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid
END
CLOSE T_CURSOR
DEALLOCATE T_CURSOR

--select * from ServerPerms

GO
/****** Object:  StoredProcedure [dbo].[spLoadServers]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE proc [dbo].[spLoadServers] @serverid int=0, @debug BIT=0
AS
BEGIN 
declare @IsActive bit

if @serverid <> 0
	UPDATE S SET IsActive=1
	FROM SERVERS S
	where serverid = @serverid
	 
/**************
	SERVERS
**************/
if OBJECT_ID('tempdb..#t1') is not null
	drop table #t1

CREATE table #t1 (
	ServerId int,
	Version varchar(255),
	Edition varchar(200),
	EngineEdition varchar(50),
	Collation varchar(200),
	Build varchar(200),
	)

if OBJECT_ID('tempdb..#t2') is not null
	drop table #t2

CREATE table #t2 (
	ServerId int,
	WindowsRelease varchar(20),
	CreatedDate datetime,
	LogicalCPUCount int,
	HyperthreadRatio int,
	PhysicalCPUCount int,
	PhysicalMemoryMB int,
	VMType varchar(50),
	resource_governor_enabled_functions tinyint
	)

declare @sql nvarchar(max), @SERVER VARCHAR(100), @error varchar(255), @Version varchar(200), @EngineEdition varchar(50)
DECLARE T_CURSOR CURSOR FAST_FORWARD FOR
	SELECT SERVERNAME, IsActive, serverid
	FROM vwSERVERS 
	where (isActive=1 or DailyChecks=1) 
	and serverid = case when @serverid =0 then serverid else  @serverid end
	ORDER BY serverid

OPEN T_CURSOR
FETCH NEXT FROM T_CURSOR INTO @SERVER, @IsActive, @serverid
WHILE @@FETCH_STATUS=0
BEGIN
	--local server
	if @SERVER = @@SERVERNAME
		set @SERVER = '.' 

	set @sql='SELECT '''+cast(@serverid as varchar)+''' serverid
					, cast(@@Version as varchar(255)) AS [SQL Server and OS Version Info]
					, cast(SERVERPROPERTY(''Edition'') as varchar) AS [Edition]
					, case when SERVERPROPERTY(''EngineEdition'') = 1 then ''Personal''
						   when SERVERPROPERTY(''EngineEdition'') = 2 then ''Standard''
						   when SERVERPROPERTY(''EngineEdition'') = 3 then ''Enterprise''
						   when SERVERPROPERTY(''EngineEdition'') = 4 then ''Express''
						   when SERVERPROPERTY(''EngineEdition'') = 5 then ''SQL Azure''
						   when SERVERPROPERTY(''EngineEdition'') = 6 then ''Synapse Dedicated SQL Pool''
						   when SERVERPROPERTY(''EngineEdition'') = 8 then ''Managed Instance''
						   when SERVERPROPERTY(''EngineEdition'') = 9 then ''SQL Edge''
						   when SERVERPROPERTY(''EngineEdition'') = 11 then ''Synapse Serverless SQL Pool''
					  end AS EngineEdition
					, cast(SERVERPROPERTY(''Collation'') as varchar) AS [Collation]
					, cast(SERVERPROPERTY(''ProductVersion'') as varchar(50)) Build
				   '

	SET @SQL = 'SELECT a.*	FROM OPENQUERY(['+@SERVER+'], 
			'''+replace(@sql, '''', '''''')+'''
			) AS a;'

	BEGIN TRY
		insert into #t1
		exec dbo.spExec @sql= @sql, @raiserror = 1, @debug = @debug

		select @Version = Version , @EngineEdition = EngineEdition 
		from #t1 
		where serverid = @serverid 

		if @debug=1
			 select @Serverid Serverid, @Version Version , @EngineEdition EngineEdition

		if @Version like 'Microsoft SQL Server 2000%' or @EngineEdition = 'SQL Azure'
			set @sql='SELECT '''+cast(@serverid as varchar)+''' serverid
						   , null windows_release
						   , null createdate
						   , null cpu_count
						   , null hyperthread_ratio
						   , null Physical_cpu
						   , null [Memory]
						   , null VM
						   , null resource_governor_enabled_functions
						   '
		if @EngineEdition = 'SQL Azure'
			set @sql='SELECT '''+cast(@serverid as varchar)+''' serverid
						   , null windows_release
						   , (SELECT  min([create_date]) FROM  [sys].[database_principals] WHERE principal_id=5) createdate
						   , null cpu_count
						   , null hyperthread_ratio
						   , null Physical_cpu
						   , null [Memory]
						   , null VM
						   , (select count(*) from sys.resource_governor_configuration where is_enabled=1) resource_governor_enabled_functions
						   '
		else if @Version like 'Microsoft SQL Server 2005%' or @Version like 'Microsoft SQL Server 2008%'
			set @sql='SELECT '''+cast(@serverid as varchar)+''' serverid
						   , null windows_release
						   , (SELECT  createdate AS [Server Name] FROM  sys.syslogins WHERE   [sid] = 0x010100000000000512000000) createdate
						   , (SELECT  cpu_count FROM    sys.dm_os_sys_info) cpu_count
						   , (SELECT  hyperthread_ratio FROM    sys.dm_os_sys_info) hyperthread_ratio
						   , (SELECT  cpu_count / hyperthread_ratio FROM    sys.dm_os_sys_info) Physical_cpu
						   , (SELECT  physical_memory_in_bytes / 1024 /1024 FROM    sys.dm_os_sys_info) [Memory]
						   , null VM
						   , null resource_governor_enabled_functions
						   '
		else 
			set @sql='SELECT '''+cast(@serverid as varchar)+''' serverid
			   , (SELECT cast(windows_release as varchar) FROM    sys.dm_os_windows_info) windows_release
			   , (SELECT  min(createdate) FROM  sys.syslogins WHERE name not like ''##%'' and name<>''sa'' ) createdate
			   , (SELECT  cpu_count FROM    sys.dm_os_sys_info) cpu_count
			   , (SELECT  hyperthread_ratio FROM    sys.dm_os_sys_info) hyperthread_ratio
			   , (SELECT  cpu_count / hyperthread_ratio FROM    sys.dm_os_sys_info) Physical_cpu
			   , (SELECT  physical_memory_kb / 1024  FROM    sys.dm_os_sys_info) [Memory]
			   , (SELECT  virtual_machine_type_desc FROM    sys.dm_os_sys_info) VM
			   , (select count(*) from sys.resource_governor_configuration where is_enabled=1) resource_governor_enabled_functions
			   '
	
		SET @SQL = 'SELECT a.*	FROM OPENQUERY(['+@SERVER+'], 
			'''+replace(@sql, '''', '''''')+'''
			) AS a;'
		
		insert into #t2
		exec dbo.spExec @sql= @sql, @raiserror = 1, @debug = @debug

		--server got back online
		if @IsActive = 0
			update servers set IsActive=1, Error = '' where Serverid = @serverid
	END TRY
    BEGIN CATCH
		PRINT @sql
		PRINT ERROR_MESSAGE()
		--could not reach server, mark as inactive to trigger notification
		UPDATE servers SET isActive=0 
			, Error = ERROR_MESSAGE()
			, ErrorDate = GETDATE()
		WHERE serverid = @serverid
	END catch


	FETCH NEXT FROM T_CURSOR INTO @SERVER, @IsActive, @serverid
END
CLOSE T_CURSOR
DEALLOCATE T_CURSOR
	
	UPDATE S SET 
		Version = t.Version,
		Edition = t.Edition,
		EngineEdition = t.EngineEdition ,
		Collation = t.Collation,
		Build = t. Build
	FROM SERVERS S 
	join #t1 t on t.serverid = s.serverid 

	UPDATE S SET 
		WindowsRelease = t.WindowsRelease,
		CreatedDate = t.CreatedDate,
		LogicalCPUCount = t.LogicalCPUCount,
		HyperthreadRatio = t.HyperthreadRatio,
		PhysicalCPUCount = t.PhysicalCPUCount,
		PhysicalMemoryMB = t.PhysicalMemoryMB,
		VMType = t.VMType,
		resource_governor_enabled_functions = t.resource_governor_enabled_functions
	FROM SERVERS S 
	join #t2 t on t.serverid = s.serverid 
	--select *  FROM SERVERS
END

GO
/****** Object:  StoredProcedure [dbo].[spLoadServices]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE proc [dbo].[spLoadServices] @serverid int=0
as

declare @sql nvarchar(max), @SERVER VARCHAR(100)
DECLARE T_CURSOR CURSOR FAST_FORWARD FOR
	SELECT SERVERNAME, serverid FROM vwServersVms s
	where isActive=1
	and Build > '10'
	and (	s.ServerId = @serverid
			or (@serverid =0 and not exists(select * from Services d where s.ServerId =d.serverid))
		)
OPEN T_CURSOR
FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid 
WHILE @@FETCH_STATUS=0
BEGIN
	if @SERVER = @@SERVERNAME
		set @SERVER = '.' 
	set @sql='SELECT  distinct serverid = '+cast(@serverid as varchar)+', servicename,startup_type,startup_type_desc,status,status_desc,process_id,last_startup_time,service_account,filename,is_clustered,cluster_nodename
	 from master.sys.dm_server_services'
	
	SET @SQL = 'SELECT a.*	FROM OPENQUERY(['+@SERVER+'], 
		'''+replace(@sql, '''', '''''')+'''
		) AS a;'
	begin try
		insert into [Services] (ServerId,servicename,startup_type,startup_type_desc,status,status_desc,process_id,last_startup_time,service_account,filename,is_clustered,cluster_nodename)
		exec dbo.spExec @sql
	end try
	begin catch
		print error_message()
		print @sql
	end catch
	FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid
END
CLOSE T_CURSOR
DEALLOCATE T_CURSOR
--select * from jobs



GO
/****** Object:  StoredProcedure [dbo].[spLoadSQLAzure]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


create   proc [dbo].[spLoadSQLAzure] @serverid varchar(10)='0'
as

/***********************
	SQLAzure
************************/
if @serverid = '0' 
	truncate table [SQLAzure]

declare @sql nvarchar(max), @SERVER VARCHAR(100), @DatabaseName varchar(100), @DatabaseId varchar(10), @linkedserver varchar(255)
DECLARE T_CURSOR CURSOR FAST_FORWARD FOR
	SELECT SERVERNAME, serverid 
	FROM SERVERS s
	where isActive=1 
	and EngineEdition = 'SQL Azure'
	and (	s.ServerId = @serverid
			or (@serverid =0 and not exists(select * from [SQLAzure] d where s.ServerId =d.serverid))
		)
OPEN T_CURSOR
FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid 
WHILE @@FETCH_STATUS=0
BEGIN
	declare d_cursor cursor fast_forward for
		select databaseid, databasename from vwdatabases 
		where state_desc = 'online'
		and databasename not in ('master','tempdb','msdb','model')
		and serverid=@serverid 
	open d_cursor
	FETCH NEXT FROM d_CURSOR INTO @databaseid, @databasename
	while @@FETCH_STATUS=0
	begin
		set @sql='
		SELECT rg.slo_name,
			   CAST(DATABASEPROPERTYEX(DB_NAME(), ''Edition'') AS nvarchar(40)) COLLATE DATABASE_DEFAULT AS service_tier,
			   CASE WHEN slo.slo_name LIKE ''%SQLG4%'' THEN ''Gen4''
					WHEN slo.slo_name LIKE ''%SQLGZ%'' THEN ''Gen4''
					WHEN slo.slo_name LIKE ''%SQLG5%'' THEN ''Gen5''
					WHEN slo.slo_name LIKE ''%SQLG6%'' THEN ''Gen5''
					WHEN slo.slo_name LIKE ''%SQLG7%'' THEN ''Gen5''
					WHEN slo.slo_name LIKE ''%GPGEN8%'' THEN ''Gen5''
			   END COLLATE DATABASE_DEFAULT AS hardware_gen,
			   s.scheduler_count * CAST(rg.instance_cap_cpu/100. AS decimal(3,2)) AS logical_cpus,
			   CAST((jo.process_memory_limit_mb) / 1024. AS decimal(4,2)) AS memory
			   , dtu_limit, cpu_limit
			   --select *
		FROM ['+@DatabaseName+'].sys.dm_user_db_resource_governance AS rg
		CROSS JOIN (SELECT COUNT(1) AS scheduler_count 
			FROM ['+@DatabaseName+'].sys.dm_os_schedulers 
			WHERE status COLLATE DATABASE_DEFAULT = ''VISIBLE ONLINE'') AS s
		CROSS JOIN ['+@DatabaseName+'].sys.dm_os_job_object AS jo
		CROSS APPLY (
					SELECT UPPER(rg.slo_name) COLLATE DATABASE_DEFAULT AS slo_name
					) slo
		WHERE  rg.database_id = DB_ID()
		'
		
	set @linkedserver = @server+'.'+@DatabaseName
	
	SET @SQL = 'SELECT '+@serverid+', '+@databaseid+', a.*
			FROM OPENQUERY(['+@linkedserver+'], 
			'''+replace(@sql, '''', '''''')+'''
			) AS a
			;'
		begin try
			insert into [SQLAzure] (ServerId, DatabaseId, SLO_Name, Service_Tier, Hardware_Gen, Logical_CPUs, Memory, DTUs, CPUs)
			exec dbo.spExec @sql
		end try
		begin catch
			print error_message()
			print @sql
		end catch
		FETCH NEXT FROM d_CURSOR INTO @databaseid, @databasename
	end
	close d_cursor
	deallocate d_cursor
	FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid 
END
CLOSE T_CURSOR
DEALLOCATE T_CURSOR
GO
/****** Object:  StoredProcedure [dbo].[spLoadSubscriptions]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



CREATE proc [dbo].[spLoadSubscriptions] @serverid int=0
as
declare @sql nvarchar(max), @SERVER VARCHAR(100)
	, @publisherid int, @publisher varchar(100), @Distribution_db varchar(100)

--truncate table Subscriptions


DECLARE server_cursor CURSOR FAST_FORWARD FOR
	SELECT SERVERNAME, serverid FROM SERVERS s
	where isActive=1 
	and (	s.ServerId = @serverid
			or (@serverid =0 and not exists(select * from [Subscriptions] d where s.ServerId =d.serverid))
		)
OPEN server_cursor
FETCH NEXT FROM server_cursor INTO @SERVER, @serverid 
WHILE @@FETCH_STATUS=0
BEGIN
	declare publisher_cursor cursor fast_forward for
		select  publisherid, p.PublisherName, p.distribution_db from [Publishers] p where Active=1 and Serverid = @serverid
	open publisher_cursor
	FETCH NEXT FROM publisher_cursor INTO @publisherid, @publisher, @Distribution_db
	while @@FETCH_STATUS=0
	begin
		begin try

			set @sql='
			select PublisherId = '+cast(@publisherid as varchar)+', serverId= '+cast(@serverid as varchar)+', s.Publication_Id, s.Article_Id
				, sub.name as subscriber_server, s.subscriber_db
				, s.subscription_type, s.sync_type, s.status, s.snapshot_seqno_flag, s.independent_agent, s.subscription_time, s.loopback_detection, s.agent_id, s.update_mode 
			from ['+@Distribution_db+'].[dbo].[MSsubscriptions] s 
			join master.sys.servers sub on s.subscriber_id = sub.server_id'

			SET @SQL = 'SELECT a.*	FROM OPENQUERY(['+@SERVER+'], 
			'''+replace(@sql, '''', '''''')+'''
			) AS a;'

			insert into [Subscriptions] (PublisherId,serverId, remote_publication_id, remote_article_id, subscriber_server,subscriber_db,subscription_type,sync_type,status,snapshot_seqno_flag,independent_agent,subscription_time,loopback_detection,agent_id,update_mode )
			exec dbo.spExec @sql
		end try
		begin catch
			print error_message()
			print @sql
		end catch
		FETCH NEXT FROM publisher_cursor INTO @publisherid, @publisher, @Distribution_db
	end
	close publisher_cursor
	deallocate publisher_cursor
	FETCH NEXT FROM server_cursor INTO @SERVER, @serverid
END
CLOSE server_cursor
DEALLOCATE server_cursor

update s set PublicationId = a.PublicationId, Articleid = a.Articleid
from Subscriptions s 
join Articles a on a.remote_publication_id = s.remote_publication_id
	and a.ServerId = s.ServerId 
	and a.PublisherId = s.PublisherId
	and a.remote_article_id = s.remote_article_id



GO
/****** Object:  StoredProcedure [dbo].[spLoadTempConnections]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE   proc [dbo].[spLoadTempConnections] @serverid varchar(10)='0'
as

/**************
	Logins
**************/

declare @sql nvarchar(max), @SERVER VARCHAR(100)
DECLARE T_CURSOR CURSOR FAST_FORWARD FOR
	SELECT SERVERNAME, serverid FROM SERVERS s
	where isActive=1 
	and (	s.ServerId = @serverid
			or (@serverid =0 )
		)
OPEN T_CURSOR
FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid 
WHILE @@FETCH_STATUS=0
BEGIN 
	set @sql='
		select distinct 
			case when program_name like ''ServiceDesk%'' then ''ServiceDesk'' 
				 when program_name like ''%.Net Sql%'' or program_name like ''%SQL Server%'' then loginame 
			else program_name end as Application
			, db_name(dbid) db_name, hostname, count(*) cnt
		--select *
		from sysprocesses
		where dbid>4
		and program_name not in (''Microsoft SQL Server Management Studio''
			,''Microsoft SQL Server Management Studio - Query'') 
		group by case when program_name like ''ServiceDesk%'' then ''ServiceDesk'' 
					when program_name like ''%.Net Sql%'' or program_name like ''%SQL Server%'' then loginame 
			else program_name end 
			, db_name(dbid), hostname
		'
	
	SET @SQL = 'SELECT serverid = '+@serverid +', a.*
		FROM OPENQUERY(['+@SERVER+'], 
		'''+replace(@sql, '''', '''''')+'''
		) AS a;'
	begin try
		insert into TempConnections (ServerId, App, Db,Host, Cnt)
		exec dbo.spExec @sql
	end try
	begin catch
		print error_message()
		print @sql
	end catch
	FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid
END
CLOSE T_CURSOR
DEALLOCATE T_CURSOR

--select * from Logins

GO
/****** Object:  StoredProcedure [dbo].[spLoadTopSql]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE proc [dbo].[spLoadTopSql]  @serverid varchar(10)='0'
as

/***********************
	TopSql
************************/
if @serverid = '0'
	truncate table TopSql
else 
	delete from TopSql where serverid = @serverid

declare @sql nvarchar(max), @SERVER VARCHAR(100), @DatabaseName varchar(100), @DatabaseId varchar(10), @version varchar(255), @linkedserver varchar(255), @EngineEdition varchar(50)

DECLARE T_CURSOR CURSOR FAST_FORWARD FOR
	SELECT SERVERNAME, serverid, version, EngineEdition FROM SERVERS s
	where isActive=1 
	and (	s.ServerId = @serverid
			or (@serverid =0 and not exists(select * from TopSql d where s.ServerId =d.serverid))
		)
OPEN T_CURSOR
FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid , @version, @EngineEdition
WHILE @@FETCH_STATUS=0
BEGIN
	declare d_cursor cursor fast_forward for
		select databaseid, databasename from vwdatabases where serverid=@serverid and databasename not in ('msdb','master','tempdb','model') and state_desc = 'online' and ServerName = coalesce(PrimaryReplicaServerName,ServerName)
	open d_cursor
	FETCH NEXT FROM d_CURSOR INTO @databaseid, @databasename
	while @@FETCH_STATUS=0
	begin
		if @SERVER = @@SERVERNAME
			set @SERVER = '.' 
		set @sql='
		SELECT TOP ( 10 )
				s.name + ''.''+ p.name AS [SP Name]
			  , qs.total_worker_time AS [TotalWorkerTime]
			  , qs.total_worker_time / qs.execution_count AS [AvgWorkerTime]
			  , qs.execution_count
			  , ISNULL(qs.execution_count / DATEDIFF(Second, qs.cached_time, GETDATE()), 0) AS [Calls/Second]
			  , qs.total_elapsed_time
			  , qs.total_elapsed_time / qs.execution_count AS [avg_elapsed_time]
			  , qs.cached_time
		FROM    ['+@DatabaseName+'].sys.procedures AS p WITH ( NOLOCK )
				INNER JOIN ['+@DatabaseName+'].sys.dm_exec_procedure_stats AS qs WITH ( NOLOCK ) ON p.[object_id] = qs.[object_id]
				INNER JOIN ['+@DatabaseName+'].sys.schemas s on s.schema_id = p.schema_id
		WHERE   qs.database_id = DB_ID('''+@DatabaseName+''')
		ORDER BY qs.total_worker_time DESC
		OPTION  ( RECOMPILE );
		'
		
		if @DatabaseName = 'master' or @EngineEdition <> 'SQL Azure' 
			set @linkedserver = @server
		else 
			set @linkedserver = @server+'.'+@DatabaseName

		SET @SQL = 'SELECT '+@serverid+', '+@databaseid+', a.*
			FROM OPENQUERY(['+@linkedserver+'], 
			'''+replace(@sql, '''', '''''')+'''
			) AS a
			;'
		begin try
			insert into TopSql (Serverid,DatabaseId,SPName,TotalWorkerTime,AvgWorkerTime,execution_count,CallsPerSecond,total_elapsed_time,avg_elapsed_time,cached_time)
			exec dbo.spExec @sql
		end try
		begin catch
			print error_message()
			print @sql
		end catch
		FETCH NEXT FROM d_CURSOR INTO @databaseid, @databasename
	end
	close d_cursor
	deallocate d_cursor
	FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid, @version, @EngineEdition
END
CLOSE T_CURSOR
DEALLOCATE T_CURSOR


GO
/****** Object:  StoredProcedure [dbo].[spLoadTopWait]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE proc [dbo].[spLoadTopWait] @serverid varchar(10)='0'
as

/***********************
	TopWait
************************/
if @serverid = '0'
	truncate table TopWait

declare @sql nvarchar(max), @SERVER VARCHAR(100), @DatabaseName varchar(100), @DatabaseId varchar(10)
DECLARE T_CURSOR CURSOR FAST_FORWARD FOR
	SELECT SERVERNAME, serverid FROM SERVERS s
	where isActive=1 
	and (	s.ServerId = @serverid
			or (@serverid =0 and not exists(select * from TopWait d where s.ServerId =d.serverid))
		)
OPEN T_CURSOR
FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid 
WHILE @@FETCH_STATUS=0
BEGIN
	if @SERVER = @@SERVERNAME
		set @SERVER = '.' 
	set @sql='
		SELECT TOP 40 wait_type, 
              max_wait_time_ms 
              wait_time_ms, 
              signal_wait_time_ms, 
              wait_time_ms - signal_wait_time_ms       AS resource_wait_time_ms, 
              100.0 * wait_time_ms / Sum(wait_time_ms)   OVER ( ) AS percent_total_waits, 
              100.0 * signal_wait_time_ms / Sum(signal_wait_time_ms) OVER ( )   AS percent_total_signal_waits, 
              100.0 * ( wait_time_ms - signal_wait_time_ms ) / Sum(wait_time_ms)  OVER ( ) AS percent_total_resource_waits 
		FROM   sys.dm_os_wait_stats 
		WHERE  wait_time_ms > 0 -- remove zero wait_time 
			   AND wait_type NOT IN -- filter out additional irrelevant waits 
				   ( 
					       ''BROKER_EVENTHANDLER'', ''BROKER_RECEIVE_WAITFOR'',
						   ''BROKER_TASK_STOP'', ''BROKER_TO_FLUSH'',
						   ''BROKER_TRANSMITTER'', ''CHECKPOINT_QUEUE'',
						   ''CHKPT'', ''CLR_AUTO_EVENT'',
						   ''CLR_MANUAL_EVENT'', ''CLR_SEMAPHORE'', 
						   -- Maybe uncomment these four if you have mirroring issues
						   ''DBMIRROR_DBM_EVENT'', ''DBMIRROR_EVENTS_QUEUE'',
						   ''DBMIRROR_WORKER_QUEUE'', ''DBMIRRORING_CMD'',
						   ''DIRTY_PAGE_POLL'', ''DISPATCHER_QUEUE_SEMAPHORE'',
						   ''EXECSYNC'', ''FSAGENT'',
						   ''FT_IFTS_SCHEDULER_IDLE_WAIT'', ''FT_IFTSHC_MUTEX'',
						   --Maybe uncomment these six if you have AG issues
						   ''HADR_CLUSAPI_CALL'', ''HADR_FILESTREAM_IOMGR_IOCOMPLETION'',
						   ''HADR_LOGCAPTURE_WAIT'', ''HADR_NOTIFICATION_DEQUEUE'',
						   ''HADR_TIMER_TASK'', ''HADR_WORK_QUEUE'',
						   ''KSOURCE_WAKEUP'', ''LAZYWRITER_SLEEP'',
						   ''LOGMGR_QUEUE'', ''MEMORY_ALLOCATION_EXT'',
						   ''ONDEMAND_TASK_QUEUE'',
						   ''PREEMPTIVE_XE_GETTARGETSTATE'',
						   ''PWAIT_ALL_COMPONENTS_INITIALIZED'',
						   ''PWAIT_DIRECTLOGCONSUMER_GETNEXT'',
						   ''QDS_PERSIST_TASK_MAIN_LOOP_SLEEP'', ''QDS_ASYNC_QUEUE'',
						   ''QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP'',
						   ''QDS_SHUTDOWN_QUEUE'', ''REDO_THREAD_PENDING_WORK'',
						   ''REQUEST_FOR_DEADLOCK_SEARCH'', ''RESOURCE_QUEUE'',
						   ''SERVER_IDLE_CHECK'', ''SLEEP_BPOOL_FLUSH'',
						   ''SLEEP_DBSTARTUP'', ''SLEEP_DCOMSTARTUP'',
						   ''SLEEP_MASTERDBREADY'', ''SLEEP_MASTERMDREADY'',
						   ''SLEEP_MASTERUPGRADED'', ''SLEEP_MSDBSTARTUP'',
						   ''SLEEP_SYSTEMTASK'', ''SLEEP_TASK'',
						   ''SLEEP_TEMPDBSTARTUP'', ''SNI_HTTP_ACCEPT'',
						   ''SP_SERVER_DIAGNOSTICS_SLEEP'', ''SQLTRACE_BUFFER_FLUSH'',
						   ''SQLTRACE_INCREMENTAL_FLUSH_SLEEP'',
						   ''SQLTRACE_WAIT_ENTRIES'', ''WAIT_FOR_RESULTS'',
						   ''WAITFOR'', ''WAITFOR_TASKSHUTDOWN'',
						   ''WAIT_XTP_RECOVERY'',
						   ''WAIT_XTP_HOST_WAIT'', ''WAIT_XTP_OFFLINE_CKPT_NEW_LOG'',
						   ''WAIT_XTP_CKPT_CLOSE'', ''XE_DISPATCHER_JOIN'',
						   ''XE_DISPATCHER_WAIT'', ''XE_TIMER_EVENT''
						   , ''CXPACKET'', ''PREEMPTIVE_XE_DISPATCHER'', ''SOS_WORK_DISPATCHER''
					 ) 
		ORDER  BY 7 DESC 
		'
	
		SET @sql = 'SELECT '+@serverid+', a.*
			FROM OPENQUERY(['+@SERVER+'], 
			'''+replace(@sql, '''', '''''')+'''
			) AS a
			;'
		begin try
			insert into TopWait (Serverid, wait_type,wait_time_ms,signal_wait_time_ms,resource_wait_time_ms,percent_total_waits,percent_total_signal_waits,percent_total_resource_waits)
			exec dbo.spExec @sql
		end try
		begin catch
			print error_message()
			print @sql
		end catch

	FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid
END
CLOSE T_CURSOR
DEALLOCATE T_CURSOR




GO
/****** Object:  StoredProcedure [dbo].[spLoadVolumes]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE proc [dbo].[spLoadVolumes]  @serverid int=0
as
if @serverid = '0'
	truncate table Volumes
else 
	delete from Volumes where serverid=@serverid


/**************
	Volumes
**************/
declare @sql nvarchar(max), @SERVER VARCHAR(100), @Version	varchar	(255)

DECLARE T_CURSOR CURSOR FAST_FORWARD FOR
	SELECT SERVERNAME, serverid, Version FROM vwServersVms S
	where isActive=1 
	and Build >='10'
	--and EngineEdition not in ('SQL Azure', 'Managed Instance','Synapse Dedicated SQL Pool','Synapse Serverless SQL Pool')
	and (	s.ServerId = @serverid
			or (@serverid =0 and not exists(select * from Volumes d where s.ServerId =d.serverid))
		)
OPEN T_CURSOR
FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid, @Version 
WHILE @@FETCH_STATUS=0
BEGIN
	if @SERVER = @@SERVERNAME
		set @SERVER = '.' 
	set @sql='
		SELECT  distinct serverid = '+cast(@serverid as varchar)+'
			  , vs.volume_mount_point
			  , vs.total_bytes / 1024 / 1024 / 1024
			  , vs.available_bytes / 1024 / 1024 / 1024
			  , CAST(CAST(vs.available_bytes AS FLOAT) / CAST(vs.total_bytes AS FLOAT) AS DECIMAL(18, 3)) * 100 AS [Space Free %]
		FROM    sys.master_files AS f
				CROSS APPLY sys.dm_os_volume_stats(f.database_id, f.file_id) AS vs
		OPTION  ( RECOMPILE );
		   '
	
	SET @SQL = 'SELECT a.*	FROM OPENQUERY(['+@SERVER+'], 
		'''+replace(@sql, '''', '''''')+'''
		) AS a;'
	begin try
		insert into Volumes (ServerId, volume_mount_point, TotalGB,AvailableGB,PercentageFree)
		exec dbo.spExec @sql = @sql, @raiserror = 0
	end try
	begin catch
		print error_message()
		print @sql
	end catch
	FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid, @Version
END
CLOSE T_CURSOR
DEALLOCATE T_CURSOR

--select * from volumes
GO
/****** Object:  StoredProcedure [dbo].[spLogmanCreate]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




CREATE PROC [dbo].[spLogmanCreate] ( 
	@action varchar(50)='start'--create, start, stop, delete
	,@serversuffix varchar(50)=''
	)
as
 
declare @sql varchar(max), @SERVER VARCHAR(100), @cmd varchar(max), @counters varchar(max)
DECLARE T_CURSOR CURSOR FAST_FORWARD FOR
	SELECT SERVERNAME FROM SERVERS where servername  like '%'+@serversuffix+'%' and PerfMonLogs > '' and PerfMonLogs is not null and version not like '%azure%'
OPEN T_CURSOR
FETCH NEXT FROM T_CURSOR INTO @SERVER
WHILE @@FETCH_STATUS=0
BEGIN
	if @action = 'create'
	begin
		if @server like '%SQL%'
			set @counters = ' "\Memory\Available MBytes" "\Processor(_Total)\% Processor Time" "\\'+@server+'\SQLServer:Access Methods\Forwarded Records/sec" "\\'+@server+'\SQLServer:Access Methods\Full Scans/sec" "\\'+@server+'\SQLServer:Access Methods\Index Searches/sec" "\\'+@server+'\SQLServer:Buffer Manager\Page life expectancy" "\\'+@server+'\SQLServer:Buffer Manager\Page reads/sec" "\\'+@server+'\SQLServer:Buffer Manager\Page writes/sec" "\\'+@server+'\SQLServer:Buffer Manager\Lazy writes/sec" "\PhysicalDisk(1 C:)\Avg. Disk Bytes/Read" "\PhysicalDisk(1 C:)\Avg. Disk Bytes/Write" "\PhysicalDisk(1 C:)\Avg. Disk Queue Length" "\PhysicalDisk(1 C:)\Avg. Disk sec/Read" "\PhysicalDisk(1 C:)\Avg. Disk sec/Write" "\PhysicalDisk(1 D:)\Avg. Disk Bytes/Read" "\PhysicalDisk(1 D:)\Avg. Disk Bytes/Write" "\PhysicalDisk(1 D:)\Avg. Disk Queue Length" "\PhysicalDisk(1 D:)\Avg. Disk sec/Read" "\PhysicalDisk(1 D:)\Avg. Disk sec/Write" '
		else if @server like '%app%'
			set @counters = ' "\\'+@server+'\.NET CLR Memory(_Global_)\# Total committed Bytes" "\\'+@server+'\ASP.NET\Application Restarts" "\\'+@server+'\ASP.NET\Request Wait Time" "\\'+@server+'\ASP.NET\Requests Queued" "\\'+@server+'\ASP.NET Applications(__Total__)\Requests/Sec" "\\'+@server+'\LogicalDisk(C:)\% Disk Time" "\\'+@server+'\LogicalDisk(D:)\% Disk Time" "\\'+@server+'\Memory\Available MBytes" "\\'+@server+'\Memory\Pages/sec" "\\'+@server+'\PhysicalDisk(_Total)\% Disk Time" "\\'+@server+'\System\Processor Queue Length" "\\'+@server+'\Web Service(_Total)\Post Requests/sec" "\\'+@server+'\Web Service(_Total)\Current Connections" "\\'+@server+'\Network Interface(Broadcom BCM5716C NetXtreme II GigE [NDIS VBD Client] _32)\Bytes Total/sec" "\\'+@server+'\Processor(_Total)\% Processor Time" '
				
		set @cmd = 'Logman create counter '+@server+' -b 7/22/2014 08:00:00 -e 7/22/2014 23:00:00 -s '+@server+' -a -si 01:00 -r  -v mmddhhmm -f csv -o "c:\Logs\PerfMon\'+@server+'" -c "c:\Logs\PerfMon\'+@server+'" ' + @counters
	end
	else
	begin
		set @cmd= 'Logman '+@action+' '+@server + ' -s '+@server
	end
	print @cmd
	FETCH NEXT FROM T_CURSOR INTO @SERVER
END
CLOSE T_CURSOR
DEALLOCATE T_CURSOR




GO


CREATE   proc [dbo].[spPurgeAudit] @days varchar(50)=14, @debug bit=0, @exec bit=1
as
declare @sql varchar(max)
declare t_cursor cursor fast_forward for 
	select 'exec ( ''delete from ['+DatabaseName+'].dbo.['+ObjectName+'] where TSQLCommand like ''''alter table%check constraint%'''' or TSQLCommand like ''''UPDATE STATISTICS%'''' or TSQLCommand like ''''alter index%'''' '') at ['+ServerName+']'
	from vwDatabaseObjects 
	where ObjectName = 'DatabaseAudit' and SchemaName='dbo'
	union all
	select 'exec ( ''delete from ['+DatabaseName+'].dbo.['+ObjectName+'] where TSQLCommand like ''''alter table%check constraint%'''' or TSQLCommand like ''''update statistics MS%'''' or ObjectName=''''telemetry_xevents''''  '') at ['+ServerName+']'
	from vwDatabaseObjects 
	where ObjectName = 'ServerAudit' and SchemaName='dbo'
open t_cursor
fetch next from t_cursor into @sql
while @@FETCH_STATUS=0
begin
	exec [dbo].[spExec] @sql, @debug, @exec
	fetch next from t_cursor into @sql
end
close t_cursor
deallocate t_cursor
GO
/****** Object:  StoredProcedure [dbo].[spPurgeDates]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE proc [dbo].[spPurgeDates]
as
declare @sql varchar(max)
declare t_cursor cursor fast_forward for 
	with a as (
		select servername, databasename, SchemaName, ObjectName 
		from vwDatabaseObjects 
		where xtype='u'
		and SchemaName = 'rpl'
		and ObjectName like 'Dates%'
	)
	/*
	After 14 days keep only 1 record per hour
	After 90 days keep only 1 record per day
	After 270 days delete
	*/
	select 'exec ( ''
	delete d from '+DatabaseName+'.rpl.['+ObjectName+'] d 
	where  (Date <  getdate()-14 and datepart(mi, Date) > 0)--keep only the 0 minute
		or (Date <  getdate()-90 and not (datepart(mi, Date) = 0 and datepart(hh, Date) = 0 ))--keep only the zero minute of the zero hour
		or (Date <  getdate()-270 )
	'') at ['+ServerName+']
	'
	from a
open t_cursor
fetch next from t_cursor into @sql
while @@FETCH_STATUS=0
begin
	exec spExec @sql, 0,1,0
	fetch next from t_cursor into @sql
end
close t_cursor
deallocate t_cursor
GO
/****** Object:  StoredProcedure [dbo].[spPurgeDeadLocks]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE proc [dbo].[spPurgeDeadLocks] @serverid int=0, @days int=1, @debug	bit=0, @exec bit=1
as

exec [spLoadDeadLockFiles] @serverid

declare @sql varchar(max)='', @DeadLockEvents varchar(200), @path varchar(255), @ServerName varchar(100)

declare t_cursor cursor fast_forward for
	select servername, DeadlockEvents, serverid
	from Servers 
	where IsActive=1 and DeadlockEvents is not null and DeadlockEvents <> '' 
	and serverid = case when @serverid =0 then serverid else  @serverid end 
	order by 1
open t_cursor
fetch next from t_cursor into @ServerName, @DeadLockEvents, @serverid
while @@FETCH_STATUS=0
begin
	begin try
		set @sql = 'exec (''alter EVENT SESSION [DeadLocks] ON SERVER state=stop'') at ['+@ServerName+']'
		exec spExec @sql, @debug, @exec, 0

		declare f_cursor cursor fast_forward for 
			select foldername + filename 
			from DeadLockFiles 
			where serverid = @serverid
			and date <= getdate()-@days
		open f_cursor
		fetch next from f_cursor into @path
		while @@FETCH_STATUS=0
		begin
			set @sql =  'exec master..xp_cmdshell ''del '+@path+''''
			exec spExec @sql, @debug, @exec, 0
			fetch next from f_cursor into @path
		end
		close f_cursor
		deallocate f_cursor

		set @sql = 'exec (''alter EVENT SESSION [DeadLocks] ON SERVER state=start'') at ['+@ServerName+']'
		exec spExec @sql, @debug, @exec, 0
	end try
	begin catch
		select @ServerName, error_message()
	end catch
	fetch next from t_cursor into @ServerName, @DeadLockEvents, @serverid
end
close t_cursor
deallocate t_cursor



GO
/****** Object:  StoredProcedure [dbo].[spPurgeErrors]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE proc [dbo].[spPurgeErrors] @serverid int=0, @days int=1, @debug	bit=0, @exec	bit=1
as

exec [spLoadErrorFiles] @serverid

declare @sql varchar(max)='', @ErrorEvents varchar(200), @path varchar(255), @ServerName varchar(100)

declare t_cursor cursor fast_forward for
	select servername, ErrorEvents, serverid
	from Servers 
	where IsActive=1 and ErrorEvents is not null and ErrorEvents <> '' 
	and serverid = case when @serverid =0 then serverid else  @serverid end 
	order by 1
open t_cursor
fetch next from t_cursor into @ServerName, @ErrorEvents, @serverid
while @@FETCH_STATUS=0
begin
	begin try
		set @sql = 'exec (''alter EVENT SESSION [Errors] ON SERVER state=stop'') at ['+@ServerName+']'
		exec spExec @sql, @debug, @exec

		declare f_cursor cursor fast_forward for 
			select foldername + filename 
			from ErrorFiles 
			where serverid = @serverid
			and date <= getdate()-@days
		open f_cursor
		fetch next from f_cursor into @path
		while @@FETCH_STATUS=0
		begin
			set @sql =  'exec master..xp_cmdshell ''del '+@path+''''
			exec spExec @sql, @debug, @exec
			fetch next from f_cursor into @path
		end
		close f_cursor
		deallocate f_cursor

		set @sql = 'exec (''alter EVENT SESSION [Errors] ON SERVER state=start'') at ['+@ServerName+']'
		exec spExec @sql, @debug, @exec
	end try
	begin catch
		select @ServerName, error_message()
	end catch
	fetch next from t_cursor into @ServerName, @ErrorEvents, @serverid
end
close t_cursor
deallocate t_cursor

GO
/****** Object:  StoredProcedure [dbo].[spPurgeLongSql]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE   proc [dbo].[spPurgeLongSql] @serverid int=0, @days int=1, @debug	bit=0, @exec	bit=1
as

exec [spLoadLongSqlFiles] @serverid

declare @sql varchar(max)='', @LongQueryEvents varchar(200), @path varchar(255), @ServerName varchar(100)

declare t_cursor cursor fast_forward for
	select servername, LongQueryEvents, serverid
	from Servers 
	where IsActive=1 and LongQueryEvents is not null and LongQueryEvents <> '' 
	and serverid = case when @serverid =0 then serverid else  @serverid end 
	order by 1
open t_cursor
fetch next from t_cursor into @ServerName, @LongQueryEvents, @serverid
while @@FETCH_STATUS=0
begin
	begin try
		set @sql = 'exec (''alter EVENT SESSION [LongSql] ON SERVER state=stop'') at ['+@ServerName+']'
		exec spExec @sql, @debug, @exec, @debug

		declare f_cursor cursor fast_forward for 
			select foldername + filename 
			from LongSqlFiles 
			where serverid = @serverid
			and date <= getdate()-@days
		open f_cursor
		fetch next from f_cursor into @path
		while @@FETCH_STATUS=0
		begin
			set @sql =  'exec master..xp_cmdshell ''del '+@path+''''
			exec spExec @sql, @debug, @exec, @debug
			fetch next from f_cursor into @path
		end
		close f_cursor
		deallocate f_cursor

		set @sql = 'exec (''alter EVENT SESSION [LongSql] ON SERVER state=start'') at ['+@ServerName+']'
		exec spExec @sql, @debug, @exec, @debug
	end try
	begin catch
		select @ServerName, error_message()
	end catch
	fetch next from t_cursor into @ServerName, @LongQueryEvents, @serverid
end
close t_cursor
deallocate t_cursor


GO
/****** Object:  StoredProcedure [dbo].[spPurgeRplDeleteTables]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE proc [dbo].[spPurgeRplDeleteTables] @days varchar(50)=14, @debug bit=0, @exec bit=1
as
declare @sql varchar(max)
declare t_cursor cursor fast_forward for 
	select 'exec ( ''delete from '+DatabaseName+'.rpl.['+ObjectName+'] where dt < getdate()-'+@days+''') at ['+ServerName+']
	'
	from vwDatabaseObjects 
	where ObjectName like 'del%' and schemaname='rpl' and [RowCount]>0 and xtype='u'
	--and ObjectName not in ('del_account_TBL_StockNotificationList')-- exclusion list
	order by [RowCount] desc
open t_cursor
fetch next from t_cursor into @sql
while @@FETCH_STATUS=0
begin
	exec [dbo].[spExec] @sql, @debug, @exec
	fetch next from t_cursor into @sql
end
close t_cursor
deallocate t_cursor

GO
/****** Object:  StoredProcedure [dbo].[spPurgeRplImportLog]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE proc [dbo].[spPurgeRplImportLog] @debug bit = 0
as
declare @sql varchar(max)
declare t_cursor cursor fast_forward for 
	with a as (select servername, databasename, count(*) cnt from vwRplImportLog group by servername, databasename)
	select 'exec ( ''
	delete d from '+DatabaseName+'.rpl.ImportLogDetail d join '+DatabaseName+'.rpl.ImportLog l on l.ImportLogid = d.ImportLogId where l.rvFrom <> ''''0x'''' and l.startDate < cast(getdate()-7 as date)
	delete l from '+DatabaseName+'.rpl.ImportLog l where l.rvFrom <> ''''0x'''' and l.startDate < cast(getdate()-7 as date) 
	'') at ['+ServerName+']
	'
	from a
open t_cursor
fetch next from t_cursor into @sql
while @@FETCH_STATUS=0
begin
	exec spExec @sql, @debug,1,@debug
	
	fetch next from t_cursor into @sql
end
close t_cursor
deallocate t_cursor

GO
/****** Object:  StoredProcedure [dbo].[spReplicationCheck]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE proc [dbo].[spReplicationCheck] (@MinutesBehind smallint=-1, @ServerName varchar(100)='%', @DatabaseName varchar(100)='%', @TableName varchar(100)='%', @Email varchar(100) ='', @debug bit=0)
as

if OBJECT_ID('tempdb..#t') is not null
	drop table #t

create table #t (
	PublisherServer varchar(100),
	PublisherDatabase varchar(100),
	SubscriberServer varchar(100),
	SubscriberDatabase varchar(100),
	SubscriptionId int,
	DateFromSubscription datetime,
	LastSuccessDate datetime,
	MinutesBehind int,
	LastStartDate datetime,
	LastEndDate datetime,
	LastDurationInSeconds int,
	LastTotalRows bigint,
	LastMessage varchar(max), 
	RunsToday int,
	TotalRowsToday bigint,
	TotalKbsToday bigint,
	RplSubscriptionRowid int,
	TableList  varchar(max),
	DelayAlertInMinutes int
)

set nocount on
declare @Server varchar(100), @Sql varchar(max)
declare t_cursor cursor fast_forward for
	select s.ServerName, '
		select '''+su.ServerName+'''
			, '''+su.DatabaseName+'''
			, '''+s.ServerName+'''
			, '''+d.DatabaseName+'''			
			, '+cast(su.SubscriptionId as varchar)+'
			, d.DateFromSubscription
			, LastSuccessDate
			, datediff(mi, LastSuccessDate, getdate())
			, l.StartDate
			, l.EndDate
			, datediff(ss, l.StartDate, isnull(l.EndDate,getdate()))
			, l.TotalRows
			, l.Message
			, t.RunsToday
			, t.TotalRowsToday
			, t.TotalKbsToday
			, '+cast(su.RowId as varchar)+' RplSubscriptionRowId
			, STUFF(
                   (SELECT
                        '', '' +st.SchemaName+''.''+st.TableName
                        FROM ['+d.DatabaseName+'].rpl.SubscriptionTable st
                        WHERE st.SubscriptionId = '+cast(su.SubscriptionId as varchar)+'
                        ORDER BY  st.SchemaName, st.TableName
                        FOR XML PATH(''''), TYPE
                   ).value(''.'',''varchar(max)'')
                   ,1,2, ''''
              ) AS TableList
			, s.DelayAlertInMinutes
		from (select * from ['+d.DatabaseName+'].[rpl].Subscription where isActive=1 and SubscriptionId = '+cast(su.SubscriptionId as varchar)+') s 
		outer apply (
			select max(Date) DateFromSubscription from ['+d.DatabaseName+'].rpl.[DatesFromSubscription_'+cast(su.SubscriptionId as varchar)+']
		) d
		outer apply (
			select top 1 *
			from ['+d.DatabaseName+'].[rpl].[ImportLog] L
			where SubscriptionId = '+cast(su.SubscriptionId as varchar)+'
			order by L.ImportLogId desc
		) L
		outer apply (
			select count(*) RunsToday
				, sum(TotalRows) TotalRowsToday
				, max(StartDate) LastSuccessDate
				, sum(TotalKbs) TotalKbsToday
			from ['+d.DatabaseName+'].[rpl].[ImportLog] L
			where SubscriptionId = '+cast(su.SubscriptionId as varchar)+'
			and Success=1
			and StartDate >=  cast(getdate() as date)
		) T
		where s.SubscriptionId = '+cast(su.SubscriptionId as varchar)+'
	'--select distinct su.ServerName, su.DatabaseName, s.ServerName, d.DatabaseName, su.SubscriptionId 
	from RplSubscription su
	join servers s on s.ServerId = su.ServerId
	join databases d on d.DatabaseId = su.DatabaseId
	where su.IsActive=1
	and s.IsActive=1
	and s.DailyChecks=1
	and s.ServerName like @ServerName 
	and d.DatabaseName like @DatabaseName 
	order by su.ServerName, d.DatabaseName 
open t_cursor
fetch next from t_cursor into @Server, @Sql
while @@FETCH_STATUS=0
begin 
	SET @SQL = 'SELECT a.*	FROM OPENQUERY(['+@SERVER+'], 
		'''+replace(@sql, '''', '''''')+'''
		) AS a;'
	begin try
		insert into #t
		exec spExec @SQL, @debug
	end try
	begin catch
		print error_message()
		print @sql
	end catch
	fetch next from t_cursor into @Server, @Sql
end
close t_cursor 
deallocate t_cursor
set nocount off

if OBJECT_ID('tempdb..#errors') is not null
	drop table #errors

select *, cast(TotalRowsToday  as varchar) TotalRowsTodayFormatted
into #errors
from #t	
where (TableList like '%'+@TableName+'%')
and MinutesBehind >= case when @MinutesBehind = -1 then isnull(DelayAlertInMinutes, 30) else @MinutesBehind end
order by 1,2,3,4,5

select * from #errors

if isnull(@Email,'') <> '' and exists(select * from #errors)
begin
	DECLARE @xml NVARCHAR(MAX)=''
	DECLARE @body NVARCHAR(MAX)=''

	--open body
	SET @body = @body + '
	<html>
		<H1>gSyncReplication Latency Alert</H1>
		<body bgcolor=white>'

	--get publication in error status
	set @xml = null
	SET @xml = CAST(
		( SELECT  PublisherServer AS 'td' ,''
				, PublisherDatabase AS 'td' ,''
				, SubscriberServer AS 'td' ,''
				, SubscriberDatabase AS 'td',''
				, SubscriptionId  AS 'td' ,''
				, MinutesBehind  AS 'td' ,''
				, LastSuccessDate  AS 'td' ,''
				, LastMessage   AS 'td' --last one has no comma
			from #errors
			FOR XML PATH('tr'), ELEMENTS ) 
		AS NVARCHAR(MAX))

	if @xml is not null
		SET @body = @body + '
			<table border = 2>
				<tr>
					<th>PublisherServer</th>
					<th>PublisherDatabase</th>
					<th>SubscriberServer</th>
					<th>SubscriberDatabase</th>
					<th>SubscriptionId</th>
					<th>MinutesBehind</th>
					<th>LastSuccessDate</th>
					<th>LastMessage</th>					
				</tr>
				' + @xml +'
			</table>'


	--close body
	SET @body = @body + '
		</body>
	</html>'

	print @body

	if len (@body) > 100
		EXEC msdb.dbo.sp_send_dbmail 
		 @recipients =@Email
		,@body = @body
		,@body_format ='HTML'
		,@subject ='Replication Latency Alert'
		,@profile_name ='DBAs'
end


GO
/****** Object:  StoredProcedure [dbo].[spReplicationCheckDetail]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE proc [dbo].[spReplicationCheckDetail] (@ServerName varchar(100)='%', @DatabaseName varchar(100)='%', @TableName varchar(100)='%', @MinRows int=1)
as

if 1=2
 select ServerName,	DatabaseName,	StartDate,	RvFrom,	SchemaName,	TableName,	TotalRows from vwRplImportLogDetail


declare @sql varchar(max)
	set @sql = 'select @@ServerName ServerName,	DatabaseName,	StartDate,	RvFrom,	SchemaName,	TableName,	TotalRows from ['+@DatabaseName+'].rpl.vwImportLogDetail where StartDate>= cast(getdate() as date) and TableName like '''+ @TableName + ''' and TotalRows >= '+cast(@MinRows as varchar) + ' order by TotalRows desc '

	SET @SQL = 'SELECT a.*	FROM OPENQUERY(['+@ServerName+'], 
		'''+replace(@sql, '''', '''''')+'''
		) AS a;'
	begin try
		exec (@SQL)
	end try
	begin catch
		print error_message()
		print @sql
	end catch

GO
/****** Object:  StoredProcedure [dbo].[spReportDeadLock]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE   proc [dbo].[spReportDeadLock] (@ServerId int=0, @StartDate date='1/1/2000', @EndDate date='1/1/2000', @PurposeId int=0, @EnvironmentId int=0, @Dbname varchar(100) = '%', @Search varchar(100) = '%')
as
select  *
 from vwdeadlocks d
where (d.serverid = @ServerId or @ServerId=0)
and (Event_Timestamp >= @StartDate)
and (Event_Timestamp <= @EndDate)
and (d.PurposeId = @PurposeId or @PurposeId=0)
and (d.EnvironmentId = @EnvironmentId or @EnvironmentId=0)
and dbname like @dbname
and code like '%'+@Search+'%' 
order by Event_Timestamp desc
GO
/****** Object:  StoredProcedure [dbo].[spReportDeadLockDbSummary]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE   proc [dbo].[spReportDeadLockDbSummary] (@ServerId int=0, @StartDate date='1/1/2000', @EndDate date='1/1/2000', @PurposeId int=0, @EnvironmentId int=0, @Dbname varchar(100) = '%', @Search varchar(100) = '%')
as
select servername, dbname,  obj, count(*) cnt
from vwDeadlocks d
where (d.serverid = @ServerId or @ServerId=0)
and (Event_Timestamp >= @StartDate)
and (Event_Timestamp <= @EndDate)
and (d.PurposeId = @PurposeId or @PurposeId=0)
and (d.EnvironmentId = @EnvironmentId or @EnvironmentId=0)
and dbname like @dbname
and code like '%'+@Search+'%' 
group by servername, dbname,  obj

GO
/****** Object:  StoredProcedure [dbo].[spReportDeadLockSummary]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



CREATE   proc [dbo].[spReportDeadLockSummary] (@ServerId int=0, @StartDate date='1/1/2000', @EndDate date='1/1/2000', @PurposeId int=0, @EnvironmentId int=0, @Dbname varchar(100) = '%', @Search varchar(100) = '%')
as
select  DISTINCT CAST (Event_Timestamp AS date) [Date], COUNT(*) Cnt
 from vwdeadlocks d
where (d.serverid = @ServerId or @ServerId=0)
and (Event_Timestamp >= @StartDate)
and (Event_Timestamp <= @EndDate)
and (d.PurposeId = @PurposeId or @PurposeId=0)
and (d.EnvironmentId = @EnvironmentId or @EnvironmentId=0)
and dbname like @dbname
and code like '%'+@Search+'%' 
GROUP BY CAST (Event_Timestamp AS date) 
ORDER BY CAST (Event_Timestamp AS date) 

GO
/****** Object:  StoredProcedure [dbo].[spReportDisableServer]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
create proc [dbo].[spReportDisableServer] @ServerId int=0
as
Update Servers set DailyChecks=1- DailyChecks  where ServerId = @ServerId
Select * from Servers where ServerId = @ServerId

GO
/****** Object:  StoredProcedure [dbo].[spReportPerfMon]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



CREATE proc [dbo].[spReportPerfMon] (
	@PurposeId int=0,
	@EnvironmentId int=0,
	@ServerId int=0,
	@StartDate date='1/1/2014',
	@EndDate date='1/1/2016',
	@StartTime int=5,
	@EndTime int=23,
	@PLE int = 99999
)
as

declare @sql varchar(max)


if @EndDate < @StartDate
	set @EndDate = @StartDate	

set @sql = '
select distinct servername
	, case when datepart (hh, MetricTime) < 10 then ''0'' else '''' end + cast(datepart (hh, MetricTime) as varchar) + '':''+ cast(datepart (mi, MetricTime)/ 10 as varchar) + ''0''  Hr
	
	, cast(avg(PageLifeExpectancy)			as int)	PageLifeExpectancy
	, cast(avg(D_AvgDiskQueueLength)		as int)	D_AvgDiskQueueLength
	, cast(avg(PercentageProcessorTime)		as int)	PercentageProcessorTime

	, cast(avg(PageReadsPerSec)				as int)	PageReadsPerSec
	, cast(avg(PageWritesPerSec)			as int)	PageWritesPerSec
	, cast(avg(LazyWritesPerSec)			as int)	LazyWritesPerSec
	, cast(avg(D_AvgDiskSecPerRead)*1000	as int)	D_AvgDiskSecPerRead
	, cast(avg(D_AvgDiskSecPerWrite)*1000	as int)	D_AvgDiskSecPerWrite

from perfmon p
join servers s on s.ServerId = p.serverid
where 1 =1 
'

if @PurposeId > 0
	set @sql = @sql + ' and s.PurposeId = ' + cast(@PurposeId  as varchar)
if @EnvironmentId > 0
	set @sql = @sql + ' and s.EnvironmentId = ' + cast(@EnvironmentId  as varchar)
if @ServerId > 0
	set @sql = @sql + ' and s.ServerId = ' + cast(@ServerId  as varchar)
if @StartDate is not null
	set @sql = @sql + ' and MetricDate >= ''' + cast(@StartDate  as varchar)+''''
if @EndDate is not null
	set @sql = @sql + ' and MetricDate <= ''' + cast(@EndDate  as varchar)+''''
if @StartTime is not null
	set @sql = @sql + ' and datepart (hh, MetricTime) >= ' + cast(@StartTime  as varchar)
if @EndTime is not null
	set @sql = @sql + ' and datepart (hh, MetricTime) <= ' + cast(@EndTime  as varchar)

set @sql = @sql + '
group by servername
	, case when datepart (hh, MetricTime) < 10 then ''0'' else '''' end + cast(datepart (hh, MetricTime) as varchar) + '':''+ cast(datepart (mi, MetricTime)/ 10 as varchar) + ''0''
 having avg(PageLifeExpectancy) < '+cast(@PLE as varchar)+'
order by servername, hr
 '
 exec (@sql)

if @@error<>0 
	print @sql


GO
/****** Object:  StoredProcedure [dbo].[spReportPerfMonApp]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE proc [dbo].[spReportPerfMonApp] (
	@PurposeId int=0,
	@EnvironmentId int=0,
	@ServerId int=0,
	@StartDate date='1/1/2014',
	@EndDate date='1/1/2016',
	@StartTime int=5,
	@EndTime int=23,
	@TimeWindow varchar(30)='Hour'
)
as

declare @sql varchar(max), @TimeSql varchar(max)

if @TimeWindow = 'Hour'
	set @TimeSql = 'case when datepart (hh, MetricTime) < 10 then ''0'' else '''' end + cast(datepart (hh, MetricTime) as varchar) + '':''+ cast(datepart (mi, MetricTime)/ 10 as varchar) + ''0'''

if @TimeWindow = 'Day'
	set @TimeSql = '12'

set @sql = '
select distinct servername
	, MetricDate
	,  '+@TimeSql+'  [Time]
	
	, cast(avg(TotalCommittedBytes)/1024/1024/1024			as numeric(9,2))	TotalCommittedGB
	, cast(avg(ApplicationRestarts)		as int)	ApplicationRestarts
	, cast(avg(RequestWaitTime)		as int)	RequestWaitTime

	, cast(avg(RequestsQueued)				as int)	RequestsQueued
	, cast(avg(RequestsPerSec)			as int)	RequestsPerSec
	, cast(avg(C_PencentageDiskTime)			as int)	C_PencentageDiskTime
	, cast(avg(D_PercentageDiskTime)	as int)	D_PercentageDiskTime
	, cast(avg(PhisicalPercentageDiskTime)	as int)	PhisicalPercentageDiskTime
	, cast(avg(MemoryAvailableMBytes/1024)	as int)	MemoryAvailableGB


	, cast(avg(MemoryPagesPerSec)	as int)	MemoryPagesPerSec
	, cast(avg(ProcessorQueueLength)	as int)	ProcessorQueueLength
	, cast(avg(PercentageProcessorTime)	as int)	PercentageProcessorTime
	, cast(avg(CurrentConnections)	as int)	CurrentConnections
	, cast(avg(PostRequestsPerSec)	as int)	PostRequestsPerSec
	, cast(avg(NetworkBytesTotalPerSec/1024/1024)	as int)	NetworkTotalMBPerSec
from perfmonapp p
join servers s on s.ServerId = p.serverid 
where 1=1 '

if @PurposeId > 0
	set @sql = @sql + ' and s.PurposeId = ' + cast(@PurposeId  as varchar)
if @EnvironmentId > 0
	set @sql = @sql + ' and s.EnvironmentId = ' + cast(@EnvironmentId  as varchar)
if @ServerId > 0
	set @sql = @sql + ' and s.ServerId = ' + cast(@ServerId  as varchar)
if @StartDate is not null
	set @sql = @sql + ' and MetricDate >= ''' + cast(@StartDate  as varchar)+''''
if @EndDate is not null
	set @sql = @sql + ' and MetricDate <= ''' + cast(@EndDate  as varchar)+''''
if @StartTime is not null
	set @sql = @sql + ' and datepart (hh, MetricTime) >= ' + cast(@StartTime  as varchar)
if @EndTime is not null
	set @sql = @sql + ' and datepart (hh, MetricTime) <= ' + cast(@EndTime  as varchar)

set @sql = @sql + '
group by servername, MetricDate
	, '+@TimeSql+'
order by servername, [time]
 '
 exec (@sql)

if @@error<>0 
	print @sql



GO
/****** Object:  StoredProcedure [dbo].[spReportPerfMonServerSummary]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE proc [dbo].[spReportPerfMonServerSummary] (
	@PurposeId int=0,
	@EnvironmentId int=0,
	@ServerId int=0,
	@StartDate date='1/1/2014',
	@EndDate date='1/1/2016',
	@StartTime int=5,
	@EndTime int=23
)
as

declare @sql varchar(max)


if @EndDate < @StartDate
	set @EndDate = @StartDate	

set @sql = '

select servername, avg(D_AvgDiskQueueLength) D_AvgDiskQueueLength
	, avg(PageLifeExpectancy) PageLifeExpectancy
	, avg(D_AvgDiskSecPerWrite)  D_AvgDiskSecPerWrite
from perfmon p
join servers s on s.ServerId = p.serverid
where 1 =1 
'

if @PurposeId > 0
	set @sql = @sql + ' and s.PurposeId = ' + cast(@PurposeId  as varchar)
if @EnvironmentId > 0
	set @sql = @sql + ' and s.EnvironmentId = ' + cast(@EnvironmentId  as varchar)
if @ServerId > 0
	set @sql = @sql + ' and s.ServerId = ' + cast(@ServerId  as varchar)
if @StartDate is not null
	set @sql = @sql + ' and MetricDate >= ''' + cast(@StartDate  as varchar)+''''
if @EndDate is not null
	set @sql = @sql + ' and MetricDate <= ''' + cast(@EndDate  as varchar)+''''
if @StartTime is not null
	set @sql = @sql + ' and datepart (hh, MetricTime) >= ' + cast(@StartTime  as varchar)
if @EndTime is not null
	set @sql = @sql + ' and datepart (hh, MetricTime) <= ' + cast(@EndTime  as varchar)

set @sql = @sql + '
group by servername
order by servername
 '
 exec (@sql)

if @@error<>0 
	print @sql



GO
/****** Object:  StoredProcedure [dbo].[spReportReplicationVolume]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE proc [dbo].[spReportReplicationVolume] (
	 @Server varchar(100) ='Distdb1Lvv'
	, @Database varchar(100) = 'catalog'
	, @Date date = '1/1/1900' 
	)
as
begin

	select CONVERT(VARCHAR(4), StartDate, 108) + '0' Time, TableName, sum(TotalRows) TotalRows, sum(TotalKbs)/1024 TotalMbs
	from vwRplImportLogDetail l
	where l.ServerName = @Server 
	and l.DatabaseName = @Database
	and l.StartDate between @Date and dateadd(dd, 1, @Date)
	group by CONVERT(VARCHAR(4), StartDate, 108)+ '0', TableName
	order by 1, 4 desc
end

GO
/****** Object:  StoredProcedure [dbo].[spRunSql]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE proc [dbo].[spRunSql] @cmd nvarchar(max), @where nvarchar(max)='isActive=1' 
AS
BEGIN 

declare @SERVER VARCHAR(100), @serverid int
declare @sql nvarchar(max) = '
 select servername, serverid
 from vwservers
 where '+@where+'
 order by servername'

if OBJECT_ID('tempdb..#servers') is not null
	drop table #servers

create table #servers (
	servername varchar(100),
	serverid int
)

insert into #servers
exec (@sql)

DECLARE T_CURSOR CURSOR FAST_FORWARD FOR
	SELECT SERVERNAME, serverid
	FROM #servers
OPEN T_CURSOR
FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid
WHILE @@FETCH_STATUS=0
BEGIN
	if @SERVER = @@SERVERNAME
		set @SERVER = '.' 

	SET @SQL = 'exec ('''+replace(@cmd, '''', '''''')+''') at ['+@SERVER+']'

	BEGIN TRY
		exec (@sql)
	END TRY
    BEGIN CATCH
		print @server
		PRINT @sql
		PRINT ERROR_MESSAGE()
	END catch
	FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid
END
CLOSE T_CURSOR
DEALLOCATE T_CURSOR
	
END

GO
/****** Object:  StoredProcedure [dbo].[spRunSqlDb]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE   proc [dbo].[spRunSqlDb] @cmd nvarchar(max), @where nvarchar(max)='isActive=1' 
AS
BEGIN 

declare @SERVER VARCHAR(100), @serverid int, @DatabaseName varchar(100), @DatabaseId int
 , @Version	varchar	(255), @linkedserver varchar(255), @EngineEdition varchar(50)

declare @sql nvarchar(max) = '
 select servername, serverid, DatabaseName, DatabaseId , Version , EngineEdition
 --select * 
 from vwDatabases
 where '+@where+'
 order by servername, DatabaseName'

if OBJECT_ID('tempdb..#databases') is not null
	drop table #databases

create table #databases (
	servername varchar(100),
	serverid int,
	databasename varchar(100),
	databaseid int
	, Version varchar(255)
	, EngineEdition varchar(50)
)

BEGIN TRY
	insert into #databases
	exec (@sql)
END TRY
  BEGIN CATCH
	PRINT @sql
	PRINT ERROR_MESSAGE()
END catch


DECLARE T_CURSOR CURSOR FAST_FORWARD FOR
	SELECT * FROM #databases
OPEN T_CURSOR
FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid, @DatabaseName, @DatabaseId, @Version , @EngineEdition
WHILE @@FETCH_STATUS=0
BEGIN
	if @SERVER = @@SERVERNAME
		set @SERVER = '.' 
	else if @DatabaseName = 'master' or @EngineEdition <> 'SQL Azure'
		set @linkedserver = @server
	else 
		set @linkedserver = @server+'.'+@DatabaseName
	
	set @sql = replace(replace(@cmd, '#database#', @DatabaseName), '''', '''''')

	--SET @SQL = 'exec ('''+replace(@cmd, '''', '''''')+''') at ['+@linkedserver+']'
	SET @sql = 'SELECT '''+@SERVER+''' [servername], '''+@Databasename+''' [databasename], *
				FROM OPENQUERY(['+@linkedserver+'], 
				'''+@sql+''') AS a
				;'


	BEGIN TRY
		exec (@sql)
	END TRY
    BEGIN CATCH
		PRINT @sql
		PRINT ERROR_MESSAGE()
	END catch
	FETCH NEXT FROM T_CURSOR INTO @SERVER, @serverid, @DatabaseName, @DatabaseId, @Version , @EngineEdition
END
CLOSE T_CURSOR
DEALLOCATE T_CURSOR
	
END
GO
/****** Object:  StoredProcedure [dbo].[spSaveHistTables]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO





CREATE   proc [dbo].[spSaveHistTables]
as

	
	INSERT INTO [dbo].[ServersHist]
           ([PurposeName]
           ,[EnvironmentName]
           ,[ServerName]
           ,[ServerDescription]
           ,[WindowsRelease]
           ,[CreatedDate]
           ,[Version]
           ,[Edition]
           ,[Collation]
           ,[LogicalCPUCount]
           ,[HyperthreadRatio]
           ,[PhysicalCPUCount]
           ,[PhysicalMemoryMB]
           ,[VMType]
           ,[Hardware]
           ,[ProcessorNameString]
           ,[BlockedProcessEvents]
           ,[DeadlockEvents]
           ,[ErrorEvents]
           ,[LongQueryEvents]
           ,[PerfMonLogs]
           ,[IsActive]
           ,[IP]
           ,[Error]
           ,[BackupFolder]
           ,[DailyChecks]
           ,[Domain]
           ,[BackupChecks]
           ,[Build]
           ,[ErrorDate]
           ,[resource_governor_enabled_functions]
           ,[RemoteUser]
           ,[MajorVersion]
           ,[ReleaseDate]
           ,[service_account]
           ,[last_startup_time]
           ,[NodeName]
           ,[Logins]
           ,[SysAdmins]
           ,[NTUsers]
           ,[NTGroups]
           ,[Databases]
           ,[DataMB]
           ,[LogMB]
           ,[CachedSizeMbs]
           ,[CPUTime]
           ,[GSync_Published_Dbs]
           ,[GSync_SubscribedDbs]
           ,[AvailabilityGroups]
           ,[PrimaryServer]
           ,[ReplicaServer]
           ,[Volumes]
           ,[TotalGB]
           ,[AvailableGB]
           ,[Jobs]
           ,[PublicationArticles]
           ,[SubscriptionArticles]
           ,[top_wait]
           ,[TopSql]
           ,[providername]
           ,[datasource]
           ,[location]
           ,[providerstring])
     select [PurposeName]
           ,[EnvironmentName]
           ,[ServerName]
           ,[ServerDescription]
           ,[WindowsRelease]
           ,[CreatedDate]
           ,[Version]
           ,[Edition]
           ,[Collation]
           ,[LogicalCPUCount]
           ,[HyperthreadRatio]
           ,[PhysicalCPUCount]
           ,[PhysicalMemoryMB]
           ,[VMType]
           ,[Hardware]
           ,[ProcessorNameString]
           ,[BlockedProcessEvents]
           ,[DeadlockEvents]
           ,[ErrorEvents]
           ,[LongQueryEvents]
           ,[PerfMonLogs]
           ,[IsActive]
           ,[IP]
           ,[Error]
           ,[BackupFolder]
           ,[DailyChecks]
           ,[Domain]
           ,[BackupChecks]
           ,[Build]
           ,[ErrorDate]
           ,[resource_governor_enabled_functions]
           ,[RemoteUser]
           ,[MajorVersion]
           ,[ReleaseDate]
           ,[service_account]
           ,[last_startup_time]
           ,[NodeName]
           ,[Logins]
           ,[SysAdmins]
           ,[NTUsers]
           ,[NTGroups]
           ,[Databases]
           ,[DataMB]
           ,[LogMB]
           ,[CachedSizeMbs]
           ,[CPUTime]
           ,[GSync_Published_Dbs]
           ,[GSync_SubscribedDbs]
           ,[AvailabilityGroups]
           ,[PrimaryServer]
           ,[ReplicaServer]
           ,[Volumes]
           ,[TotalGB]
           ,[AvailableGB]
           ,[Jobs]
           ,[PublicationArticles]
           ,[SubscriptionArticles]
           ,[top_wait]
           ,[TopSql]
           ,[providername]
           ,[datasource]
           ,[location]
           ,[providerstring]
	from vwServers


	INSERT INTO [dbo].[VolumesHist]
           ([ServerName]
           ,[volume_mount_point]
           ,[TotalGB]
           ,[AvailableGB]
           ,[PercentageFree])
    select [ServerName]
           ,[volume_mount_point]
           ,[TotalGB]
           ,[AvailableGB]
           ,[PercentageFree]
	from vwVolumes

	INSERT INTO [dbo].[DatabasesHist]
           ([PurposeName]
           ,[EnvironmentName]
           ,[ServerName]
           ,[IsActive]
           ,[DailyChecks]
           ,[Version]
           ,[EngineEdition]
           ,[datasource]
           ,[location]
           ,[DatabaseName]
           ,[DataMB]
           ,[DataMB_S]
           ,[LogMB]
           ,[State_Desc]
           ,[RecoveryModel]
           ,[LogSizeKB]
           ,[LogUsedKB]
           ,[LogUsedPercentage]
           ,[DBCompatibilityLevel]
           ,[PageVerifyOption]
           ,[is_auto_create_stats_on]
           ,[is_auto_update_stats_on]
           ,[is_auto_update_stats_async_on]
           ,[is_parameterization_forced]
           ,[snapshot_isolation_state_desc]
           ,[is_read_committed_snapshot_on]
           ,[is_auto_close_on]
           ,[is_auto_shrink_on]
           ,[target_recovery_time_in_seconds]
           ,[Create_Date]
           ,[is_published]
           ,[is_subscribed]
           ,[Collation]
           ,[CachedSizeMbs]
           ,[CPUTime]
           ,[Is_Read_Only]
           ,[delayed_durability_desc]
           ,[containment_desc]
           ,[is_cdc_enabled]
           ,[is_broker_enabled]
           ,[is_encrypted]
           ,[is_memory_optimized_elevate_to_snapshot_on]
           ,[AvailabilityGroup]
           ,[PrimaryReplicaServerName]
           ,[LocalReplicaRole]
           ,[SynchronizationState]
           ,[IsSuspended]
           ,[IsJoined]
           ,[SourceDatabaseName]
           ,[owner]
           ,[mirroring_state]
           ,[mirroring_role]
           ,[mirroring_safety_level]
           ,[mirroring_partner]
           ,[mirroring_partner_instance]
           ,[mirroring_witness]
           ,[mirroring_witness_state]
           ,[mirroring_connection_timeout]
           ,[mirroring_redo_queue]
           ,[edition]
           ,[service_objective]
           ,[elastic_pool_name]
           ,[SLO_Name]
           ,[Service_Tier]
           ,[Hardware_Gen]
           ,[Logical_CPUs]
           ,[Memory]
           ,[DTUs]
           ,[CPUs]
           ,[Files]
           ,[FileGroups]
           ,[Users]
           ,[DBO_Users]
           ,[Rows]
           ,[Tables]
           ,[Views]
           ,[Procs]
           ,[Functions]
           ,[Gsync_Published_Tables]
           ,[Gsync_Subscribed_Tables]
           ,[GsyncReplication_LastSuccess]
           ,[GsyncReplication_Lag]
           ,[MS_Published_Tables]
           ,[MS_Subscribed_Tables]
           ,[Indexes]
           ,[Index_data_size]
           ,[Index_used_size])
    select [PurposeName]
           ,[EnvironmentName]
           ,[ServerName]
           ,[IsActive]
           ,[DailyChecks]
           ,[Version]
           ,[EngineEdition]
           ,[datasource]
           ,[location]
           ,[DatabaseName]
           ,[DataMB]
           ,[DataMB_S]
           ,[LogMB]
           ,[State_Desc]
           ,[RecoveryModel]
           ,[LogSizeKB]
           ,[LogUsedKB]
           ,[LogUsedPercentage]
           ,[DBCompatibilityLevel]
           ,[PageVerifyOption]
           ,[is_auto_create_stats_on]
           ,[is_auto_update_stats_on]
           ,[is_auto_update_stats_async_on]
           ,[is_parameterization_forced]
           ,[snapshot_isolation_state_desc]
           ,[is_read_committed_snapshot_on]
           ,[is_auto_close_on]
           ,[is_auto_shrink_on]
           ,[target_recovery_time_in_seconds]
           ,[Create_Date]
           ,[is_published]
           ,[is_subscribed]
           ,[Collation]
           ,[CachedSizeMbs]
           ,[CPUTime]
           ,[Is_Read_Only]
           ,[delayed_durability_desc]
           ,[containment_desc]
           ,[is_cdc_enabled]
           ,[is_broker_enabled]
           ,[is_encrypted]
           ,[is_memory_optimized_elevate_to_snapshot_on]
           ,[AvailabilityGroup]
           ,[PrimaryReplicaServerName]
           ,[LocalReplicaRole]
           ,[SynchronizationState]
           ,[IsSuspended]
           ,[IsJoined]
           ,[SourceDatabaseName]
           ,[owner]
           ,[mirroring_state]
           ,[mirroring_role]
           ,[mirroring_safety_level]
           ,[mirroring_partner]
           ,[mirroring_partner_instance]
           ,[mirroring_witness]
           ,[mirroring_witness_state]
           ,[mirroring_connection_timeout]
           ,[mirroring_redo_queue]
           ,[edition]
           ,[service_objective]
           ,[elastic_pool_name]
           ,[SLO_Name]
           ,[Service_Tier]
           ,[Hardware_Gen]
           ,[Logical_CPUs]
           ,[Memory]
           ,[DTUs]
           ,[CPUs]
           ,[Files]
           ,[FileGroups]
           ,[Users]
           ,[DBO_Users]
           ,[Rows]
           ,[Tables]
           ,[Views]
           ,[Procs]
           ,[Functions]
           ,[Gsync_Published_Tables]
           ,[Gsync_Subscribed_Tables]
           ,[GsyncReplication_LastSuccess]
           ,[GsyncReplication_Lag]
           ,[MS_Published_Tables]
           ,[MS_Subscribed_Tables]
           ,[Indexes]
           ,[Index_data_size]
           ,[Index_used_size]
	from vwDatabases

	INSERT INTO [dbo].[DatabaseFilesHist]
           ([ServerName]
           ,[DatabaseName]
           ,[FileName]
           ,[PhysicalName]
           ,[TotalMbs]
           ,[AvailableMbs]
           ,[fileid]
           ,[filegroupname])
     select [ServerName]
           ,[DatabaseName]
           ,[FileName]
           ,[PhysicalName]
           ,[TotalMbs]
           ,[AvailableMbs]
           ,[fileid]
           ,[filegroupname]
	from vwDatabaseFiles


	INSERT INTO [dbo].[IndexUsageHist]
           ([ServerName]
           ,[DatabaseName]
           ,[RowCount]
           ,[data_space]
           ,[allocation_desc]
           ,[table_schema]
           ,[object_type]
           ,[table_name]
           ,[index_type]
           ,[index_name]
           ,[is_unique]
           ,[is_disabled]
           ,[database_file]
           ,[size_mbs]
           ,[used_size]
           ,[data_size]
           ,[writes]
           ,[reads]
           ,[index_id]
           ,[fill_factor]
           ,[avg_fragmentation_in_percent]
           ,[cols]
           ,[included]
           ,[filter_definition]
           ,[data_compression_desc]
           ,[ColCount])
    select [ServerName]
           ,[DatabaseName]
           ,[RowCount]
           ,[data_space]
           ,[allocation_desc]
           ,[table_schema]
           ,[object_type]
           ,[table_name]
           ,[index_type]
           ,[index_name]
           ,[is_unique]
           ,[is_disabled]
           ,[database_file]
           ,[size_mbs]
           ,[used_size]
           ,[data_size]
           ,[writes]
           ,[reads]
           ,[index_id]
           ,[fill_factor]
           ,[avg_fragmentation_in_percent]
           ,[cols]
           ,[included]
           ,[filter_definition]
           ,[data_compression_desc]
           ,[ColCount]
		from vwIndexUsage
		where [allocation_desc] = 'In_Row_Data'
GO
/****** Object:  StoredProcedure [dbo].[spSearch]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




CREATE PROC [dbo].[spSearch] @s VARCHAR(100), @serverid INT=0
AS


select  'Servers' Scope, * 
from vwServers
where ServerName like @s
and (@serverid = 0 or serverid=@serverid)
order by servername

select  'Databases' Scope, * 
from vwDatabases
where DatabaseName like @s
and (@serverid = 0 or serverid=@serverid)
order by servername, DatabaseName

select  'DatabaseObjects' Scope,* 
from [vwDatabaseObjects]
where ObjectName like @s
and (@serverid = 0 or serverid=@serverid)
order by servername, DatabaseName, xtype, SchemaName, ObjectName

select  'DatabaseObjectColumns' Scope,* 
from [vwDatabaseObjectColumns]
where (COLUMN_NAME like @s)
and (@serverid = 0 or serverid=@serverid)
order by servername, DatabaseName, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME

select  'IndexUsage' Scope,* 
from [vwIndexUsage]
where (Cols like @s
or Included like @s
or Table_Name like @s)
and (@serverid = 0 or serverid=@serverid)
order by servername, DatabaseName, TABLE_SCHEMA, TABLE_NAME

SELECT  'MissingIndexes' Scope,* 
FROM [vwMissingIndexes]
WHERE TableName LIKE @s
AND (@serverid = 0 OR serverid=@serverid)
ORDER BY servername, DatabaseName, TableName

/*
select 'Microsoft Replication', * 
from vwReplication
where (Distribution_db like @s
or publisher_db like @s
or source_object like @s
or subscriber_server like @s
or subscriber_db like @s
or destination_object like @s)
and (@serverid = 0 or serverid=@serverid)
order by PublisherName, publisher_db, source_owner, source_object

*/
SELECT 'Jobs' Scope,* FROM vwJobs
WHERE Jobname LIKE @s
AND (@serverid = 0 OR serverid=@serverid)

SELECT 'JobSteps' Scope,* FROM vwJobSteps
WHERE command LIKE @s
AND (@serverid = 0 OR serverid=@serverid)



GO
/****** Object:  StoredProcedure [dbo].[spSearchADGroup]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE   proc [dbo].[spSearchADGroup] (@login varchar(100)='')
as

select 'ADGroupMembers' Scope, * from ADGroupMembers 
where permission_path like @login
order by account

select 'Logins' Scope,* from vwlogins 
where loginname like @login
and isntgroup=1
order by ServerName, LoginName

select 'DatabasePerms' Scope,* from vwDatabasePerms
where loginname like @login
and isntgroup =1
order by ServerName, LoginName, DatabaseName

select 'DatabaseObjectPerms' Scope,* from vwDatabaseObjectPerms
where loginname like @login
and isntgroup =1
order by ServerName, LoginName, DatabaseName, type_desc, ObjectName

GO
/****** Object:  StoredProcedure [dbo].[spSearchDatabase]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE   PROC [dbo].[spSearchDatabase] @DatabaseName VARCHAR(100), @ServerName VARCHAR(100)='%' 
AS

SELECT 'Databases' Scope,* FROM [dbo].[vwDatabases] WHERE ServerName LIKE  @ServerName and DatabaseName LIKE  @DatabaseName order by ServerName, DatabaseName

SELECT top 100 'TopTablesBySize' Scope,* FROM [dbo].[vwIndexUsage] WHERE ServerName LIKE  @ServerName and DatabaseName LIKE  @DatabaseName order by size_mbs desc
SELECT top 100 'TopTablesByReads' Scope,* FROM [dbo].[vwIndexUsage] WHERE ServerName LIKE  @ServerName and DatabaseName LIKE  @DatabaseName order by reads desc
SELECT top 100 'TopTablesByWrites' Scope,* FROM [dbo].[vwIndexUsage] WHERE ServerName LIKE  @ServerName and DatabaseName LIKE  @DatabaseName order by writes desc

SELECT 'DatabaseObjects' Scope,* FROM [dbo].[vwDatabaseObjects] WHERE ServerName LIKE  @ServerName and DatabaseName LIKE  @DatabaseName order by ServerName, DatabaseName, Xtype, [RowCount] desc, SchemaName, ObjectName

SELECT 'DatabasesPerms' Scope,* FROM [dbo].[vwDatabasePerms] WHERE ServerName LIKE  @ServerName and DatabaseName LIKE  @DatabaseName order by ServerName, DatabaseName, LoginName

SELECT 'DatabaseFiles' Scope,* FROM [dbo].[vwDatabaseFiles] WHERE ServerName LIKE  @ServerName and DatabaseName LIKE  @DatabaseName order by ServerName, DatabaseName, fileid

SELECT 'TopSql' Scope,* FROM [dbo].[vwTopSql]  WHERE ServerName LIKE  @ServerName and DatabaseName LIKE  @DatabaseName order by ServerName, TotalWorkerTime desc

SELECT 'MissingIndexes' Scope,* FROM [dbo].[vwMissingIndexes]  WHERE ServerName LIKE  @ServerName and DatabaseName LIKE  @DatabaseName order by ServerName desc

SELECT 'IndexUsage' Scope,* FROM [dbo].[vwIndexUsage]  WHERE ServerName LIKE  @ServerName and DatabaseName LIKE  @DatabaseName order by ServerName desc


SELECT 'Backups' Scope,* FROM [dbo].[vwBackups]  WHERE ServerName LIKE  @ServerName and DatabaseName LIKE  @DatabaseName order by ServerName, BackupFolder


GO
/****** Object:  StoredProcedure [dbo].[spSearchLogin]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE proc [dbo].[spSearchLogin] (@login varchar(100)='')
as

select 'ADGroupMembers' Scope,* from ADGroupMembers 
where account like @login
order by permission_path

select 'Logins' Scope,* from vwLogins 
where loginname like @login
and isntgroup=0
order by ServerName, LoginName

select 'DatabasePerms' Scope,* from vwDatabasePerms
where loginname like @login
and isntgroup =0
order by ServerName, LoginName, DatabaseName

select 'DatabaseObjectPerms' Scope,* from vwDatabaseObjectPerms
where loginname like @login
and isntgroup =0
order by ServerName, LoginName, DatabaseName, type_desc, ObjectName

GO
/****** Object:  StoredProcedure [dbo].[spSearchServer]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE   PROC [dbo].[spSearchServer] @ServerName VARCHAR(100)
AS

SELECT 'Servers' Scope,* FROM dbo.vwServers WHERE ServerName LIKE  @ServerName order by ServerName

SELECT 'Logins' Scope,* FROM  [dbo].[vwLogins] WHERE ServerName LIKE  @ServerName order by ServerName, sysadmin desc, LoginName
SELECT 'ServerPerms' Scope,* FROM [dbo].[vwServerPerms] WHERE ServerName LIKE  @ServerName
SELECT 'Volumes' Scope,* FROM [dbo].[vwVolumes]  WHERE ServerName LIKE  @ServerName order by ServerName, Volume_Mount_Point


SELECT 'Databases' Scope,* FROM [dbo].[vwDatabases] WHERE ServerName LIKE  @ServerName order by ServerName, DataMB desc, DatabaseName

SELECT top 20 'TopTables' Scope,* FROM [dbo].[vwDatabaseObjects] WHERE ServerName LIKE  @ServerName and xtype='u' order by [RowCount] desc

SELECT top 20 'TopIndexesBySize' Scope,* FROM [dbo].[vwIndexUsage] WHERE ServerName LIKE  @ServerName order by ServerName, size_mbs desc
SELECT top 20 'TopIndexesByReads' Scope,* FROM [dbo].[vwIndexUsage] WHERE ServerName LIKE  @ServerName order by ServerName, reads desc
SELECT top 20 'TopIndexesByWrites' Scope,* FROM [dbo].[vwIndexUsage] WHERE ServerName LIKE  @ServerName order by ServerName, writes desc

SELECT 'DatabasesPerms' Scope,* FROM [dbo].[vwDatabasePerms] WHERE ServerName LIKE  @ServerName order by ServerName, DatabaseName, DB_OWNER desc, LoginName

SELECT 'AvailabilityGroups' Scope,* FROM [dbo].[vwAvailabilityGroups]  WHERE ServerName LIKE  @ServerName order by ServerName, AvailabiityGroup
SELECT 'ClusterNodes' Scope,* FROM [dbo].[vwClusterNodes]  WHERE ServerName LIKE  @ServerName order by ServerName

SELECT 'Services' Scope,* FROM [dbo].[vwServices] WHERE ServerName LIKE  @ServerName order by ServerName, servicename

SELECT 'DatabaseFiles' Scope,* FROM [dbo].[vwDatabaseFiles] WHERE ServerName LIKE  @ServerName order by ServerName, TotalMbs desc


SELECT 'TopSql' Scope,* FROM [dbo].[vwTopSql]  WHERE ServerName LIKE  @ServerName order by ServerName, TotalWorkerTime desc
SELECT 'TopWait' Scope,* FROM [dbo].[vwTopWait]  WHERE ServerName LIKE  @ServerName order by ServerName, percent_total_waits desc

SELECT 'MissingIndexes' Scope,* FROM [dbo].[vwMissingIndexes]  WHERE ServerName LIKE  @ServerName order by ServerName, index_advantage desc


SELECT 'Jobs' Scope,* FROM [dbo].[vwJobs]  WHERE ServerName LIKE  @ServerName order by ServerName, Jobname
SELECT 'Backups' Scope,* FROM [dbo].[vwBackups]  WHERE ServerName LIKE  @ServerName order by ServerName, BackupFolder


GO
/****** Object:  StoredProcedure [dbo].[spSearchTable]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE PROC [dbo].[spSearchTable] @s VARCHAR(100)
	, @schema varchar(100) = '%'
	, @server varchar(100) = '%'
	, @database varchar(100) = '%'
AS

select  'DatabaseObjects' Scope,* 
from [vwDatabaseObjects]
where ObjectName like @s
and SchemaName like @schema
and servername like @server
and DatabaseName like @database
order by servername, DatabaseName, xtype, SchemaName, ObjectName

select  'DatabaseObjectColumns' Scope,* 
from [vwDatabaseObjectColumns]
where Table_NAME like @s
and TABLE_SCHEMA like @schema
and servername like @server
and DatabaseName like @database
order by servername, DatabaseName, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME

select  'IndexUsage' Scope,* 
from [vwIndexUsage]
where Table_Name like @s
and TABLE_SCHEMA like @schema
and servername like @server
and DatabaseName like @database
order by servername, DatabaseName, TABLE_SCHEMA, TABLE_NAME

SELECT  'MissingIndexes' Scope,* 
FROM [vwMissingIndexes]
WHERE TableName LIKE '%' + @s + '%'
and SchemaOnly like @schema
and servername like @server
and DatabaseName like @database
ORDER BY servername, DatabaseName, TableName

SELECT  'DatabaseObjectPerms' Scope,* 
FROM [vwDatabaseObjectPerms]
WHERE ObjectName LIKE '%' + @s + '%'
and servername like @server
and DatabaseName like @database
ORDER BY servername, DatabaseName, ObjectName, USERNAME

GO
/****** Object:  StoredProcedure [dbo].[spServerResponseChecks]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE   proc [dbo].[spServerResponseChecks] (@Email varchar(255) ='')
as

if OBJECT_ID('tempdb..#t') is not null
	drop table #t

create table #t (
	Message varchar(1000),
	Servername varchar(1000),
	EnvironmentName varchar(100),
	PurposeName varchar(100),
	Version varchar(255),
	Error varchar(255),
	ErrorDate Datetime,
	ServerId int
)

set nocount on

insert into #t
select 'Server is not responsive' Message, servername, EnvironmentName, PurposeName, Version, Error, ErrorDate, ServerId
from vwServers s 
WHERE s.IsActive=0 
AND DailyChecks=1
AND s.PurposeId=1
order by 2

select * from #t	

if @Email > '' and @@ROWCOUNT > 0
begin
	DECLARE @body NVARCHAR(MAX)='Server is not responsive:'+ char(13)

	--open body
	SELECT @body = @body + Servername + char(13) + 
		+'Error: '+ Error
	    +'Disable Server: "http://cmssqlp01/ReportS/report/DBA/DisableServer?ServerId='+cast(serverid as varchar)+'"'
		+ char(13) + char(13) 
	from #t
	
	print @body

	EXEC msdb.dbo.sp_send_dbmail 
		 @recipients =@Email
		,@body = @body
		,@body_format ='TEXT'
		,@subject ='DBA Server Response Check'
		,@profile_name ='mail.southernwine.com'
	
end

GO
/****** Object:  StoredProcedure [dbo].[spUpdatePublisher]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE proc [dbo].[spUpdatePublisher] @code nvarchar(max), @Debug bit=0, @Exec bit=1
as

declare @ServerName varchar(100), @DatabaseName varchar(100), @Sql varchar(max)--, @SubscriptionId varchar(10)

declare t_cursor cursor fast_forward for
	select distinct PublisherServer, PublisherDatabase 
	from [dbo].[vwRplSubscriptionTable]
open t_cursor
fetch next from t_cursor into @ServerName, @DatabaseName 
while @@FETCH_STATUS=0
begin
	set @sql = '
exec (''use ['+@DatabaseName+'] 
	exec ('''''+replace(@code, '''', '''''''''')+'
	'''') 
'') at ['+@ServerName+'] '
	exec spExec @Sql, @Debug, @Exec
	fetch next from t_cursor into @ServerName, @DatabaseName 
end
close t_cursor
deallocate t_cursor
GO
/****** Object:  StoredProcedure [dbo].[spUpdateSubscriber]    Script Date: 10/19/2022 4:18:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


create proc [dbo].[spUpdateSubscriber] @code nvarchar(max), @Debug bit=0, @Exec bit=1
as

declare @ServerName varchar(100), @DatabaseName varchar(100), @Sql varchar(max)--, @SubscriptionId varchar(10)

declare t_cursor cursor fast_forward for
	select distinct SubscriberServer, SubscriberDatabase
	from [dbo].[vwRplSubscriptionTable]
open t_cursor
fetch next from t_cursor into @ServerName, @DatabaseName 
while @@FETCH_STATUS=0
begin
	set @sql = '
exec (''use ['+@DatabaseName+'] 
	exec ('''''+replace(@code, '''', '''''''''')+'
	'''') 
'') at ['+@ServerName+'] '
	exec spExec @Sql, @Debug, @Exec
	fetch next from t_cursor into @ServerName, @DatabaseName 
end
close t_cursor
deallocate t_cursor
GO


CREATE proc [dbo].[spLoadHourly]
as
set nocount on

print '--Start'
print getdate()

exec [spLoadDataBaseFiles]

exec [spLoadVolumes]

exec [spLoadRplImportLog]
exec [spLoadRplImportLogDetail]
exec [dbo].[spLoadRplDates]

exec spLoadJobHistory



GO

CREATE proc [dbo].[spLoad] @serverid int=0
as
set nocount on

print '--Start'
print getdate()
exec spSaveHistTables 

print '--Cleanup'
if @serverid = 0
	exec spCleanup
else 
begin
	print 'Loading Server: '+ cast(@serverid as varchar)
	exec spCleanupServer @serverid = @serverid
end
print getdate()

print '--servers'
exec spLoadServers @serverid = @serverid

exec spLoadLinkedServers @serverid = @serverid

print '--Volumes'
exec spLoadVolumes @serverid = @serverid

print getdate()

print '--databases'
exec spLoadDatabases @serverid = @serverid
print getdate()

exec spLoadSQLAzure @serverid = @serverid

print '--DatabaseFiles'
exec spLoadDataBaseFiles @serverid = @serverid
print getdate()

print '--DatabaseObjects'
exec spLoadDataBaseObjects @serverid = @serverid
print getdate()

print '--DatabaseObjectColums'
exec [spLoadDataBaseObjectColums] @serverid = @serverid
print getdate()

print '--Security' 
exec spLoadLogins @serverid = @serverid
exec spLoadServerperms @serverid = @serverid
exec spLoadADGroupMembers @serverid = @serverid
exec spLoadDataBasePerms @serverid = @serverid
exec spLoadDataBaseObjectPerms @serverid = @serverid
print getdate()

print '--SQL Agent'
exec spLoadJobs @serverid = @serverid
exec spLoadJobHistory @serverid = @serverid
print getdate()

print '--SQL Statistics'
exec spLoadTopWait @serverid = @serverid
exec spLoadMissingIndexes @serverid = @serverid
exec spLoadTopSql @serverid = @serverid
exec [spLoadIndexUsage] @serverid = @serverid
print getdate()

print '--Query Store'
exec [dbo].[spLoadQueryStoreLongestAvgTimes] @serverid = @serverid
exec [dbo].[spLoadQueryStoreTopExecCounts] @serverid = @serverid
print getdate()

print '--SQL Replication'
exec [dbo].[spLoadPublishers] @serverid = @serverid
exec [dbo].[spLoadPublications] @serverid = @serverid
exec [dbo].[spLoadArticles] @serverid = @serverid
exec [dbo].[spLoadSubscriptions] @serverid = @serverid
print getdate()

print '--MSync Replication'
exec [spLoadRplSubscription] @serverid = @serverid
exec [spLoadRplImportLog] @serverid = @serverid
exec [spLoadRplImportLogDetail] @serverid = @serverid
exec spLoadRplDates 
exec spLoadRplPublicationTable @serverid = @serverid
exec spLoadRplSubscriptionRoutine @serverid = @serverid
exec spLoadRplSubscriptionTable @serverid = @serverid

print getdate()

print '--Backups'
exec [spLoadBackups]  @serverid = @serverid
exec [spLoadMsdb_Backups] @serverid = @serverid
print getdate()

print '--Perfmon'
exec spLoadPerMon @serverid = @serverid
exec spLoadPerMonApp @serverid = @serverid
print getdate()

print '--CleanOldData'
exec [spCleanOldData] 
--exec spExportDeadlocks
print getdate()

exec spLoadClusterNodes @serverid = @serverid
--exec spLoadIndexFragmentation
exec spLoadJobSteps @serverid = @serverid
exec spLoadLongSqlFiles @serverid = @serverid
exec spLoadSequences @serverid = @serverid
exec spLoadServices @serverid = @serverid
exec [spLoadAvailabilityGroups]  @serverid = @serverid


print '--Extended events - Errors'
exec spLoadErrors @serverid = @serverid
print getdate()

print '--Extended events - Deadlocks'
exec spLoadDeadlocks @serverid = @serverid
print getdate()

print '--Extended events - LongSql'
exec spLoadLongSql @serverid = @serverid
print getdate()

GO
/****** Object:  Synonym [dbo].[spd]    Script Date: 10/19/2022 4:18:32 PM ******/
CREATE SYNONYM [dbo].[spd] FOR [dbo].[spDailyChecks]
GO
/****** Object:  Synonym [dbo].[sph]    Script Date: 10/19/2022 4:18:32 PM ******/
CREATE SYNONYM [dbo].[sph] FOR [spHourlyChecks]
GO
/****** Object:  Synonym [dbo].[spr]    Script Date: 10/19/2022 4:18:32 PM ******/
CREATE SYNONYM [dbo].[spr] FOR [DBA].[dbo].[spReplicationCheck]
GO
/****** Object:  Synonym [dbo].[sps]    Script Date: 10/19/2022 4:18:32 PM ******/
CREATE SYNONYM [dbo].[sps] FOR [dbo].[spSearch]
