
/* First time we need to populate tables Purpose, Environment and Servers manually:

--Examples:
insert into purpose (purposename)values  ('Prod')
insert into purpose (purposename)values  ('QA')
insert into purpose (purposename)values  ('Dev')
insert into purpose (purposename)values  ('Demo')

insert into Environment (Environmentname)values  ('EastUS2')
insert into Environment (Environmentname)values  ('EastUS')



INSERT INTO [dbo].[Servers]
           ([EnvironmentId]
           ,[PurposeId]
           ,[ServerName]
           ,[IsActive]
           ,[RemoteUser])
     VALUES
           ( 1--[EnvironmentId]
           , 2-- [PurposeId]
           , 'cms-mi2.public.8aab675727ef.database.windows.net,3342' --[ServerName]
           ,1 -- [IsActive]
           ,'SQLAdmin'-- [RemoteUser]
		   )
*/

/*We also need to setup linked servers for eash server, in case of azure sql we need to setup a linked server for each database.

--Example:
EXEC master.dbo.sp_dropserver @server=N'cms-mi2.public.8aab675727ef.database.windows.net,3342', @droplogins='droplogins'
GO
EXEC master.dbo.sp_addlinkedserver @server = N'cms-mi2.public.8aab675727ef.database.windows.net,3342', @srvproduct=N'', @provider=N'SQLNCLI', @datasrc=N'cms-mi2.public.8aab675727ef.database.windows.net,3342'
EXEC master.dbo.sp_addlinkedsrvlogin @rmtsrvname=N'cms-mi2.public.8aab675727ef.database.windows.net,3342',@useself=N'False',@locallogin=NULL,@rmtuser=N'SQLAdmin',@rmtpassword='?'
GO

EXEC master.dbo.sp_serveroption @server=N'cms-mi2.public.8aab675727ef.database.windows.net,3342', @optname=N'collation compatible', @optvalue=N'false'
EXEC master.dbo.sp_serveroption @server=N'cms-mi2.public.8aab675727ef.database.windows.net,3342', @optname=N'data access', @optvalue=N'true'
EXEC master.dbo.sp_serveroption @server=N'cms-mi2.public.8aab675727ef.database.windows.net,3342', @optname=N'dist', @optvalue=N'false'
EXEC master.dbo.sp_serveroption @server=N'cms-mi2.public.8aab675727ef.database.windows.net,3342', @optname=N'pub', @optvalue=N'false'
EXEC master.dbo.sp_serveroption @server=N'cms-mi2.public.8aab675727ef.database.windows.net,3342', @optname=N'rpc', @optvalue=N'true'
EXEC master.dbo.sp_serveroption @server=N'cms-mi2.public.8aab675727ef.database.windows.net,3342', @optname=N'rpc out', @optvalue=N'true'
EXEC master.dbo.sp_serveroption @server=N'cms-mi2.public.8aab675727ef.database.windows.net,3342', @optname=N'sub', @optvalue=N'false'
EXEC master.dbo.sp_serveroption @server=N'cms-mi2.public.8aab675727ef.database.windows.net,3342', @optname=N'connect timeout', @optvalue=N'0'
EXEC master.dbo.sp_serveroption @server=N'cms-mi2.public.8aab675727ef.database.windows.net,3342', @optname=N'collation name', @optvalue=null
EXEC master.dbo.sp_serveroption @server=N'cms-mi2.public.8aab675727ef.database.windows.net,3342', @optname=N'lazy schema validation', @optvalue=N'false'
EXEC master.dbo.sp_serveroption @server=N'cms-mi2.public.8aab675727ef.database.windows.net,3342', @optname=N'query timeout', @optvalue=N'0'
GO

EXEC master.dbo.sp_serveroption @server=N'cms-mi2.public.8aab675727ef.database.windows.net,3342', @optname=N'use remote collation', @optvalue=N'true'
GO

EXEC master.dbo.sp_serveroption @server=N'cms-mi2.public.8aab675727ef.database.windows.net,3342', @optname=N'remote proc transaction promotion', @optvalue=N'true'
GO

exec spLoadServers @serverid=14, @debug=1
select * from servers 
exec spLoad @Serverid= 14

*/

/*In case we need to rebuild the CMS database, like when reinstalling from github, we can backup the main reference tables so we can import them back

--create reference database to copy metadata
use master
go
create database Reference
go
use reference
go
select * into dbo.purpose from cms.dbo.purpose
select * into dbo.environment from cms.dbo.environment
select * into dbo.servers from cms.dbo.servers
go
select * from reference.dbo.Purpose
select * from reference.dbo.Environment
select * from reference.dbo.Servers

go


--Populate CMS again
use CMS
go
set identity_insert purpose on
insert into purpose (purposeid, purposename) select purposeid, purposename from reference.dbo.purpose
set identity_insert purpose off


set identity_insert environment on
insert into environment (EnvironmentId, EnvironmentName) select EnvironmentId, EnvironmentName from reference.dbo.environment
set identity_insert environment off

set identity_insert [Servers] on
INSERT INTO [dbo].[Servers]
           (ServerId
			, [EnvironmentId]
           ,[PurposeId]
           ,[ServerName]
           ,[IsActive]
           ,[RemoteUser])
     select ServerId
			, [EnvironmentId]
           ,[PurposeId]
           ,[ServerName]
           ,[IsActive]
           ,[RemoteUser]
	from reference..servers
set identity_insert [Servers] off

select * from cms.dbo.Purpose
select * from cms.dbo.Environment
select * from cms.dbo.Servers
	
*/

GO

