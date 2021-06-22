--Create liked server from CMS VM to managed server
USE [master]
GO

--Sample link to the server 
EXEC master.dbo.sp_addlinkedserver @server = N'cmsdemoazuresql.database.windows.net', @srvproduct=N'', @provider=N'SQLNCLI', @datasrc=N'cmsdemoazuresql.database.windows.net', @catalog=N'master'
 /* For security reasons the linked server remote logins password is changed with ######## */
EXEC master.dbo.sp_addlinkedsrvlogin @rmtsrvname=N'cmsdemoazuresql.database.windows.net',@useself=N'False',@locallogin=NULL,@rmtuser='Admin',@rmtpassword='?'
GO

EXEC master.dbo.sp_serveroption @server=N'cmsdemoazuresql.database.windows.net', @optname=N'collation compatible', @optvalue=N'false'
GO

EXEC master.dbo.sp_serveroption @server=N'cmsdemoazuresql.database.windows.net', @optname=N'data access', @optvalue=N'true'
GO

EXEC master.dbo.sp_serveroption @server=N'cmsdemoazuresql.database.windows.net', @optname=N'dist', @optvalue=N'false'
GO

EXEC master.dbo.sp_serveroption @server=N'cmsdemoazuresql.database.windows.net', @optname=N'pub', @optvalue=N'false'
GO

EXEC master.dbo.sp_serveroption @server=N'cmsdemoazuresql.database.windows.net', @optname=N'rpc', @optvalue=N'true'
GO

EXEC master.dbo.sp_serveroption @server=N'cmsdemoazuresql.database.windows.net', @optname=N'rpc out', @optvalue=N'true'
GO

EXEC master.dbo.sp_serveroption @server=N'cmsdemoazuresql.database.windows.net', @optname=N'sub', @optvalue=N'false'
GO

EXEC master.dbo.sp_serveroption @server=N'cmsdemoazuresql.database.windows.net', @optname=N'connect timeout', @optvalue=N'0'
GO

EXEC master.dbo.sp_serveroption @server=N'cmsdemoazuresql.database.windows.net', @optname=N'collation name', @optvalue=null
GO

EXEC master.dbo.sp_serveroption @server=N'cmsdemoazuresql.database.windows.net', @optname=N'lazy schema validation', @optvalue=N'false'
GO

EXEC master.dbo.sp_serveroption @server=N'cmsdemoazuresql.database.windows.net', @optname=N'query timeout', @optvalue=N'0'
GO

EXEC master.dbo.sp_serveroption @server=N'cmsdemoazuresql.database.windows.net', @optname=N'use remote collation', @optvalue=N'true'
GO

EXEC master.dbo.sp_serveroption @server=N'cmsdemoazuresql.database.windows.net', @optname=N'remote proc transaction promotion', @optvalue=N'true'
GO



--If target is azure sql we also need a link to each db, in this example the database is AdventureWorks2019
EXEC master.dbo.sp_addlinkedserver @server = N'cmsdemoazuresql.database.windows.net.AdventureWorks2019', @srvproduct=N'', @provider=N'SQLNCLI', @datasrc=N'cmsdemoazuresql.database.windows.net', @catalog=N'AdventureWorks2019'
 /* For security reasons the linked server remote logins password is changed with ######## */
EXEC master.dbo.sp_addlinkedsrvlogin @rmtsrvname=N'cmsdemoazuresql.database.windows.net.AdventureWorks2019',@useself=N'False',@locallogin=NULL,@rmtuser='Superuser',@rmtpassword='?'
GO

EXEC master.dbo.sp_serveroption @server=N'cmsdemoazuresql.database.windows.net.AdventureWorks2019', @optname=N'collation compatible', @optvalue=N'false'
GO

EXEC master.dbo.sp_serveroption @server=N'cmsdemoazuresql.database.windows.net.AdventureWorks2019', @optname=N'data access', @optvalue=N'true'
GO

EXEC master.dbo.sp_serveroption @server=N'cmsdemoazuresql.database.windows.net.AdventureWorks2019', @optname=N'dist', @optvalue=N'false'
GO

EXEC master.dbo.sp_serveroption @server=N'cmsdemoazuresql.database.windows.net.AdventureWorks2019', @optname=N'pub', @optvalue=N'false'
GO

EXEC master.dbo.sp_serveroption @server=N'cmsdemoazuresql.database.windows.net.AdventureWorks2019', @optname=N'rpc', @optvalue=N'true'
GO

EXEC master.dbo.sp_serveroption @server=N'cmsdemoazuresql.database.windows.net.AdventureWorks2019', @optname=N'rpc out', @optvalue=N'true'
GO

EXEC master.dbo.sp_serveroption @server=N'cmsdemoazuresql.database.windows.net.AdventureWorks2019', @optname=N'sub', @optvalue=N'false'
GO

EXEC master.dbo.sp_serveroption @server=N'cmsdemoazuresql.database.windows.net.AdventureWorks2019', @optname=N'connect timeout', @optvalue=N'0'
GO

EXEC master.dbo.sp_serveroption @server=N'cmsdemoazuresql.database.windows.net.AdventureWorks2019', @optname=N'collation name', @optvalue=null
GO

EXEC master.dbo.sp_serveroption @server=N'cmsdemoazuresql.database.windows.net.AdventureWorks2019', @optname=N'lazy schema validation', @optvalue=N'false'
GO

EXEC master.dbo.sp_serveroption @server=N'cmsdemoazuresql.database.windows.net.AdventureWorks2019', @optname=N'query timeout', @optvalue=N'0'
GO

EXEC master.dbo.sp_serveroption @server=N'cmsdemoazuresql.database.windows.net.AdventureWorks2019', @optname=N'use remote collation', @optvalue=N'true'
GO

EXEC master.dbo.sp_serveroption @server=N'cmsdemoazuresql.database.windows.net.AdventureWorks2019', @optname=N'remote proc transaction promotion', @optvalue=N'true'
GO


