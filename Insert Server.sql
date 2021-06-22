insert into purpose (purposename)values  ('demo')
insert into Environment (Environmentname)values  ('EastUS2')

select * from Purpose
select * from Environment

INSERT INTO [dbo].[Servers]
           ([EnvironmentId]
           ,[PurposeId]
           ,[ServerName]
           ,[IsActive]
           ,[RemoteUser])
     VALUES
           ( 1--[EnvironmentId]
           , 1-- [PurposeId]
           , '' --[ServerName]
           ,1 -- [IsActive]
           ,'?'-- [RemoteUser]
		   )
GO

