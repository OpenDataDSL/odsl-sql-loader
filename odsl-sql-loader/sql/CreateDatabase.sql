USE [master]
GO

CREATE DATABASE [ODSL]
GO

USE [ODSL]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[FX](
[base] varchar(3) NOT NULL,
[currency] varchar(3) NOT NULL,
[index] datetime NOT NULL,
[value] decimal(15,6) NOT NULL,
CONSTRAINT [CurrencyPairDate] UNIQUE NONCLUSTERED
(
  [base], [currency], [index]
))
GO

CREATE TYPE FXRates AS TABLE (
[base] varchar(3),
[currency] varchar(3),
[index] datetime,
[value] decimal(15,6)
)
GO

CREATE PROCEDURE dbo.InsertOrUpdateFXRates
       @Rates FXRates READONLY
AS BEGIN
	DECLARE
		@Base varchar(3),
		@Currency varchar(3),
		@Index datetime,
		@Value decimal(15,6)
		
	DECLARE
		FXValues CURSOR FOR SELECT [base],[currency],[index],[value] from @Rates

	OPEN FXValues
	WHILE 1 = 1
	BEGIN
		FETCH NEXT FROM FXValues INTO @Base, @Currency, @Index, @Value
		IF @@FETCH_STATUS = -1 BREAK;

		IF NOT EXISTS (SELECT * FROM dbo.FX WHERE [base] = @Base AND [currency] = @Currency AND [index] = @Index)
		   INSERT INTO dbo.FX([base], [currency], [index], [value])
		   VALUES (@Base, @Currency, @Index, @Value)
		ELSE
		   UPDATE dbo.FX
		   SET [value] = @Value
		   WHERE [base] = @Base AND [currency] = @Currency AND [index] = @Index
	END
	CLOSE FXValues
	DEALLOCATE FXValues
END
GO
