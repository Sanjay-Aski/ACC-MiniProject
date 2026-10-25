/* Azure Deduplication Virtual Lab schema (Azure SQL Database) */
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

IF OBJECT_ID('dbo.TestRuns','U') IS NULL
BEGIN
  CREATE TABLE dbo.TestRuns (
    TestRunId UNIQUEIDENTIFIER NOT NULL CONSTRAINT PK_TestRuns PRIMARY KEY,
    ExpectedCount INT NULL,
    CreatedAt DATETIME2(3) NOT NULL CONSTRAINT DF_TestRuns_CreatedAt DEFAULT SYSUTCDATETIME()
  );
END;
GO

IF OBJECT_ID('dbo.TestMessages','U') IS NULL
BEGIN
  CREATE TABLE dbo.TestMessages (
    RecordId UNIQUEIDENTIFIER NOT NULL CONSTRAINT PK_TestMessages PRIMARY KEY,
    TestRunId UNIQUEIDENTIFIER NOT NULL,
    LineNumber INT NOT NULL,
    RequestId UNIQUEIDENTIFIER NOT NULL,
    PayloadJson NVARCHAR(MAX) NOT NULL,
    OutputText NVARCHAR(MAX) NOT NULL,
    Status VARCHAR(20) NOT NULL CONSTRAINT DF_TestMessages_Status DEFAULT 'registered',
    Fingerprint CHAR(64) NULL,
    CreatedAt DATETIME2(3) NOT NULL CONSTRAINT DF_TestMessages_CreatedAt DEFAULT SYSUTCDATETIME(),
    QueuedAt DATETIME2(3) NULL,
    ProcessedAt DATETIME2(3) NULL,
    ErrorMessage NVARCHAR(1000) NULL,
    CONSTRAINT FK_TestMessages_TestRuns FOREIGN KEY(TestRunId) REFERENCES dbo.TestRuns(TestRunId)
  );
END;
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id=OBJECT_ID('dbo.TestMessages') AND name='IX_TestMessages_RunStatus') CREATE INDEX IX_TestMessages_RunStatus ON dbo.TestMessages(TestRunId, Status);
GO

IF OBJECT_ID('dbo.FingerprintState','U') IS NULL
BEGIN
  CREATE TABLE dbo.FingerprintState (
    Fingerprint CHAR(64) NOT NULL CONSTRAINT PK_FingerprintState PRIMARY KEY,
    ExpiresAt DATETIME2(3) NOT NULL,
    RecordId UNIQUEIDENTIFIER NOT NULL
  );
END;
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id=OBJECT_ID('dbo.FingerprintState') AND name='IX_FingerprintState_ExpiresAt') CREATE INDEX IX_FingerprintState_ExpiresAt ON dbo.FingerprintState(ExpiresAt);
GO

IF OBJECT_ID('dbo.AcceptedRecords','U') IS NULL
BEGIN
  CREATE TABLE dbo.AcceptedRecords (
    RecordId UNIQUEIDENTIFIER NOT NULL CONSTRAINT PK_AcceptedRecords PRIMARY KEY,
    TestRunId UNIQUEIDENTIFIER NOT NULL,
    LineNumber INT NOT NULL,
    Fingerprint CHAR(64) NOT NULL,
    PayloadJson NVARCHAR(MAX) NOT NULL,
    OutputText NVARCHAR(MAX) NOT NULL,
    AcceptedAt DATETIME2(3) NOT NULL CONSTRAINT DF_AcceptedRecords_AcceptedAt DEFAULT SYSUTCDATETIME(),
    CONSTRAINT FK_AcceptedRecords_TestRuns FOREIGN KEY(TestRunId) REFERENCES dbo.TestRuns(TestRunId)
  );
END;
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id=OBJECT_ID('dbo.AcceptedRecords') AND name='IX_AcceptedRecords_RunLine') CREATE INDEX IX_AcceptedRecords_RunLine ON dbo.AcceptedRecords(TestRunId, LineNumber, AcceptedAt);
GO

IF OBJECT_ID('dbo.ProcessingEvents','U') IS NULL
BEGIN
  CREATE TABLE dbo.ProcessingEvents (
    EventId BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_ProcessingEvents PRIMARY KEY,
    TestRunId UNIQUEIDENTIFIER NOT NULL,
    RecordId UNIQUEIDENTIFIER NULL,
    Stage VARCHAR(40) NOT NULL,
    Status VARCHAR(40) NOT NULL,
    DetailJson NVARCHAR(MAX) NULL,
    CreatedAt DATETIME2(3) NOT NULL CONSTRAINT DF_ProcessingEvents_CreatedAt DEFAULT SYSUTCDATETIME(),
    CONSTRAINT FK_ProcessingEvents_TestRuns FOREIGN KEY(TestRunId) REFERENCES dbo.TestRuns(TestRunId)
  );
END;
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id=OBJECT_ID('dbo.ProcessingEvents') AND name='IX_ProcessingEvents_RunEvent') CREATE INDEX IX_ProcessingEvents_RunEvent ON dbo.ProcessingEvents(TestRunId, EventId);
GO

IF OBJECT_ID('dbo.DeadLetterCheckpoint','U') IS NULL
BEGIN
  CREATE TABLE dbo.DeadLetterCheckpoint (
    QueueName NVARCHAR(260) NOT NULL CONSTRAINT PK_DeadLetterCheckpoint PRIMARY KEY,
    LastSequenceNumber BIGINT NOT NULL CONSTRAINT DF_DeadLetterCheckpoint_Sequence DEFAULT 0,
    UpdatedAt DATETIME2(3) NOT NULL CONSTRAINT DF_DeadLetterCheckpoint_UpdatedAt DEFAULT SYSUTCDATETIME()
  );
END;
GO

IF OBJECT_ID('dbo.ProcessingFailures','U') IS NULL
BEGIN
  CREATE TABLE dbo.ProcessingFailures (
    FailureId BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_ProcessingFailures PRIMARY KEY,
    TestRunId UNIQUEIDENTIFIER NOT NULL,
    RecordId UNIQUEIDENTIFIER NULL,
    Stage VARCHAR(40) NOT NULL,
    ErrorType VARCHAR(200) NOT NULL,
    ErrorMessage NVARCHAR(1000) NULL,
    CreatedAt DATETIME2(3) NOT NULL CONSTRAINT DF_ProcessingFailures_CreatedAt DEFAULT SYSUTCDATETIME(),
    CONSTRAINT FK_ProcessingFailures_TestRuns FOREIGN KEY(TestRunId) REFERENCES dbo.TestRuns(TestRunId)
  );
END;
GO

CREATE OR ALTER PROCEDURE dbo.ProcessDedupRecord
  @RecordId UNIQUEIDENTIFIER,
  @Fingerprint CHAR(64),
  @WindowSeconds INT,
  @BloomPossibleMatch BIT = NULL
AS
BEGIN
  SET NOCOUNT ON;
  SET XACT_ABORT ON;

  DECLARE @Now DATETIME2(3) = SYSUTCDATETIME();
  DECLARE @ExistingExpiry DATETIME2(3);
  DECLARE @Status VARCHAR(20);
  DECLARE @TestRunId UNIQUEIDENTIFIER;
  DECLARE @PayloadJson NVARCHAR(MAX);
  DECLARE @OutputText NVARCHAR(MAX);
  DECLARE @LineNumber INT;

  BEGIN TRANSACTION;

  -- Idempotency: a retried message returns its original decision without recounting.
  SELECT @Status = Status, @TestRunId = TestRunId
  FROM dbo.TestMessages WITH (UPDLOCK, HOLDLOCK)
  WHERE RecordId = @RecordId;

  IF @Status IS NULL
  BEGIN
    ROLLBACK TRANSACTION;
    THROW 51001, 'RecordId not registered before processing.', 1;
  END;

  IF @Status IN ('accepted','duplicate')
  BEGIN
    COMMIT TRANSACTION;
    SELECT @Status AS Status, CONVERT(VARCHAR(36), @RecordId) AS RecordId;
    RETURN;
  END;

  SET @Status = 'accepted';
  DECLARE @LockResult INT;
  DECLARE @LockResource NVARCHAR(255) = CONCAT(N'dedup:', @Fingerprint);
  EXEC @LockResult = sys.sp_getapplock
      @Resource = @LockResource,
      @LockMode = 'Exclusive',
      @LockOwner = 'Transaction',
      @LockTimeout = 10000;
  IF @LockResult < 0
  BEGIN
    ROLLBACK TRANSACTION;
    THROW 51002, 'Could not acquire fingerprint lock; retry message.', 1;
  END;

  SELECT @ExistingExpiry = ExpiresAt
  FROM dbo.FingerprintState WITH (UPDLOCK, HOLDLOCK)
  WHERE Fingerprint = @Fingerprint;

  SELECT @PayloadJson=PayloadJson, @OutputText=OutputText, @LineNumber=LineNumber
  FROM dbo.TestMessages WHERE RecordId=@RecordId;

  IF @ExistingExpiry IS NOT NULL AND @ExistingExpiry > @Now
  BEGIN
    SET @Status = 'duplicate';
  END
  ELSE
  BEGIN
    IF @ExistingExpiry IS NULL
      INSERT dbo.FingerprintState(Fingerprint, ExpiresAt, RecordId)
      VALUES (@Fingerprint, DATEADD(SECOND, @WindowSeconds, @Now), @RecordId);
    ELSE
      UPDATE dbo.FingerprintState
        SET ExpiresAt=DATEADD(SECOND, @WindowSeconds, @Now), RecordId=@RecordId
        WHERE Fingerprint=@Fingerprint;

    INSERT dbo.AcceptedRecords(RecordId, TestRunId, LineNumber, Fingerprint, PayloadJson, OutputText)
    VALUES (@RecordId, @TestRunId, @LineNumber, @Fingerprint, @PayloadJson, @OutputText);
  END;

  UPDATE dbo.TestMessages
    SET Status=@Status, Fingerprint=@Fingerprint, ProcessedAt=SYSUTCDATETIME(), ErrorMessage=NULL
    WHERE RecordId=@RecordId;

  INSERT dbo.ProcessingEvents(TestRunId, RecordId, Stage, Status, DetailJson)
  VALUES (@TestRunId, @RecordId, 'sql_decision', @Status,
    CONCAT(N'{"decisionAuthority":"Azure SQL transaction","windowSeconds":', @WindowSeconds,
      N',"bloomPossibleMatch":',
      CASE WHEN @BloomPossibleMatch IS NULL THEN N'null' WHEN @BloomPossibleMatch=1 THEN N'true' ELSE N'false' END,
      N'}'));

  COMMIT TRANSACTION;
  SELECT @Status AS Status, CONVERT(VARCHAR(36), @RecordId) AS RecordId;
END;
GO
