using RepoDb.Oracle.IntegrationTests.Models;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;

namespace RepoDb.Oracle.IntegrationTests.Setup
{
    public static class Database
    {
        #region Properties

        /// <summary>
        /// Gets or sets the connection string to be used for Postgres database.
        /// </summary>
        public static string ConnectionStringForSys { get; private set; }

        /// <summary>
        /// Gets or sets the connection string to be used.
        /// </summary>
        public static string ConnectionString { get; private set; }

        #endregion

        #region Methods

        public static void Initialize()
        {
            // Check the connection string
            var connectionStringForSys = Environment.GetEnvironmentVariable("REPODB_CONSTR_ORACLESYS", EnvironmentVariableTarget.Process);
            var connectionString = Environment.GetEnvironmentVariable("REPODB_CONSTR", EnvironmentVariableTarget.Process);

            // Master connection
            ConnectionStringForSys = (connectionStringForSys ?? "Data Source=127.0.0.1:1521/XEPDB1;User Id=sys;Password=orapass;DBA Privilege=SYSDBA;");

            // RepoDb connection
            ConnectionString = (connectionString ?? "Data Source=127.0.0.1:1521/XEPDB1;User Id=repodb;Password=repopass;");

            // Initialize Oracle
            OracleBootstrap.Initialize();

            // Create databases
            CreateDatabase();

            // Create tables
            CreateTables();
        }

        public static void Cleanup()
        {
            using (var connection = new OracleConnection(ConnectionString))
            {
                connection.Truncate<CompleteTable>();
                connection.Truncate<NonIdentityCompleteTable>();
            }
        }

        #endregion

        #region CreateDatabases

        private static void CreateDatabase()
        {
            using (var connection = new OracleConnection(ConnectionStringForSys))
            {
                var recordCount = connection.ExecuteScalar<int>("SELECT COUNT(*) FROM all_users WHERE username = 'REPODB'");
                if (recordCount <= 0)
                {
                    connection.ExecuteNonQuery(@"CREATE USER REPODB
                                                 IDENTIFIED BY ""repopass""
                                                 DEFAULT TABLESPACE USERS
                                                 TEMPORARY TABLESPACE TEMP
                                                 PROFILE DEFAULT
                                                 ACCOUNT UNLOCK");
                    connection.ExecuteNonQuery(@"GRANT CREATE SESSION TO REPODB");
                    connection.ExecuteNonQuery(@"GRANT CREATE TABLE TO REPODB");
                    connection.ExecuteNonQuery(@"GRANT CREATE SEQUENCE TO REPODB");
                    connection.ExecuteNonQuery(@"ALTER USER REPODB QUOTA UNLIMITED ON USERS");
                }
            }
        }

        #endregion

        #region CreateTables

        private static void CreateTables()
        {
            CreateCompleteTable();
            CreateNonIdentityCompleteTable();
        }

        private static void CreateCompleteTable()
        {
            using (var connection = new OracleConnection(ConnectionString))
            {
                var recordCount = connection.ExecuteScalar<int>("SELECT COUNT(*) FROM user_tables WHERE table_name = 'CompleteTable'");
                if (recordCount <= 0)
                {
                    connection.ExecuteNonQuery(@"CREATE TABLE ""CompleteTable"" (
                        ""Id"" NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        ""ColumnBFile"" BFILE,
                        ""ColumnBinaryDouble"" BINARY_DOUBLE,
                        ""ColumnBinaryFloat"" BINARY_FLOAT,
                        ""ColumnBlob"" BLOB,
                        ""ColumnChar"" CHAR(1000),
                        ""ColumnClob"" CLOB,
                        ""ColumnDate"" DATE,
                        ""ColumnIntervalDayToSecond"" INTERVAL DAY TO SECOND,
                        ""ColumnIntervalYearToMonth"" INTERVAL YEAR TO MONTH,
                        --""ColumnJson"" JSON,
                        ""ColumnLong"" LONG,
                        --""ColumnLongRaw"" LONG RAW,
                        ""ColumnNChar"" NCHAR(1000),
                        ""ColumnNClob"" NCLOB,
                        ""ColumnNumber"" NUMBER,
                        ""ColumnNVarchar2"" NVARCHAR2(1000),
                        ""ColumnRaw"" RAW(1000),
                        ""ColumnRowID"" ROWID,
                        ""ColumnTimestamp"" TIMESTAMP,
                        ""ColumnTimestampWithLocalTZ"" TIMESTAMP WITH LOCAL TIME ZONE,
                        ""ColumnTimestampWithTZ"" TIMESTAMP WITH TIME ZONE,
                        ""ColumnURowID"" UROWID,
                        ""ColumnVarchar2"" VARCHAR2(4000),
                        ""ColumnXmltype"" XMLType
                    )");
                }
            }
        }

        private static void CreateNonIdentityCompleteTable()
        {
            using (var connection = new OracleConnection(ConnectionString))
            {
                var recordCount = connection.ExecuteScalar<int>("SELECT COUNT(*) FROM user_tables WHERE table_name = 'NonIdentityCompleteTable'");
                if (recordCount <= 0)
                {
                    connection.ExecuteNonQuery(@"CREATE TABLE ""NonIdentityCompleteTable"" (
                        ""Id"" NUMBER NOT NULL PRIMARY KEY,
                        ""ColumnBFile"" BFILE,
                        ""ColumnBinaryDouble"" BINARY_DOUBLE,
                        ""ColumnBinaryFloat"" BINARY_FLOAT,
                        ""ColumnBlob"" BLOB,
                        ""ColumnChar"" CHAR(1000),
                        ""ColumnClob"" CLOB,
                        ""ColumnDate"" DATE,
                        ""ColumnIntervalDayToSecond"" INTERVAL DAY TO SECOND,
                        ""ColumnIntervalYearToMonth"" INTERVAL YEAR TO MONTH,
                        --""ColumnJson"" JSON,
                        --""ColumnLong"" LONG,
                        ""ColumnLongRaw"" LONG RAW,
                        ""ColumnNChar"" NCHAR(1000),
                        ""ColumnNClob"" NCLOB,
                        ""ColumnNumber"" NUMBER,
                        ""ColumnNVarchar2"" NVARCHAR2(1000),
                        ""ColumnRaw"" RAW(1000),
                        ""ColumnRowID"" ROWID,
                        ""ColumnTimestamp"" TIMESTAMP,
                        ""ColumnTimestampWithLocalTZ"" TIMESTAMP WITH LOCAL TIME ZONE,
                        ""ColumnTimestampWithTZ"" TIMESTAMP WITH TIME ZONE,
                        ""ColumnURowID"" UROWID,
                        ""ColumnVarchar2"" VARCHAR2(4000),
                        ""ColumnXmltype"" XMLType
                    )");
                }
            }
        }

        #endregion

        #region CompleteTable

        public static IEnumerable<CompleteTable> CreateCompleteTables(int count)
        {
            using (var connection = new OracleConnection(ConnectionString))
            {
                var tables = Helper.CreateCompleteTables(count);
                connection.InsertAll(tables);
                return tables;
            }
        }

        #endregion

        #region NonIdentityCompleteTable

        public static IEnumerable<NonIdentityCompleteTable> CreateNonIdentityCompleteTables(int count)
        {
            using (var connection = new OracleConnection(ConnectionString))
            {
                var tables = Helper.CreateNonIdentityCompleteTables(count);
                connection.InsertAll(tables);
                return tables;
            }
        }

        #endregion
    }
}
