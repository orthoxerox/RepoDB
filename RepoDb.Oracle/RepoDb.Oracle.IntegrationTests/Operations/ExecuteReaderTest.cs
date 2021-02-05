using Microsoft.VisualStudio.TestTools.UnitTesting;
using Oracle.ManagedDataAccess.Client;
using RepoDb.Extensions;
using RepoDb.Reflection;
using RepoDb.Oracle.IntegrationTests.Models;
using RepoDb.Oracle.IntegrationTests.Setup;
using System.Data.Common;
using System.Linq;

namespace RepoDb.Oracle.IntegrationTests.Operations
{
    [TestClass]
    public class ExecuteReaderTest
    {
        [TestInitialize]
        public void Initialize()
        {
            Database.Initialize();
            Cleanup();
        }

        [TestCleanup]
        public void Cleanup()
        {
            Database.Cleanup();
        }

        #region Sync

        [TestMethod]
        public void TestOracleConnectionExecuteReader()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);

            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Act
                using (var reader = connection.ExecuteReader("SELECT \"Id\", \"ColumnNumber\", \"ColumnDate\" FROM \"CompleteTable\";"))
                {
                    while (reader.Read())
                    {
                        // Act
                        var id = reader.GetInt64(0);
                        var columnInt = reader.GetInt32(1);
                        var columnDateTime = reader.GetDateTime(2);
                        var table = tables.FirstOrDefault(e => e.Id == id);

                        // Assert
                        Assert.IsNotNull(table);
                        Assert.AreEqual(columnInt, table.ColumnNumber);
                        Assert.AreEqual(columnDateTime, table.ColumnDate);
                    }
                }
            }
        }

        [TestMethod]
        public void TestOracleConnectionExecuteReaderWithMultipleStatements()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);

            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Act
                using (var reader = connection.ExecuteReader("SELECT \"Id\", \"ColumnNumber\", \"ColumnDate\" FROM \"CompleteTable\"; SELECT \"Id\", \"ColumnNumber\", \"ColumnDate\" FROM \"CompleteTable\";"))
                {
                    do
                    {
                        while (reader.Read())
                        {
                            // Act
                            var id = reader.GetInt64(0);
                            var columnInt = reader.GetInt32(1);
                            var columnDateTime = reader.GetDateTime(2);
                            var table = tables.FirstOrDefault(e => e.Id == id);

                            // Assert
                            Assert.IsNotNull(table);
                            Assert.AreEqual(columnInt, table.ColumnNumber);
                            Assert.AreEqual(columnDateTime, table.ColumnDate);
                        }
                    } while (reader.NextResult());
                }
            }
        }

        [TestMethod]
        public void TestOracleConnectionExecuteReaderAsExtractedEntity()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);

            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Act
                using (var reader = connection.ExecuteReader("SELECT * FROM \"CompleteTable\";"))
                {
                    // Act
                    var result = DataReader.ToEnumerable<CompleteTable>((DbDataReader)reader).AsList();

                    // Assert
                    tables.AsList().ForEach(table => Helper.AssertPropertiesEquality(table, result.First(e => e.Id == table.Id)));
                }
            }
        }

        [TestMethod]
        public void TestOracleConnectionExecuteReaderAsExtractedDynamic()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);

            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Act
                using (var reader = connection.ExecuteReader("SELECT * FROM \"CompleteTable\";"))
                {
                    // Act
                    var result = DataReader.ToEnumerable((DbDataReader)reader).AsList();

                    // Assert
                    tables.AsList().ForEach(table => Helper.AssertMembersEquality(table, result.First(e => e.Id == table.Id)));
                }
            }
        }

        #endregion

        #region Async

        [TestMethod]
        public void TestOracleConnectionExecuteReaderAsync()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);

            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Act
                using (var reader = connection.ExecuteReaderAsync("SELECT \"Id\", \"ColumnNumber\", \"ColumnDate\" FROM \"CompleteTable\";").Result)
                {
                    while (reader.Read())
                    {
                        // Act
                        var id = reader.GetInt64(0);
                        var columnInt = reader.GetInt32(1);
                        var columnDateTime = reader.GetDateTime(2);
                        var table = tables.FirstOrDefault(e => e.Id == id);

                        // Assert
                        Assert.IsNotNull(table);
                        Assert.AreEqual(columnInt, table.ColumnNumber);
                        Assert.AreEqual(columnDateTime, table.ColumnDate);
                    }
                }
            }
        }

        [TestMethod]
        public void TestOracleConnectionExecuteReaderAsyncWithMultipleStatements()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);

            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Act
                using (var reader = connection.ExecuteReaderAsync("SELECT \"Id\", \"ColumnNumber\", \"ColumnDate\" FROM \"CompleteTable\"; SELECT \"Id\", \"ColumnNumber\", \"ColumnDate\" FROM \"CompleteTable\";").Result)
                {
                    do
                    {
                        while (reader.Read())
                        {
                            // Act
                            var id = reader.GetInt64(0);
                            var columnInt = reader.GetInt32(1);
                            var columnDateTime = reader.GetDateTime(2);
                            var table = tables.FirstOrDefault(e => e.Id == id);

                            // Assert
                            Assert.IsNotNull(table);
                            Assert.AreEqual(columnInt, table.ColumnNumber);
                            Assert.AreEqual(columnDateTime, table.ColumnDate);
                        }
                    } while (reader.NextResult());
                }
            }
        }

        [TestMethod]
        public void TestOracleConnectionExecuteReaderAsyncAsExtractedEntity()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);

            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Act
                using (var reader = connection.ExecuteReaderAsync("SELECT * FROM \"CompleteTable\";").Result)
                {
                    // Act
                    var result = DataReader.ToEnumerable<CompleteTable>((DbDataReader)reader).AsList();

                    // Assert
                    tables.AsList().ForEach(table => Helper.AssertPropertiesEquality(table, result.First(e => e.Id == table.Id)));
                }
            }
        }

        [TestMethod]
        public void TestOracleConnectionExecuteReaderAsyncAsExtractedDynamic()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);

            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Act
                using (var reader = connection.ExecuteReaderAsync("SELECT * FROM \"CompleteTable\";").Result)
                {
                    // Act
                    var result = DataReader.ToEnumerable((DbDataReader)reader).AsList();

                    // Assert
                    tables.AsList().ForEach(table => Helper.AssertMembersEquality(table, result.First(e => e.Id == table.Id)));
                }
            }
        }

        #endregion
    }
}
