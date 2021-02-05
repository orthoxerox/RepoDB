using System;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Oracle.ManagedDataAccess.Client;
using RepoDb.Oracle.IntegrationTests.Models;
using RepoDb.Oracle.IntegrationTests.Setup;

namespace RepoDb.Oracle.IntegrationTests
{
    [TestClass]
    public class DbHelperTests
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

        #region GetFields

        #region Sync

        [TestMethod]
        public void TestDbHelperGetFields()
        {
            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Setup
                var helper = connection.GetDbHelper();

                // Act
                var fields = helper.GetFields(connection, "CompleteTable", null);

                // Assert
                using (var reader = connection.ExecuteReader(@"SELECT COLUMN_NAME AS ColumnName
                    FROM USER_TAB_COLS
                    WHERE TABLE_NAME = :TableName
                      AND HIDDEN_COLUMN != 'YES'

                    ORDER BY COLUMN_ID", new { TableName = "CompleteTable" }))
                {
                    var fieldCount = 0;

                    while (reader.Read())
                    {
                        var name = reader.GetString(0);
                        var field = fields.FirstOrDefault(f => string.Equals(f.Name, name, StringComparison.OrdinalIgnoreCase));

                        // Assert
                        Assert.IsNotNull(field);

                        fieldCount++;
                    }

                    // Assert
                    Assert.AreEqual(fieldCount, fields.Count());
                }
            }
        }

        [TestMethod]
        public void TestDbHelperGetFieldsPrimary()
        {
            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Setup
                var helper = connection.GetDbHelper();

                // Act
                var fields = helper.GetFields(connection, "CompleteTable", null);
                var primary = fields.FirstOrDefault(f => f.IsPrimary == true);

                // Assert
                Assert.IsNotNull(primary);
                Assert.AreEqual("Id", primary.Name);
            }
        }

        [TestMethod]
        public void TestDbHelperGetFieldsIdentity()
        {
            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Setup
                var helper = connection.GetDbHelper();

                // Act
                var fields = helper.GetFields(connection, "CompleteTable", null);
                var primary = fields.FirstOrDefault(f => f.IsIdentity == true);

                // Assert
                Assert.IsNotNull(primary);
                Assert.AreEqual("Id", primary.Name);
            }
        }

        #endregion

        #region Async

        [TestMethod]
        public void TestDbHelperGetFieldsAsync()
        {
            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Setup
                var helper = connection.GetDbHelper();

                // Act
                var fields = helper.GetFieldsAsync(connection, "CompleteTable", null).Result;

                // Assert
                using (var reader = connection.ExecuteReader(@"SELECT COLUMN_NAME AS ColumnName
                    FROM USER_TAB_COLS
                    WHERE TABLE_NAME = :TableName
                      AND HIDDEN_COLUMN != 'YES'
                    ORDER BY COLUMN_ID", new { TableName = "CompleteTable" }))
                {
                    var fieldCount = 0;

                    while (reader.Read())
                    {
                        var name = reader.GetString(0);
                        var field = fields.FirstOrDefault(f => string.Equals(f.Name, name, StringComparison.OrdinalIgnoreCase));

                        // Assert
                        Assert.IsNotNull(field);

                        fieldCount++;
                    }

                    // Assert
                    Assert.AreEqual(fieldCount, fields.Count());
                }
            }
        }

        [TestMethod]
        public void TestDbHelperGetFieldsAsyncPrimary()
        {
            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Setup
                var helper = connection.GetDbHelper();

                // Act
                var fields = helper.GetFieldsAsync(connection, "CompleteTable", null).Result;
                var primary = fields.FirstOrDefault(f => f.IsPrimary == true);

                // Assert
                Assert.IsNotNull(primary);
                Assert.AreEqual("Id", primary.Name);
            }
        }

        [TestMethod]
        public void TestDbHelperGetFieldsAsyncIdentity()
        {
            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Setup
                var helper = connection.GetDbHelper();

                // Act
                var fields = helper.GetFieldsAsync(connection, "CompleteTable", null).Result;
                var primary = fields.FirstOrDefault(f => f.IsIdentity == true);

                // Assert
                Assert.IsNotNull(primary);
                Assert.AreEqual("Id", primary.Name);
            }
        }

        #endregion

        #endregion

        #region GetScopeIdentity

        #region Sync

        [TestMethod]
        public void TestDbHelperGetScopeIdentity()
        {
            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Setup
                var helper = connection.GetDbHelper();
                var table = Helper.CreateCompleteTables(1).First();

                // Act
                var insertResult = connection.Insert<CompleteTable>(table);

                // Assert
                Assert.IsTrue(Convert.ToInt64(insertResult) > 0);
                Assert.IsTrue(table.Id > 0);

                // Act
                var result = helper.GetScopeIdentity(connection, null);

                // Assert
                Assert.AreEqual(insertResult, result);
            }
        }

        #endregion

        #region Async

        [TestMethod]
        public void TestDbHelperGetScopeIdentityAsync()
        {
            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Setup
                var helper = connection.GetDbHelper();
                var table = Helper.CreateCompleteTables(1).First();

                // Act
                var insertResult = connection.Insert<CompleteTable>(table);

                // Assert
                Assert.IsTrue(Convert.ToInt64(insertResult) > 0);
                Assert.IsTrue(table.Id > 0);

                // Act
                var result = helper.GetScopeIdentityAsync(connection, null).Result;

                // Assert
                Assert.AreEqual(insertResult, result);
            }
        }

        #endregion

        #endregion
    }
}
