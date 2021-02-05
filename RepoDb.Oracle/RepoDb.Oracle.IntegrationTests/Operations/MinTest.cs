using Microsoft.VisualStudio.TestTools.UnitTesting;
using Oracle.ManagedDataAccess.Client;
using RepoDb.Enumerations;
using RepoDb.Oracle.IntegrationTests.Models;
using RepoDb.Oracle.IntegrationTests.Setup;
using System;
using System.Linq;

namespace RepoDb.Oracle.IntegrationTests.Operations
{
    [TestClass]
    public class MinTest
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

        #region DataEntity

        #region Sync

        [TestMethod]
        public void TestOracleConnectionMinWithoutExpression()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);

            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Act
                var result = connection.Min<CompleteTable>(e => e.ColumnNumber,
                    (object)null);

                // Assert
                Assert.AreEqual(tables.Min(e => e.ColumnNumber), Convert.ToInt32(result));
            }
        }

        [TestMethod]
        public void TestOracleConnectionMinViaExpression()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);
            var ids = new[] { tables.First().Id, tables.Last().Id };

            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Act
                var result = connection.Min<CompleteTable>(e => e.ColumnNumber,
                    e => ids.Contains(e.Id));

                // Assert
                Assert.AreEqual(tables.Where(e => ids.Contains(e.Id)).Min(e => e.ColumnNumber), Convert.ToInt32(result));
            }
        }

        [TestMethod]
        public void TestOracleConnectionMinViaDynamic()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);

            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Act
                var result = connection.Min<CompleteTable>(e => e.ColumnNumber,
                    new { tables.First().Id });

                // Assert
                Assert.AreEqual(tables.Where(e => e.Id == tables.First().Id).Min(e => e.ColumnNumber), Convert.ToInt32(result));
            }
        }

        [TestMethod]
        public void TestOracleConnectionMinViaQueryField()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);

            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Act
                var result = connection.Min<CompleteTable>(e => e.ColumnNumber,
                    new QueryField("Id", tables.First().Id));

                // Assert
                Assert.AreEqual(tables.Where(e => e.Id == tables.First().Id).Min(e => e.ColumnNumber), Convert.ToInt32(result));
            }
        }

        [TestMethod]
        public void TestOracleConnectionMinViaQueryFields()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);
            var queryFields = new[]
            {
                new QueryField("Id", Operation.GreaterThan, tables.First().Id),
                new QueryField("Id", Operation.LessThan, tables.Last().Id)
            };

            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Act
                var result = connection.Min<CompleteTable>(e => e.ColumnNumber,
                    queryFields);

                // Assert
                Assert.AreEqual(tables.Where(e => e.Id > tables.First().Id && e.Id < tables.Last().Id).Min(e => e.ColumnNumber), Convert.ToInt32(result));
            }
        }

        [TestMethod]
        public void TestOracleConnectionMinViaQueryGroup()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);
            var queryFields = new[]
            {
                new QueryField("Id", Operation.GreaterThan, tables.First().Id),
                new QueryField("Id", Operation.LessThan, tables.Last().Id)
            };
            var queryGroup = new QueryGroup(queryFields);

            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Act
                var result = connection.Min<CompleteTable>(e => e.ColumnNumber,
                    queryGroup);

                // Assert
                Assert.AreEqual(tables.Where(e => e.Id > tables.First().Id && e.Id < tables.Last().Id).Min(e => e.ColumnNumber), Convert.ToInt32(result));
            }
        }

        [TestMethod, ExpectedException(typeof(NotSupportedException))]
        public void ThrowExceptionOnOracleConnectionMinWithHints()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);

            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Act
                connection.Min<CompleteTable>(e => e.ColumnNumber,
                    (object)null,
                    hints: "WhatEver");
            }
        }

        #endregion

        #region Async

        [TestMethod]
        public void TestOracleConnectionMinAsyncWithoutExpression()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);

            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Act
                var result = connection.MinAsync<CompleteTable>(e => e.ColumnNumber,
                    (object)null).Result;

                // Assert
                Assert.AreEqual(tables.Min(e => e.ColumnNumber), Convert.ToInt32(result));
            }
        }

        [TestMethod]
        public void TestOracleConnectionMinAsyncViaExpression()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);
            var ids = new[] { tables.First().Id, tables.Last().Id };

            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Act
                var result = connection.MinAsync<CompleteTable>(e => e.ColumnNumber,
                    e => ids.Contains(e.Id)).Result;

                // Assert
                Assert.AreEqual(tables.Where(e => ids.Contains(e.Id)).Min(e => e.ColumnNumber), Convert.ToInt32(result));
            }
        }

        [TestMethod]
        public void TestOracleConnectionMinAsyncViaDynamic()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);

            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Act
                var result = connection.MinAsync<CompleteTable>(e => e.ColumnNumber,
                    new { tables.First().Id }).Result;

                // Assert
                Assert.AreEqual(tables.Where(e => e.Id == tables.First().Id).Min(e => e.ColumnNumber), Convert.ToInt32(result));
            }
        }

        [TestMethod]
        public void TestOracleConnectionMinAsyncViaQueryField()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);

            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Act
                var result = connection.MinAsync<CompleteTable>(e => e.ColumnNumber,
                    new QueryField("Id", tables.First().Id)).Result;

                // Assert
                Assert.AreEqual(tables.Where(e => e.Id == tables.First().Id).Min(e => e.ColumnNumber), Convert.ToInt32(result));
            }
        }

        [TestMethod]
        public void TestOracleConnectionMinAsyncViaQueryFields()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);
            var queryFields = new[]
            {
                new QueryField("Id", Operation.GreaterThan, tables.First().Id),
                new QueryField("Id", Operation.LessThan, tables.Last().Id)
            };

            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Act
                var result = connection.MinAsync<CompleteTable>(e => e.ColumnNumber,
                    queryFields).Result;

                // Assert
                Assert.AreEqual(tables.Where(e => e.Id > tables.First().Id && e.Id < tables.Last().Id).Min(e => e.ColumnNumber), Convert.ToInt32(result));
            }
        }

        [TestMethod]
        public void TestOracleConnectionMinAsyncViaQueryGroup()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);
            var queryFields = new[]
            {
                new QueryField("Id", Operation.GreaterThan, tables.First().Id),
                new QueryField("Id", Operation.LessThan, tables.Last().Id)
            };
            var queryGroup = new QueryGroup(queryFields);

            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Act
                var result = connection.MinAsync<CompleteTable>(e => e.ColumnNumber,
                    queryGroup).Result;

                // Assert
                Assert.AreEqual(tables.Where(e => e.Id > tables.First().Id && e.Id < tables.Last().Id).Min(e => e.ColumnNumber), Convert.ToInt32(result));
            }
        }

        [TestMethod, ExpectedException(typeof(AggregateException))]
        public void ThrowExceptionOnOracleConnectionMinAsyncWithHints()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);

            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Act
                connection.MinAsync<CompleteTable>(e => e.ColumnNumber,
                    (object)null,
                    hints: "WhatEver").Wait();
            }
        }

        #endregion

        #endregion

        #region TableName

        #region Sync

        [TestMethod]
        public void TestOracleConnectionMinViaTableNameWithoutExpression()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);

            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Act
                var result = connection.Min(ClassMappedNameCache.Get<CompleteTable>(),
                    new Field("ColumnNumber", typeof(int)),
                    (object)null);

                // Assert
                Assert.AreEqual(tables.Min(e => e.ColumnNumber), Convert.ToInt32(result));
            }
        }

        [TestMethod]
        public void TestOracleConnectionMinViaTableNameViaDynamic()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);

            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Act
                var result = connection.Min(ClassMappedNameCache.Get<CompleteTable>(),
                    new Field("ColumnNumber", typeof(int)),
                    new { tables.First().Id });

                // Assert
                Assert.AreEqual(tables.Where(e => e.Id == tables.First().Id).Min(e => e.ColumnNumber), Convert.ToInt32(result));
            }
        }

        [TestMethod]
        public void TestOracleConnectionMinViaTableNameViaQueryField()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);

            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Act
                var result = connection.Min(ClassMappedNameCache.Get<CompleteTable>(),
                    new Field("ColumnNumber", typeof(int)),
                    new QueryField("Id", tables.First().Id));

                // Assert
                Assert.AreEqual(tables.Where(e => e.Id == tables.First().Id).Min(e => e.ColumnNumber), Convert.ToInt32(result));
            }
        }

        [TestMethod]
        public void TestOracleConnectionMinViaTableNameViaQueryFields()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);
            var queryFields = new[]
            {
                new QueryField("Id", Operation.GreaterThan, tables.First().Id),
                new QueryField("Id", Operation.LessThan, tables.Last().Id)
            };

            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Act
                var result = connection.Min(ClassMappedNameCache.Get<CompleteTable>(),
                    new Field("ColumnNumber", typeof(int)),
                    queryFields);

                // Assert
                Assert.AreEqual(tables.Where(e => e.Id > tables.First().Id && e.Id < tables.Last().Id).Min(e => e.ColumnNumber), Convert.ToInt32(result));
            }
        }

        [TestMethod]
        public void TestOracleConnectionMinViaTableNameViaQueryGroup()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);
            var queryFields = new[]
            {
                new QueryField("Id", Operation.GreaterThan, tables.First().Id),
                new QueryField("Id", Operation.LessThan, tables.Last().Id)
            };
            var queryGroup = new QueryGroup(queryFields);

            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Act
                var result = connection.Min(ClassMappedNameCache.Get<CompleteTable>(),
                    new Field("ColumnNumber", typeof(int)),
                    queryGroup);

                // Assert
                Assert.AreEqual(tables.Where(e => e.Id > tables.First().Id && e.Id < tables.Last().Id).Min(e => e.ColumnNumber), Convert.ToInt32(result));
            }
        }

        [TestMethod, ExpectedException(typeof(NotSupportedException))]
        public void ThrowExceptionOnOracleConnectionMinViaTableNameWithHints()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);

            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Act
                connection.Min(ClassMappedNameCache.Get<CompleteTable>(),
                    new Field("ColumnNumber", typeof(int)),
                    (object)null,
                    hints: "WhatEver");
            }
        }

        #endregion

        #region Async

        [TestMethod]
        public void TestOracleConnectionMinAsyncViaTableNameWithoutExpression()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);

            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Act
                var result = connection.MinAsync(ClassMappedNameCache.Get<CompleteTable>(),
                    new Field("ColumnNumber", typeof(int)),
                    (object)null).Result;

                // Assert
                Assert.AreEqual(tables.Min(e => e.ColumnNumber), Convert.ToInt32(result));
            }
        }

        [TestMethod]
        public void TestOracleConnectionMinAsyncViaTableNameViaDynamic()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);

            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Act
                var result = connection.MinAsync(ClassMappedNameCache.Get<CompleteTable>(),
                    new Field("ColumnNumber", typeof(int)),
                    new { tables.First().Id }).Result;

                // Assert
                Assert.AreEqual(tables.Where(e => e.Id == tables.First().Id).Min(e => e.ColumnNumber), Convert.ToInt32(result));
            }
        }

        [TestMethod]
        public void TestOracleConnectionMinAsyncViaTableNameViaQueryField()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);

            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Act
                var result = connection.MinAsync(ClassMappedNameCache.Get<CompleteTable>(),
                    new Field("ColumnNumber", typeof(int)),
                    new QueryField("Id", tables.First().Id)).Result;

                // Assert
                Assert.AreEqual(tables.Where(e => e.Id == tables.First().Id).Min(e => e.ColumnNumber), Convert.ToInt32(result));
            }
        }

        [TestMethod]
        public void TestOracleConnectionMinAsyncViaTableNameViaQueryFields()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);
            var queryFields = new[]
            {
                new QueryField("Id", Operation.GreaterThan, tables.First().Id),
                new QueryField("Id", Operation.LessThan, tables.Last().Id)
            };

            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Act
                var result = connection.MinAsync(ClassMappedNameCache.Get<CompleteTable>(),
                    new Field("ColumnNumber", typeof(int)),
                    queryFields).Result;

                // Assert
                Assert.AreEqual(tables.Where(e => e.Id > tables.First().Id && e.Id < tables.Last().Id).Min(e => e.ColumnNumber), Convert.ToInt32(result));
            }
        }

        [TestMethod]
        public void TestOracleConnectionMinAsyncViaTableNameViaQueryGroup()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);
            var queryFields = new[]
            {
                new QueryField("Id", Operation.GreaterThan, tables.First().Id),
                new QueryField("Id", Operation.LessThan, tables.Last().Id)
            };
            var queryGroup = new QueryGroup(queryFields);

            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Act
                var result = connection.MinAsync(ClassMappedNameCache.Get<CompleteTable>(),
                    new Field("ColumnNumber", typeof(int)),
                    queryGroup).Result;

                // Assert
                Assert.AreEqual(tables.Where(e => e.Id > tables.First().Id && e.Id < tables.Last().Id).Min(e => e.ColumnNumber), Convert.ToInt32(result));
            }
        }

        [TestMethod, ExpectedException(typeof(AggregateException))]
        public void ThrowExceptionOnOracleConnectionMinAsyncViaTableNameWithHints()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);

            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Act
                connection.MinAsync(ClassMappedNameCache.Get<CompleteTable>(),
                    new Field("ColumnNumber", typeof(int)),
                    (object)null,
                    hints: "WhatEver").Wait();
            }
        }

        #endregion

        #endregion
    }
}
