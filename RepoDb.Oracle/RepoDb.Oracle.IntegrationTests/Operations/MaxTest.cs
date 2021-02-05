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
    public class MaxTest
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
        public void TestOracleConnectionMaxWithoutExpression()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);

            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Act
                var result = connection.Max<CompleteTable>(e => e.ColumnNumber,
                    (object)null);

                // Assert
                Assert.AreEqual(tables.Max(e => e.ColumnNumber), Convert.ToInt32(result));
            }
        }

        [TestMethod]
        public void TestOracleConnectionMaxViaExpression()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);
            var ids = new[] { tables.First().Id, tables.Last().Id };

            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Act
                var result = connection.Max<CompleteTable>(e => e.ColumnNumber,
                    e => ids.Contains(e.Id));

                // Assert
                Assert.AreEqual(tables.Where(e => ids.Contains(e.Id)).Max(e => e.ColumnNumber), Convert.ToInt32(result));
            }
        }

        [TestMethod]
        public void TestOracleConnectionMaxViaDynamic()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);

            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Act
                var result = connection.Max<CompleteTable>(e => e.ColumnNumber,
                    new { tables.First().Id });

                // Assert
                Assert.AreEqual(tables.Where(e => e.Id == tables.First().Id).Max(e => e.ColumnNumber), Convert.ToInt32(result));
            }
        }

        [TestMethod]
        public void TestOracleConnectionMaxViaQueryField()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);

            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Act
                var result = connection.Max<CompleteTable>(e => e.ColumnNumber,
                    new QueryField("Id", tables.First().Id));

                // Assert
                Assert.AreEqual(tables.Where(e => e.Id == tables.First().Id).Max(e => e.ColumnNumber), Convert.ToInt32(result));
            }
        }

        [TestMethod]
        public void TestOracleConnectionMaxViaQueryFields()
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
                var result = connection.Max<CompleteTable>(e => e.ColumnNumber,
                    queryFields);

                // Assert
                Assert.AreEqual(tables.Where(e => e.Id > tables.First().Id && e.Id < tables.Last().Id).Max(e => e.ColumnNumber), Convert.ToInt32(result));
            }
        }

        [TestMethod]
        public void TestOracleConnectionMaxViaQueryGroup()
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
                var result = connection.Max<CompleteTable>(e => e.ColumnNumber,
                    queryGroup);

                // Assert
                Assert.AreEqual(tables.Where(e => e.Id > tables.First().Id && e.Id < tables.Last().Id).Max(e => e.ColumnNumber), Convert.ToInt32(result));
            }
        }

        [TestMethod, ExpectedException(typeof(NotSupportedException))]
        public void ThrowExceptionOnOracleConnectionMaxWithHints()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);

            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Act
                connection.Max<CompleteTable>(e => e.ColumnNumber,
                    (object)null,
                    hints: "WhatEver");
            }
        }

        #endregion

        #region Async

        [TestMethod]
        public void TestOracleConnectionMaxAsyncWithoutExpression()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);

            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Act
                var result = connection.MaxAsync<CompleteTable>(e => e.ColumnNumber,
                    (object)null).Result;

                // Assert
                Assert.AreEqual(tables.Max(e => e.ColumnNumber), Convert.ToInt32(result));
            }
        }

        [TestMethod]
        public void TestOracleConnectionMaxAsyncViaExpression()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);
            var ids = new[] { tables.First().Id, tables.Last().Id };

            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Act
                var result = connection.MaxAsync<CompleteTable>(e => e.ColumnNumber,
                    e => ids.Contains(e.Id)).Result;

                // Assert
                Assert.AreEqual(tables.Where(e => ids.Contains(e.Id)).Max(e => e.ColumnNumber), Convert.ToInt32(result));
            }
        }

        [TestMethod]
        public void TestOracleConnectionMaxAsyncViaDynamic()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);

            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Act
                var result = connection.MaxAsync<CompleteTable>(e => e.ColumnNumber,
                    new { tables.First().Id }).Result;

                // Assert
                Assert.AreEqual(tables.Where(e => e.Id == tables.First().Id).Max(e => e.ColumnNumber), Convert.ToInt32(result));
            }
        }

        [TestMethod]
        public void TestOracleConnectionMaxAsyncViaQueryField()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);

            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Act
                var result = connection.MaxAsync<CompleteTable>(e => e.ColumnNumber,
                    new QueryField("Id", tables.First().Id)).Result;

                // Assert
                Assert.AreEqual(tables.Where(e => e.Id == tables.First().Id).Max(e => e.ColumnNumber), Convert.ToInt32(result));
            }
        }

        [TestMethod]
        public void TestOracleConnectionMaxAsyncViaQueryFields()
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
                var result = connection.MaxAsync<CompleteTable>(e => e.ColumnNumber,
                    queryFields).Result;

                // Assert
                Assert.AreEqual(tables.Where(e => e.Id > tables.First().Id && e.Id < tables.Last().Id).Max(e => e.ColumnNumber), Convert.ToInt32(result));
            }
        }

        [TestMethod]
        public void TestOracleConnectionMaxAsyncViaQueryGroup()
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
                var result = connection.MaxAsync<CompleteTable>(e => e.ColumnNumber,
                    queryGroup).Result;

                // Assert
                Assert.AreEqual(tables.Where(e => e.Id > tables.First().Id && e.Id < tables.Last().Id).Max(e => e.ColumnNumber), Convert.ToInt32(result));
            }
        }

        [TestMethod, ExpectedException(typeof(AggregateException))]
        public void ThrowExceptionOnOracleConnectionMaxAsyncWithHints()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);

            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Act
                connection.MaxAsync<CompleteTable>(e => e.ColumnNumber,
                    (object)null,
                    hints: "WhatEver").Wait();
            }
        }

        #endregion

        #endregion

        #region TableName

        #region Sync

        [TestMethod]
        public void TestOracleConnectionMaxViaTableNameWithoutExpression()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);

            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Act
                var result = connection.Max(ClassMappedNameCache.Get<CompleteTable>(),
                    new Field("ColumnNumber", typeof(int)),
                    (object)null);

                // Assert
                Assert.AreEqual(tables.Max(e => e.ColumnNumber), Convert.ToInt32(result));
            }
        }

        [TestMethod]
        public void TestOracleConnectionMaxViaTableNameViaDynamic()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);

            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Act
                var result = connection.Max(ClassMappedNameCache.Get<CompleteTable>(),
                    new Field("ColumnNumber", typeof(int)),
                    new { tables.First().Id });

                // Assert
                Assert.AreEqual(tables.Where(e => e.Id == tables.First().Id).Max(e => e.ColumnNumber), Convert.ToInt32(result));
            }
        }

        [TestMethod]
        public void TestOracleConnectionMaxViaTableNameViaQueryField()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);

            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Act
                var result = connection.Max(ClassMappedNameCache.Get<CompleteTable>(),
                    new Field("ColumnNumber", typeof(int)),
                    new QueryField("Id", tables.First().Id));

                // Assert
                Assert.AreEqual(tables.Where(e => e.Id == tables.First().Id).Max(e => e.ColumnNumber), Convert.ToInt32(result));
            }
        }

        [TestMethod]
        public void TestOracleConnectionMaxViaTableNameViaQueryFields()
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
                var result = connection.Max(ClassMappedNameCache.Get<CompleteTable>(),
                    new Field("ColumnNumber", typeof(int)),
                    queryFields);

                // Assert
                Assert.AreEqual(tables.Where(e => e.Id > tables.First().Id && e.Id < tables.Last().Id).Max(e => e.ColumnNumber), Convert.ToInt32(result));
            }
        }

        [TestMethod]
        public void TestOracleConnectionMaxViaTableNameViaQueryGroup()
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
                var result = connection.Max(ClassMappedNameCache.Get<CompleteTable>(),
                    new Field("ColumnNumber", typeof(int)),
                    queryGroup);

                // Assert
                Assert.AreEqual(tables.Where(e => e.Id > tables.First().Id && e.Id < tables.Last().Id).Max(e => e.ColumnNumber), Convert.ToInt32(result));
            }
        }

        [TestMethod, ExpectedException(typeof(NotSupportedException))]
        public void ThrowExceptionOnOracleConnectionMaxViaTableNameWithHints()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);

            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Act
                connection.Max(ClassMappedNameCache.Get<CompleteTable>(),
                    new Field("ColumnNumber", typeof(int)),
                    (object)null,
                    hints: "WhatEver");
            }
        }

        #endregion

        #region Async

        [TestMethod]
        public void TestOracleConnectionMaxAsyncViaTableNameWithoutExpression()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);

            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Act
                var result = connection.MaxAsync(ClassMappedNameCache.Get<CompleteTable>(),
                    new Field("ColumnNumber", typeof(int)),
                    (object)null).Result;

                // Assert
                Assert.AreEqual(tables.Max(e => e.ColumnNumber), Convert.ToInt32(result));
            }
        }

        [TestMethod]
        public void TestOracleConnectionMaxAsyncViaTableNameViaDynamic()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);

            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Act
                var result = connection.MaxAsync(ClassMappedNameCache.Get<CompleteTable>(),
                    new Field("ColumnNumber", typeof(int)),
                    new { tables.First().Id }).Result;

                // Assert
                Assert.AreEqual(tables.Where(e => e.Id == tables.First().Id).Max(e => e.ColumnNumber), Convert.ToInt32(result));
            }
        }

        [TestMethod]
        public void TestOracleConnectionMaxAsyncViaTableNameViaQueryField()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);

            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Act
                var result = connection.MaxAsync(ClassMappedNameCache.Get<CompleteTable>(),
                    new Field("ColumnNumber", typeof(int)),
                    new QueryField("Id", tables.First().Id)).Result;

                // Assert
                Assert.AreEqual(tables.Where(e => e.Id == tables.First().Id).Max(e => e.ColumnNumber), Convert.ToInt32(result));
            }
        }

        [TestMethod]
        public void TestOracleConnectionMaxAsyncViaTableNameViaQueryFields()
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
                var result = connection.MaxAsync(ClassMappedNameCache.Get<CompleteTable>(),
                    new Field("ColumnNumber", typeof(int)),
                    queryFields).Result;

                // Assert
                Assert.AreEqual(tables.Where(e => e.Id > tables.First().Id && e.Id < tables.Last().Id).Max(e => e.ColumnNumber), Convert.ToInt32(result));
            }
        }

        [TestMethod]
        public void TestOracleConnectionMaxAsyncViaTableNameViaQueryGroup()
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
                var result = connection.MaxAsync(ClassMappedNameCache.Get<CompleteTable>(),
                    new Field("ColumnNumber", typeof(int)),
                    queryGroup).Result;

                // Assert
                Assert.AreEqual(tables.Where(e => e.Id > tables.First().Id && e.Id < tables.Last().Id).Max(e => e.ColumnNumber), Convert.ToInt32(result));
            }
        }

        [TestMethod, ExpectedException(typeof(AggregateException))]
        public void ThrowExceptionOnOracleConnectionMaxAsyncViaTableNameWithHints()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);

            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Act
                connection.MaxAsync(ClassMappedNameCache.Get<CompleteTable>(),
                    new Field("ColumnNumber", typeof(int)),
                    (object)null,
                    hints: "WhatEver").Wait();
            }
        }

        #endregion

        #endregion
    }
}
