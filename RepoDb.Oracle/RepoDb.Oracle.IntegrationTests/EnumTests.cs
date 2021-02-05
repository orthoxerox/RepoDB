using Microsoft.VisualStudio.TestTools.UnitTesting;
using Oracle.ManagedDataAccess.Client;
using RepoDb.Attributes;
using RepoDb.Extensions;
using RepoDb.Oracle.IntegrationTests.Setup;
using System;
using System.Collections.Generic;
using System.Linq;

namespace RepoDb.Oracle.IntegrationTests
{
    [TestClass]
    public class EnumTests
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

        #region Enumerations

        public enum Hands
        {
            Unidentified,
            Left,
            Right
        }

        #endregion

        #region SubClasses

        [Map("CompleteTable")]
        public class PersonWithText
        {
            public System.Int64 Id { get; set; }
            public Hands? ColumnVarchar2 { get; set; }
        }

        [Map("CompleteTable")]
        public class PersonWithInteger
        {
            public System.Int64 Id { get; set; }
            public Hands? ColumnNumber { get; set; }
        }

        [Map("CompleteTable")]
        public class PersonWithTextAsInteger
        {
            public System.Int64 Id { get; set; }
            [TypeMap(System.Data.DbType.Int32)]
            public Hands? ColumnVarchar2 { get; set; }
        }

        #endregion

        #region Helpers

        public IEnumerable<PersonWithText> GetPersonWithText(int count)
        {
            var random = new Random();
            for (var i = 0; i < count; i++)
            {
                var hand = random.Next(100) > 50 ? Hands.Right : Hands.Left;
                yield return new PersonWithText
                {
                    Id = i,
                    ColumnVarchar2 = hand
                };
            }
        }

        public IEnumerable<PersonWithInteger> GetPersonWithInteger(int count)
        {
            var random = new Random();
            for (var i = 0; i < count; i++)
            {
                var hand = random.Next(100) > 50 ? Hands.Right : Hands.Left;
                yield return new PersonWithInteger
                {
                    Id = i,
                    ColumnNumber = hand
                };
            }
        }

        public IEnumerable<PersonWithTextAsInteger> GetPersonWithTextAsInteger(int count)
        {
            var random = new Random();
            for (var i = 0; i < count; i++)
            {
                var hand = random.Next(100) > 50 ? Hands.Right : Hands.Left;
                yield return new PersonWithTextAsInteger
                {
                    Id = i,
                    ColumnVarchar2 = hand
                };
            }
        }

        #endregion

        [TestMethod]
        public void TestInsertAndQueryEnumAsTextAsNull()
        {
            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Setup
                var person = GetPersonWithText(1).First();
                person.ColumnVarchar2 = null;

                // Act
                connection.Insert(person);

                // Query
                var queryResult = connection.Query<PersonWithText>(person.Id).First();

                // Assert
                Assert.IsNull(queryResult.ColumnVarchar2);
            }
        }

        [TestMethod]
        public void TestInsertAndQueryEnumAsText()
        {
            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Setup
                var person = GetPersonWithText(1).First();

                // Act
                connection.Insert(person);

                // Query
                var queryResult = connection.Query<PersonWithText>(person.Id).First();

                // Assert
                Assert.AreEqual(person.ColumnVarchar2, queryResult.ColumnVarchar2);
            }
        }

        [TestMethod]
        public void TestInsertAndQueryEnumAsTextByBatch()
        {
            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Setup
                var people = GetPersonWithText(10).AsList();

                // Act
                connection.InsertAll(people);

                // Query
                var queryResult = connection.QueryAll<PersonWithText>().AsList();

                // Assert
                people.ForEach(p =>
                {
                    var item = queryResult.First(e => e.Id == p.Id);
                    Assert.AreEqual(p.ColumnVarchar2, item.ColumnVarchar2);
                });
            }
        }

        [TestMethod]
        public void TestInsertAndQueryEnumAsIntegerAsNull()
        {
            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Setup
                var person = GetPersonWithInteger(1).First();
                person.ColumnNumber = null;

                // Act
                connection.Insert(person);

                // Query
                var queryResult = connection.Query<PersonWithInteger>(person.Id).First();

                // Assert
                Assert.IsNull(queryResult.ColumnNumber);
            }
        }

        [TestMethod]
        public void TestInsertAndQueryEnumAsInteger()
        {
            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Setup
                var person = GetPersonWithInteger(1).First();

                // Act
                connection.Insert(person);

                // Query
                var queryResult = connection.Query<PersonWithInteger>(person.Id).First();

                // Assert
                Assert.AreEqual(person.ColumnNumber, queryResult.ColumnNumber);
            }
        }

        [TestMethod]
        public void TestInsertAndQueryEnumAsIntegerAsBatch()
        {
            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Setup
                var people = GetPersonWithInteger(10).AsList();

                // Act
                connection.InsertAll(people);

                // Query
                var queryResult = connection.QueryAll<PersonWithInteger>().AsList();

                // Assert
                people.ForEach(p =>
                {
                    var item = queryResult.First(e => e.Id == p.Id);
                    Assert.AreEqual(p.ColumnNumber, item.ColumnNumber);
                });
            }
        }

        [TestMethod]
        public void TestInsertAndQueryEnumAsTextAsInt()
        {
            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Setup
                var person = GetPersonWithTextAsInteger(1).First();

                // Act
                connection.Insert(person);

                // Query
                var queryResult = connection.Query<PersonWithTextAsInteger>(person.Id).First();

                // Assert
                Assert.AreEqual(person.ColumnVarchar2, queryResult.ColumnVarchar2);
            }
        }

        [TestMethod]
        public void TestInsertAndQueryEnumAsTextAsIntAsBatch()
        {
            using (var connection = new OracleConnection(Database.ConnectionString))
            {
                // Setup
                var people = GetPersonWithTextAsInteger(10).AsList();

                // Act
                connection.InsertAll(people);

                // Query
                var queryResult = connection.QueryAll<PersonWithTextAsInteger>().AsList();

                // Assert
                people.ForEach(p =>
                {
                    var item = queryResult.First(e => e.Id == p.Id);
                    Assert.AreEqual(p.ColumnVarchar2, item.ColumnVarchar2);
                });
            }
        }
    }
}
