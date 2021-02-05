using Microsoft.VisualStudio.TestTools.UnitTesting;
using Oracle.ManagedDataAccess.Client;
using RepoDb.Enumerations;
using RepoDb.Exceptions;
using System;

namespace RepoDb.Oracle.UnitTests
{
    [TestClass]
    public class StatementBuilderTest
    {
        [TestInitialize]
        public void Initialize()
        {
            OracleBootstrap.Initialize();
        }

        #region CreateBatchQuery

        [TestMethod]
        public void TestOracleStatementBuilderCreateBatchQuery()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            var query = builder.CreateBatchQuery(new QueryBuilder(),
                "Table",
                Field.From("Id", "Name"),
                0,
                10,
                OrderField.Parse(new { Id = Order.Ascending }));
            var expected = "SELECT \"Id\", \"Name\" FROM \"Table\" ORDER BY \"Id\" ASC FETCH FIRST 10 ROWS ONLY";

            // Assert
            Assert.AreEqual(expected, query);
        }

        [TestMethod]
        public void TestOracleStatementBuilderCreateBatchQueryWithPage()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            var query = builder.CreateBatchQuery(new QueryBuilder(),
                "Table",
                Field.From("Id", "Name"),
                3,
                10,
                OrderField.Parse(new { Id = Order.Ascending }));
            var expected = "SELECT \"Id\", \"Name\" FROM \"Table\" ORDER BY \"Id\" ASC OFFSET 30 ROWS FETCH NEXT 10 ROWS ONLY";

            // Assert
            Assert.AreEqual(expected, query);
        }

        [TestMethod, ExpectedException(typeof(NullReferenceException))]
        public void ThrowExceptionOnOracleStatementBuilderCreateBatchQueryIfThereAreNoFields()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            builder.CreateBatchQuery(new QueryBuilder(),
                "Table",
                null,
                0,
                10,
                OrderField.Parse(new { Id = Order.Ascending }));
        }

        [TestMethod, ExpectedException(typeof(EmptyException))]
        public void ThrowExceptionOnOracleStatementBuilderCreateBatchQueryIfThereAreNoOrderFields()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            builder.CreateBatchQuery(new QueryBuilder(),
                "Table",
                Field.From("Id", "Name"),
                0,
                10,
                null);
        }

        [TestMethod, ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void ThrowExceptionOnOracleStatementBuilderCreateBatchQueryIfThePageValueIsNullOrOutOfRange()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            builder.CreateBatchQuery(new QueryBuilder(),
                "Table",
                Field.From("Id", "Name"),
                -1,
                10,
                OrderField.Parse(new { Id = Order.Ascending }));
        }

        [TestMethod, ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void ThrowExceptionOnOracleStatementBuilderCreateBatchQueryIfTheRowsPerBatchValueIsNullOrOutOfRange()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            builder.CreateBatchQuery(new QueryBuilder(),
                "Table",
                Field.From("Id", "Name"),
                0,
                -1,
                OrderField.Parse(new { Id = Order.Ascending }));
        }

        [TestMethod]
        public void TestOracleStatementBuilderCreateBatchQueryWithHints()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            var query = builder.CreateBatchQuery(new QueryBuilder(),
                "Table",
                Field.From("Id", "Name"),
                0,
                10,
                OrderField.Parse(new { Id = Order.Ascending }),
                null,
                "/*+ HINT */");

            var expected = "SELECT /*+ HINT */ \"Id\", \"Name\" FROM \"Table\" ORDER BY \"Id\" ASC FETCH FIRST 10 ROWS ONLY";

            // Assert
            Assert.AreEqual(expected, query);
        }

        #endregion

        #region CreateCount

        [TestMethod]
        public void TestOracleStatementBuilderCreateCount()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            var query = builder.CreateCount(new QueryBuilder(),
                "Table",
                null,
                null);
            var expected = "SELECT COUNT (*) AS \"CountValue\" FROM \"Table\"";

            // Assert
            Assert.AreEqual(expected, query);
        }

        [TestMethod]
        public void TestOracleStatementBuilderCreateCountWithExpression()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            var query = builder.CreateCount(new QueryBuilder(),
                "Table",
                QueryGroup.Parse(new { Id = 1 }),
                null);
            var expected = "SELECT COUNT (*) AS \"CountValue\" FROM \"Table\" WHERE (\"Id\" = :Id)";

            // Assert
            Assert.AreEqual(expected, query);
        }

        [TestMethod]
        public void TestOracleStatementBuilderCreateCountWithExpressionAndHints()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            var query = builder.CreateCount(new QueryBuilder(),
                "Table",
                QueryGroup.Parse(new { Id = 1 }),
                "/*+ HINT */");

            var expected = "SELECT /*+ HINT */ COUNT (*) AS \"CountValue\" FROM \"Table\" WHERE (\"Id\" = :Id)";

            // Assert
            Assert.AreEqual(expected, query);
        }

        #endregion

        #region CreateCountAll

        [TestMethod]
        public void TestOracleStatementBuilderCreateCountAll()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            var query = builder.CreateCountAll(new QueryBuilder(),
                "Table",
                null);
            var expected = "SELECT COUNT (*) AS \"CountValue\" FROM \"Table\"";

            // Assert
            Assert.AreEqual(expected, query);
        }

        [TestMethod]
        public void TestOracleStatementBuilderCreateCountAllWithHints()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            var query = builder.CreateCountAll(new QueryBuilder(),
                "Table",
                "/*+ HINT */");

            var expected = "SELECT /*+ HINT */ COUNT (*) AS \"CountValue\" FROM \"Table\"";

            // Assert
            Assert.AreEqual(expected, query);
        }

        #endregion

        #region CreateExists

        [TestMethod]
        public void TestOracleStatementBuilderCreateExists()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            var query = builder.CreateExists(new QueryBuilder(),
                "Table",
                QueryGroup.Parse(new { Id = 1 }));
            var expected = "SELECT 1 AS \"ExistsValue\" FROM \"Table\" WHERE (\"Id\" = :Id) FETCH FIRST 1 ROW ONLY";

            // Assert
            Assert.AreEqual(expected, query);
        }

        #endregion

        #region CreateInsert

        [TestMethod]
        public void TestOracleStatementBuilderCreateInsert()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            var query = builder.CreateInsert(new QueryBuilder(),
                "Table",
                Field.From("Id", "Name", "Address"),
                null,
                null);
            var expected = "INSERT INTO \"Table\" ( \"Id\", \"Name\", \"Address\" ) VALUES ( :Id, :Name, :Address ) RETURNING NULL INTO :Result";

            // Assert
            Assert.AreEqual(expected, query);
        }

        [TestMethod]
        public void TestOracleStatementBuilderCreateInsertWithPrimary()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            var query = builder.CreateInsert(new QueryBuilder(),
                "Table",
                Field.From("Id", "Name", "Address"),
                new DbField("Id", true, false, false, typeof(int), null, null, null, null),
                null);
            var expected = "INSERT INTO \"Table\" ( \"Id\", \"Name\", \"Address\" ) VALUES ( :Id, :Name, :Address ) RETURNING \"Id\" INTO :Result";

            // Assert
            Assert.AreEqual(expected, query);
        }

        [TestMethod]
        public void TestOracleStatementBuilderCreateInsertWithIdentity()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            var query = builder.CreateInsert(new QueryBuilder(),
                "Table",
                Field.From("Id", "Name", "Address"),
                null,
                new DbField("Id", false, true, false, typeof(int), null, null, null, null));
            var expected = "INSERT INTO \"Table\" ( \"Name\", \"Address\" ) VALUES ( :Name, :Address ) RETURNING CAST(\"Id\" AS NUMBER) INTO :Result";

            // Assert
            Assert.AreEqual(expected, query);
        }

        [TestMethod]
        public void TestOracleStatementBuilderCreateInsertWothHints()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            var query = builder.CreateInsert(new QueryBuilder(),
                "Table",
                Field.From("Id", "Name", "Address"),
                null,
                new DbField("Id", false, true, false, typeof(int), null, null, null, null),
                "/*+ HINT */");
            var expected = "INSERT /*+ HINT */ INTO \"Table\" ( \"Name\", \"Address\" ) VALUES ( :Name, :Address ) RETURNING CAST(\"Id\" AS NUMBER) INTO :Result";

            // Assert
            Assert.AreEqual(expected, query);
        }

        #endregion

        #region CreateInsertAll

        [TestMethod]
        public void TestOracleStatementBuilderCreateInsertAll()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            var query = builder.CreateInsertAll(new QueryBuilder(),
                "Table",
                Field.From("Id", "Name", "Address"),
                3,
                null,
                null);
            var expected = "BEGIN INSERT INTO \"Table\" ( \"Id\", \"Name\", \"Address\" ) VALUES ( :Id, :Name, :Address ) ; " +
                "INSERT INTO \"Table\" ( \"Id\", \"Name\", \"Address\" ) VALUES ( :Id_1, :Name_1, :Address_1 ) ; " +
                "INSERT INTO \"Table\" ( \"Id\", \"Name\", \"Address\" ) VALUES ( :Id_2, :Name_2, :Address_2 ) ; END ;";

            // Assert
            Assert.AreEqual(expected, query);
        }

        [TestMethod]
        public void TestOracleStatementBuilderCreateInsertAllWithPrimary()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            var query = builder.CreateInsertAll(new QueryBuilder(),
                "Table",
                Field.From("Id", "Name", "Address"),
                3,
                new DbField("Id", true, false, false, typeof(int), null, null, null, null),
                null);
            var expected = "BEGIN INSERT INTO \"Table\" ( \"Id\", \"Name\", \"Address\" ) VALUES ( :Id, :Name, :Address ) ; " +
                "INSERT INTO \"Table\" ( \"Id\", \"Name\", \"Address\" ) VALUES ( :Id_1, :Name_1, :Address_1 ) ; " +
                "INSERT INTO \"Table\" ( \"Id\", \"Name\", \"Address\" ) VALUES ( :Id_2, :Name_2, :Address_2 ) ; END ;";

            // Assert
            Assert.AreEqual(expected, query);
        }

        [TestMethod]
        public void TestOracleStatementBuilderCreateInsertAllWithIdentity()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            var query = builder.CreateInsertAll(new QueryBuilder(),
                "Table",
                Field.From("Id", "Name", "Address"),
                3,
                null,
                new DbField("Id", false, true, false, typeof(int), null, null, null, null));
            var expected = "BEGIN INSERT INTO \"Table\" ( \"Name\", \"Address\" ) VALUES ( :Name, :Address ) ; " +
                "INSERT INTO \"Table\" ( \"Name\", \"Address\" ) VALUES ( :Name_1, :Address_1 ) ; " +
                "INSERT INTO \"Table\" ( \"Name\", \"Address\" ) VALUES ( :Name_2, :Address_2 ) ; END ;";

            // Assert
            Assert.AreEqual(expected, query);
        }

        [TestMethod]
        public void TestOracleStatementBuilderCreateInsertAllWithHints()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            var query = builder.CreateInsertAll(new QueryBuilder(),
                "Table",
                Field.From("Id", "Name", "Address"),
                3,
                new DbField("Id", true, false, false, typeof(int), null, null, null, null),
                null,
                "/*+ HINT */");
            var expected = "BEGIN INSERT /*+ HINT */ INTO \"Table\" ( \"Id\", \"Name\", \"Address\" ) VALUES ( :Id, :Name, :Address ) ; " +
                "INSERT /*+ HINT */ INTO \"Table\" ( \"Id\", \"Name\", \"Address\" ) VALUES ( :Id_1, :Name_1, :Address_1 ) ; " +
                "INSERT /*+ HINT */ INTO \"Table\" ( \"Id\", \"Name\", \"Address\" ) VALUES ( :Id_2, :Name_2, :Address_2 ) ; END ;";

            // Assert
            Assert.AreEqual(expected, query);
        }

        #endregion

        #region CreateMax

        [TestMethod]
        public void TestOracleStatementBuilderCreateMax()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            var query = builder.CreateMax(new QueryBuilder(),
                "Table",
                new Field("Field", typeof(int)),
                null,
                null);
            var expected = "SELECT MAX (\"Field\") AS \"MaxValue\" FROM \"Table\"";

            // Assert
            Assert.AreEqual(expected, query);
        }

        [TestMethod]
        public void TestOracleStatementBuilderCreateMaxWithExpression()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            var query = builder.CreateMax(new QueryBuilder(),
                "Table",
                new Field("Field", typeof(int)),
                QueryGroup.Parse(new { Id = 1 }),
                null);
            var expected = "SELECT MAX (\"Field\") AS \"MaxValue\" FROM \"Table\" WHERE (\"Id\" = :Id)";

            // Assert
            Assert.AreEqual(expected, query);
        }

        [TestMethod]
        public void TestOracleStatementBuilderCreateMaxWithHints()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            var query = builder.CreateMax(new QueryBuilder(),
                "Table",
                new Field("Field", typeof(int)),
                QueryGroup.Parse(new { Id = 1 }),
                "/*+ HINT */");
            var expected = "SELECT /*+ HINT */ MAX (\"Field\") AS \"MaxValue\" FROM \"Table\" WHERE (\"Id\" = :Id)";

            // Assert
            Assert.AreEqual(expected, query);
        }

        #endregion

        #region CreateMaxAll

        [TestMethod]
        public void TestOracleStatementBuilderCreateMaxAll()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            var query = builder.CreateMaxAll(new QueryBuilder(),
                "Table",
                new Field("Field", typeof(int)),
                null);
            var expected = "SELECT MAX (\"Field\") AS \"MaxValue\" FROM \"Table\"";

            // Assert
            Assert.AreEqual(expected, query);
        }

        [TestMethod]
        public void TestOracleStatementBuilderCreateMaxAllWithHints()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            var query = builder.CreateMaxAll(new QueryBuilder(),
                "Table",
                new Field("Field", typeof(int)),
                "/*+ HINT */");
            var expected = "SELECT /*+ HINT */ MAX (\"Field\") AS \"MaxValue\" FROM \"Table\"";

            // Assert
            Assert.AreEqual(expected, query);
        }

        #endregion

        #region CreateMin

        [TestMethod]
        public void TestOracleStatementBuilderCreateMin()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            var query = builder.CreateMin(new QueryBuilder(),
                "Table",
                new Field("Field", typeof(int)),
                null,
                null);
            var expected = "SELECT MIN (\"Field\") AS \"MinValue\" FROM \"Table\"";

            // Assert
            Assert.AreEqual(expected, query);
        }

        [TestMethod]
        public void TestOracleStatementBuilderCreateMinWithExpression()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            var query = builder.CreateMin(new QueryBuilder(),
                "Table",
                new Field("Field", typeof(int)),
                QueryGroup.Parse(new { Id = 1 }),
                null);
            var expected = "SELECT MIN (\"Field\") AS \"MinValue\" FROM \"Table\" WHERE (\"Id\" = :Id)";

            // Assert
            Assert.AreEqual(expected, query);
        }

        [TestMethod]
        public void TestOracleStatementBuilderCreateMinWithHints()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            var query = builder.CreateMin(new QueryBuilder(),
                "Table",
                new Field("Field", typeof(int)),
                QueryGroup.Parse(new { Id = 1 }),
                "/*+ HINT */");
            var expected = "SELECT /*+ HINT */ MIN (\"Field\") AS \"MinValue\" FROM \"Table\" WHERE (\"Id\" = :Id)";

            // Assert
            Assert.AreEqual(expected, query);
        }

        #endregion

        #region CreateMinAll

        [TestMethod]
        public void TestOracleStatementBuilderCreateMinAll()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            var query = builder.CreateMinAll(new QueryBuilder(),
                "Table",
                new Field("Field", typeof(int)),
                null);
            var expected = "SELECT MIN (\"Field\") AS \"MinValue\" FROM \"Table\"";

            // Assert
            Assert.AreEqual(expected, query);
        }

        [TestMethod]
        public void TestOracleStatementBuilderCreateMinAllWithHints()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            var query = builder.CreateMinAll(new QueryBuilder(),
                "Table",
                new Field("Field", typeof(int)),
                "/*+ HINT */");

            var expected = "SELECT /*+ HINT */ MIN (\"Field\") AS \"MinValue\" FROM \"Table\"";

            // Assert
            Assert.AreEqual(expected, query);
        }

        #endregion

        #region CreateMerge

        [TestMethod]
        public void TestOracleStatementBuilderCreateMerge()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            var query = builder.CreateMerge(new QueryBuilder(),
                "Table",
                Field.From("Id", "Name", "Address"),
                null,
                new DbField("Id", true, false, false, typeof(int), null, null, null, null),
                null);
            var expected = @"MERGE INTO ""Table"" USING ( " +
                @"SELECT :Id AS ""Id"", :Name AS ""Name"", :Address AS ""Address"" FROM ""DUAL"" " +
                @") ""_SOURCE"" ON ( ""Table"".""Id"" = ""_SOURCE"".""Id"" ) " +
                @"WHEN MATCHED THEN UPDATE SET ""Table"".""Name"" = ""_SOURCE"".""Name"", ""Table"".""Address"" = ""_SOURCE"".""Address"" " +
                @"WHEN NOT MATCHED THEN INSERT ( ""Id"", ""Name"", ""Address"" ) " +
                @"VALUES ( ""_SOURCE"".""Id"", ""_SOURCE"".""Name"", ""_SOURCE"".""Address"" )";

            // Assert
            Assert.AreEqual(expected, query);
        }

        [TestMethod]
        public void TestOracleStatementBuilderCreateMergeWithPrimaryAsQualifier()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            var query = builder.CreateMerge(new QueryBuilder(),
                "Table",
                Field.From("Id", "Name", "Address"),
                Field.From("Id"),
                new DbField("Id", true, false, false, typeof(int), null, null, null, null),
                null);
            var expected = @"MERGE INTO ""Table"" USING ( " +
                @"SELECT :Id AS ""Id"", :Name AS ""Name"", :Address AS ""Address"" FROM ""DUAL"" " +
                @") ""_SOURCE"" ON ( ""Table"".""Id"" = ""_SOURCE"".""Id"" ) " +
                @"WHEN MATCHED THEN UPDATE SET ""Table"".""Name"" = ""_SOURCE"".""Name"", ""Table"".""Address"" = ""_SOURCE"".""Address"" " +
                @"WHEN NOT MATCHED THEN INSERT ( ""Id"", ""Name"", ""Address"" ) " +
                @"VALUES ( ""_SOURCE"".""Id"", ""_SOURCE"".""Name"", ""_SOURCE"".""Address"" )";

            // Assert
            Assert.AreEqual(expected, query);
        }

        [TestMethod]
        public void TestOracleStatementBuilderCreateMergeWithIdentity()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            var query = builder.CreateMerge(new QueryBuilder(),
                "Table",
                Field.From("Id", "Name", "Address"),
                null,
                new DbField("Id", true, false, false, typeof(int), null, null, null, null),
                new DbField("Id", false, true, false, typeof(int), null, null, null, null));
            var expected = @"MERGE INTO ""Table"" USING ( " +
                @"SELECT :Id AS ""Id"", :Name AS ""Name"", :Address AS ""Address"" FROM ""DUAL"" " +
                @") ""_SOURCE"" ON ( ""Table"".""Id"" = ""_SOURCE"".""Id"" ) " +
                @"WHEN MATCHED THEN UPDATE SET ""Table"".""Name"" = ""_SOURCE"".""Name"", ""Table"".""Address"" = ""_SOURCE"".""Address"" " +
                @"WHEN NOT MATCHED THEN INSERT ( ""Name"", ""Address"" ) " +
                @"VALUES ( ""_SOURCE"".""Name"", ""_SOURCE"".""Address"" )";

            // Assert
            Assert.AreEqual(expected, query);
        }

        [TestMethod]
        public void TestOracleStatementBuilderCreateMergeWhenThereIsNoPrimary()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            var query = builder.CreateMerge(new QueryBuilder(),
                "Table",
                Field.From("Id", "Name", "Address"),
                Field.From("Name"),
                null,
                null);
            var expected = @"MERGE INTO ""Table"" USING ( " +
                @"SELECT :Id AS ""Id"", :Name AS ""Name"", :Address AS ""Address"" FROM ""DUAL"" " +
                @") ""_SOURCE"" ON ( ""Table"".""Name"" = ""_SOURCE"".""Name"" ) " +
                @"WHEN MATCHED THEN UPDATE SET ""Table"".""Id"" = ""_SOURCE"".""Id"", ""Table"".""Address"" = ""_SOURCE"".""Address"" " +
                @"WHEN NOT MATCHED THEN INSERT ( ""Id"", ""Name"", ""Address"" ) " +
                @"VALUES ( ""_SOURCE"".""Id"", ""_SOURCE"".""Name"", ""_SOURCE"".""Address"" )";

            // Assert
            Assert.AreEqual(expected, query);
        }

        [TestMethod, ExpectedException(typeof(PrimaryFieldNotFoundException))]
        public void ThrowExceptionOnOracleStatementBuilderCreateMergeIfThereAreNoFields()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            builder.CreateMerge(new QueryBuilder(),
                "Table",
                Field.From("Id", "Name", "Address"),
                null,
                null,
                null);
        }

        [TestMethod]
        public void TestOracleStatementBuilderCreateMergeWhenThereAreOtherFieldsAsQualifers()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            var query = builder.CreateMerge(new QueryBuilder(),
                "Table",
                Field.From("Id", "Name", "Address"),
                Field.From("Id", "Name"),
                new DbField("Id", true, false, false, typeof(int), null, null, null, null),
                null);
            var expected = @"MERGE INTO ""Table"" USING ( " +
                @"SELECT :Id AS ""Id"", :Name AS ""Name"", :Address AS ""Address"" FROM ""DUAL"" " +
                @") ""_SOURCE"" ON ( ""Table"".""Id"" = ""_SOURCE"".""Id"" AND ""Table"".""Name"" = ""_SOURCE"".""Name"" ) " +
                @"WHEN MATCHED THEN UPDATE SET ""Table"".""Address"" = ""_SOURCE"".""Address"" " +
                @"WHEN NOT MATCHED THEN INSERT ( ""Id"", ""Name"", ""Address"" ) " +
                @"VALUES ( ""_SOURCE"".""Id"", ""_SOURCE"".""Name"", ""_SOURCE"".""Address"" )";

            // Assert
            Assert.AreEqual(expected, query);
        }

        [TestMethod]
        public void TestOracleStatementBuilderCreateMergeWithHints()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            var query = builder.CreateMerge(new QueryBuilder(),
                "Table",
                Field.From("Id", "Name", "Address"),
                Field.From("Id"),
                new DbField("Id", true, false, false, typeof(int), null, null, null, null),
                null,
                "/*+ HINT */");
            var expected = @"MERGE /*+ HINT */ INTO ""Table"" USING ( " +
                @"SELECT :Id AS ""Id"", :Name AS ""Name"", :Address AS ""Address"" FROM ""DUAL"" " +
                @") ""_SOURCE"" ON ( ""Table"".""Id"" = ""_SOURCE"".""Id"" ) " +
                @"WHEN MATCHED THEN UPDATE SET ""Table"".""Name"" = ""_SOURCE"".""Name"", ""Table"".""Address"" = ""_SOURCE"".""Address"" " +
                @"WHEN NOT MATCHED THEN INSERT ( ""Id"", ""Name"", ""Address"" ) " +
                @"VALUES ( ""_SOURCE"".""Id"", ""_SOURCE"".""Name"", ""_SOURCE"".""Address"" )";

            // Assert
            Assert.AreEqual(expected, query);
        }

        #endregion

        #region CreateMergeAll

        [TestMethod]
        public void TestOracleStatementBuilderCreateMergeAll()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            var query = builder.CreateMergeAll(new QueryBuilder(),
                "Table",
                Field.From("Id", "Name", "Address"),
                null,
                3,
                new DbField("Id", true, false, false, typeof(int), null, null, null, null),
                null);
            var expected = @"MERGE INTO ""Table"" USING ( " +
                @"SELECT :Id AS ""Id"", :Name AS ""Name"", :Address AS ""Address"" FROM ""DUAL"" UNION ALL " +
                @"SELECT :Id_1 AS ""Id"", :Name_1 AS ""Name"", :Address_1 AS ""Address"" FROM ""DUAL"" UNION ALL " +
                @"SELECT :Id_2 AS ""Id"", :Name_2 AS ""Name"", :Address_2 AS ""Address"" FROM ""DUAL"" " +
                @") ""_SOURCE"" ON ( ""Table"".""Id"" = ""_SOURCE"".""Id"" ) " +
                @"WHEN MATCHED THEN UPDATE SET ""Table"".""Name"" = ""_SOURCE"".""Name"", ""Table"".""Address"" = ""_SOURCE"".""Address"" " +
                @"WHEN NOT MATCHED THEN INSERT ( ""Id"", ""Name"", ""Address"" ) " +
                @"VALUES ( ""_SOURCE"".""Id"", ""_SOURCE"".""Name"", ""_SOURCE"".""Address"" )";

            // Assert
            Assert.AreEqual(expected, query);
        }

        [TestMethod]
        public void TestOracleStatementBuilderCreateMergeAllWithPrimaryAsQualifier()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            var query = builder.CreateMergeAll(new QueryBuilder(),
                "Table",
                Field.From("Id", "Name", "Address"),
                Field.From("Id"),
                3,
                new DbField("Id", true, false, false, typeof(int), null, null, null, null),
                null);
            var expected = @"MERGE INTO ""Table"" USING ( " +
                @"SELECT :Id AS ""Id"", :Name AS ""Name"", :Address AS ""Address"" FROM ""DUAL"" UNION ALL " +
                @"SELECT :Id_1 AS ""Id"", :Name_1 AS ""Name"", :Address_1 AS ""Address"" FROM ""DUAL"" UNION ALL " +
                @"SELECT :Id_2 AS ""Id"", :Name_2 AS ""Name"", :Address_2 AS ""Address"" FROM ""DUAL"" " +
                @") ""_SOURCE"" ON ( ""Table"".""Id"" = ""_SOURCE"".""Id"" ) " +
                @"WHEN MATCHED THEN UPDATE SET ""Table"".""Name"" = ""_SOURCE"".""Name"", ""Table"".""Address"" = ""_SOURCE"".""Address"" " +
                @"WHEN NOT MATCHED THEN INSERT ( ""Id"", ""Name"", ""Address"" ) " +
                @"VALUES ( ""_SOURCE"".""Id"", ""_SOURCE"".""Name"", ""_SOURCE"".""Address"" )";
            // Assert
            Assert.AreEqual(expected, query);
        }

        [TestMethod]
        public void TestOracleStatementBuilderCreateMergeAllWithIdentity()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            var query = builder.CreateMergeAll(new QueryBuilder(),
                "Table",
                Field.From("Id", "Name", "Address"),
                null,
                3,
                new DbField("Id", true, false, false, typeof(int), null, null, null, null),
                new DbField("Id", false, true, false, typeof(int), null, null, null, null));
            var expected = @"MERGE INTO ""Table"" USING ( " +
                @"SELECT :Id AS ""Id"", :Name AS ""Name"", :Address AS ""Address"" FROM ""DUAL"" UNION ALL " +
                @"SELECT :Id_1 AS ""Id"", :Name_1 AS ""Name"", :Address_1 AS ""Address"" FROM ""DUAL"" UNION ALL " +
                @"SELECT :Id_2 AS ""Id"", :Name_2 AS ""Name"", :Address_2 AS ""Address"" FROM ""DUAL"" " +
                @") ""_SOURCE"" ON ( ""Table"".""Id"" = ""_SOURCE"".""Id"" ) " +
                @"WHEN MATCHED THEN UPDATE SET ""Table"".""Name"" = ""_SOURCE"".""Name"", ""Table"".""Address"" = ""_SOURCE"".""Address"" " +
                @"WHEN NOT MATCHED THEN INSERT ( ""Name"", ""Address"" ) " +
                @"VALUES ( ""_SOURCE"".""Name"", ""_SOURCE"".""Address"" )";

            // Assert
            Assert.AreEqual(expected, query);
        }

        [TestMethod, ExpectedException(typeof(PrimaryFieldNotFoundException))]
        public void ThrowExceptionOnOracleStatementBuilderCreateMergeAllIfThereAreNoFields()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            builder.CreateMergeAll(new QueryBuilder(),
                "Table",
                Field.From("Id", "Name", "Address"),
                null,
                3,
                null,
                null);
        }

        [TestMethod]
        public void TestOracleStatementBuilderCreateMergeAllWhenThereAreOtherFieldsAsQualifers()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            var query = builder.CreateMergeAll(new QueryBuilder(),
                "Table",
                Field.From("Id", "Name", "Address"),
                Field.From("Id", "Name"),
                3,
                new DbField("Id", true, false, false, typeof(int), null, null, null, null),
                null);
            var expected = @"MERGE INTO ""Table"" USING ( " +
                @"SELECT :Id AS ""Id"", :Name AS ""Name"", :Address AS ""Address"" FROM ""DUAL"" UNION ALL " +
                @"SELECT :Id_1 AS ""Id"", :Name_1 AS ""Name"", :Address_1 AS ""Address"" FROM ""DUAL"" UNION ALL " +
                @"SELECT :Id_2 AS ""Id"", :Name_2 AS ""Name"", :Address_2 AS ""Address"" FROM ""DUAL"" " +
                @") ""_SOURCE"" ON ( ""Table"".""Id"" = ""_SOURCE"".""Id"" AND ""Table"".""Name"" = ""_SOURCE"".""Name"" ) " +
                @"WHEN MATCHED THEN UPDATE SET ""Table"".""Address"" = ""_SOURCE"".""Address"" " +
                @"WHEN NOT MATCHED THEN INSERT ( ""Id"", ""Name"", ""Address"" ) " +
                @"VALUES ( ""_SOURCE"".""Id"", ""_SOURCE"".""Name"", ""_SOURCE"".""Address"" )";

            // Assert
            Assert.AreEqual(expected, query);
        }

        [TestMethod]
        public void TestOracleStatementBuilderCreateMergeAllWithHints()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            var query = builder.CreateMergeAll(new QueryBuilder(),
                "Table",
                Field.From("Id", "Name", "Address"),
                Field.From("Id"),
                3,
                new DbField("Id", true, false, false, typeof(int), null, null, null, null),
                null,
                "/*+ HINT */");

            var expected = @"MERGE /*+ HINT */ INTO ""Table"" USING ( " +
                @"SELECT :Id AS ""Id"", :Name AS ""Name"", :Address AS ""Address"" FROM ""DUAL"" UNION ALL " +
                @"SELECT :Id_1 AS ""Id"", :Name_1 AS ""Name"", :Address_1 AS ""Address"" FROM ""DUAL"" UNION ALL " +
                @"SELECT :Id_2 AS ""Id"", :Name_2 AS ""Name"", :Address_2 AS ""Address"" FROM ""DUAL"" " +
                @") ""_SOURCE"" ON ( ""Table"".""Id"" = ""_SOURCE"".""Id"" ) " +
                @"WHEN MATCHED THEN UPDATE SET ""Table"".""Name"" = ""_SOURCE"".""Name"", ""Table"".""Address"" = ""_SOURCE"".""Address"" " +
                @"WHEN NOT MATCHED THEN INSERT ( ""Id"", ""Name"", ""Address"" ) " +
                @"VALUES ( ""_SOURCE"".""Id"", ""_SOURCE"".""Name"", ""_SOURCE"".""Address"" )";

            // Assert
            Assert.AreEqual(expected, query);
        }

        #endregion

        #region CreateQuery

        [TestMethod]
        public void TestOracleStatementBuilderCreateQuery()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            var query = builder.CreateQuery(new QueryBuilder(),
                "Table",
                Field.From("Id", "Name", "Address"),
                null,
                null,
                null,
                null);
            var expected = "SELECT \"Id\", \"Name\", \"Address\" FROM \"Table\"";

            // Assert
            Assert.AreEqual(expected, query);
        }

        [TestMethod]
        public void TestOracleStatementBuilderCreateQueryWithExpression()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            var query = builder.CreateQuery(new QueryBuilder(),
                "Table",
                Field.From("Id", "Name", "Address"),
                QueryGroup.Parse(new { Id = 1, Name = "Michael" }),
                null,
                null,
                null);
            var expected = "SELECT \"Id\", \"Name\", \"Address\" FROM \"Table\" WHERE (\"Id\" = :Id AND \"Name\" = :Name)";

            // Assert
            Assert.AreEqual(expected, query);
        }

        [TestMethod]
        public void TestOracleStatementBuilderCreateQueryWithTop()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            var query = builder.CreateQuery(new QueryBuilder(),
                "Table",
                Field.From("Id", "Name", "Address"),
                null,
                null,
                10,
                null);
            var expected = "SELECT \"Id\", \"Name\", \"Address\" FROM \"Table\" FETCH FIRST 10 ROWS ONLY";

            // Assert
            Assert.AreEqual(expected, query);
        }

        [TestMethod]
        public void TestOracleStatementBuilderCreateQueryOrderBy()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            var query = builder.CreateQuery(new QueryBuilder(),
                "Table",
                Field.From("Id", "Name", "Address"),
                null,
                OrderField.Parse(new { Id = Order.Ascending }),
                null,
                null);
            var expected = "SELECT \"Id\", \"Name\", \"Address\" FROM \"Table\" ORDER BY \"Id\" ASC";

            // Assert
            Assert.AreEqual(expected, query);
        }

        [TestMethod]
        public void TestOracleStatementBuilderCreateQueryOrderByFields()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            var query = builder.CreateQuery(new QueryBuilder(),
                "Table",
                Field.From("Id", "Name", "Address"),
                null,
                OrderField.Parse(new { Id = Order.Ascending, Name = Order.Ascending }),
                null,
                null);
            var expected = "SELECT \"Id\", \"Name\", \"Address\" FROM \"Table\" ORDER BY \"Id\" ASC, \"Name\" ASC";

            // Assert
            Assert.AreEqual(expected, query);
        }

        [TestMethod]
        public void TestOracleStatementBuilderCreateQueryOrderByDescending()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            var query = builder.CreateQuery(new QueryBuilder(),
                "Table",
                Field.From("Id", "Name", "Address"),
                null,
                OrderField.Parse(new { Id = Order.Descending }),
                null,
                null);
            var expected = "SELECT \"Id\", \"Name\", \"Address\" FROM \"Table\" ORDER BY \"Id\" DESC";

            // Assert
            Assert.AreEqual(expected, query);
        }

        [TestMethod]
        public void TestOracleStatementBuilderCreateQueryOrderByFieldsDescending()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            var query = builder.CreateQuery(new QueryBuilder(),
                "Table",
                Field.From("Id", "Name", "Address"),
                null,
                OrderField.Parse(new { Id = Order.Descending, Name = Order.Descending }),
                null,
                null);
            var expected = "SELECT \"Id\", \"Name\", \"Address\" FROM \"Table\" ORDER BY \"Id\" DESC, \"Name\" DESC";

            // Assert
            Assert.AreEqual(expected, query);
        }

        [TestMethod]
        public void TestOracleStatementBuilderCreateQueryOrderByFieldsMultiDirection()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            var query = builder.CreateQuery(new QueryBuilder(),
                "Table",
                Field.From("Id", "Name", "Address"),
                null,
                OrderField.Parse(new { Id = Order.Ascending, Name = Order.Descending }),
                null,
                null);
            var expected = "SELECT \"Id\", \"Name\", \"Address\" FROM \"Table\" ORDER BY \"Id\" ASC, \"Name\" DESC";

            // Assert
            Assert.AreEqual(expected, query);
        }

        [TestMethod, ExpectedException(typeof(MissingFieldsException))]
        public void ThrowExceptionOnOracleStatementBuilderCreateQueryIfOrderFieldsAreNotPresentAtTheFields()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            builder.CreateQuery(new QueryBuilder(),
                "Table",
                Field.From("Id", "Name", "Address"),
                null,
                OrderField.Parse(new { Id = Order.Descending, SSN = Order.Ascending }),
                null,
                null);
        }

        [TestMethod]
        public void TestOracleStatementBuilderCreateQueryWithHints()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            var query = builder.CreateQuery(new QueryBuilder(),
                "Table",
                Field.From("Id", "Name", "Address"),
                null,
                null,
                null,
                "/*+ HINT */");
            var expected = "SELECT /*+ HINT */ \"Id\", \"Name\", \"Address\" FROM \"Table\"";

            // Assert
            Assert.AreEqual(expected, query);
        }

        #endregion

        #region CreateSum

        [TestMethod]
        public void TestOracleStatementBuilderCreateSum()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            var query = builder.CreateSum(new QueryBuilder(),
                "Table",
                new Field("Field", typeof(int)),
                null,
                null);
            var expected = "SELECT SUM (\"Field\") AS \"SumValue\" FROM \"Table\"";

            // Assert
            Assert.AreEqual(expected, query);
        }

        [TestMethod]
        public void TestOracleStatementBuilderCreateSumWithExpression()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            var query = builder.CreateSum(new QueryBuilder(),
                "Table",
                new Field("Field", typeof(int)),
                QueryGroup.Parse(new { Id = 1 }),
                null);
            var expected = "SELECT SUM (\"Field\") AS \"SumValue\" FROM \"Table\" WHERE (\"Id\" = :Id)";

            // Assert
            Assert.AreEqual(expected, query);
        }

        [TestMethod]
        public void TestOracleStatementBuilderCreateSumWithHints()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            var query = builder.CreateSum(new QueryBuilder(),
                "Table",
                new Field("Field", typeof(int)),
                QueryGroup.Parse(new { Id = 1 }),
                "/*+ HINT */");
            var expected = "SELECT /*+ HINT */ SUM (\"Field\") AS \"SumValue\" FROM \"Table\" WHERE (\"Id\" = :Id)";

            // Assert
            Assert.AreEqual(expected, query);
        }

        #endregion

        #region CreateSumAll

        [TestMethod]
        public void TestOracleStatementBuilderCreateSumAll()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            var query = builder.CreateSumAll(new QueryBuilder(),
                "Table",
                new Field("Field", typeof(int)),
                null);
            var expected = "SELECT SUM (\"Field\") AS \"SumValue\" FROM \"Table\"";

            // Assert
            Assert.AreEqual(expected, query);
        }

        [TestMethod]
        public void TestOracleStatementBuilderCreateSumAllWithHints()
        {
            // Setup
            var builder = StatementBuilderMapper.Get<OracleConnection>();

            // Act
            var query = builder.CreateSumAll(new QueryBuilder(),
                "Table",
                new Field("Field", typeof(int)),
                "/*+ HINT */");
            var expected = "SELECT /*+ HINT */ SUM (\"Field\") AS \"SumValue\" FROM \"Table\"";

            // Assert
            Assert.AreEqual(expected, query);
        }

        #endregion

        //TODO DELETE
    }
}
