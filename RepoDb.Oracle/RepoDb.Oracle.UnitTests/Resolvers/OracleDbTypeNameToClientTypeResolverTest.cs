using Microsoft.VisualStudio.TestTools.UnitTesting;
using RepoDb.Resolvers;
using System;

namespace RepoDb.Oracle.UnitTests.Resolvers
{
    [TestClass]
    public class OracleDbTypeNameToClientTypeResolverTest
    {
        [TestInitialize]
        public void Initialize()
        {
            OracleBootstrap.Initialize();
        }

        [TestMethod]
        public void TestOracleDbTypeNameToClientTypeResolverForBFile()
        {
            // Setup
            var resolver = new OracleDbTypeNameToClientTypeResolver();

            // Act
            var result = resolver.Resolve("BFILE");

            // Assert
            Assert.AreEqual(typeof(byte[]), result);
        }

        [TestMethod]
        public void TestOracleDbTypeNameToClientTypeResolverForBinaryDouble()
        {
            // Setup
            var resolver = new OracleDbTypeNameToClientTypeResolver();

            // Act
            var result = resolver.Resolve("BINARY_DOUBLE");

            // Assert
            Assert.AreEqual(typeof(decimal), result);
        }

        [TestMethod]
        public void TestOracleDbTypeNameToClientTypeResolverForBinaryFloat()
        {
            // Setup
            var resolver = new OracleDbTypeNameToClientTypeResolver();

            // Act
            var result = resolver.Resolve("BINARY_DOUBLE");

            // Assert
            Assert.AreEqual(typeof(decimal), result);
        }

        [TestMethod]
        public void TestOracleDbTypeNameToClientTypeResolverForBinaryInteger()
        {
            // Setup
            var resolver = new OracleDbTypeNameToClientTypeResolver();

            // Act
            var result = resolver.Resolve("BINARY_INTEGER");

            // Assert
            Assert.AreEqual(typeof(decimal), result);
        }

        [TestMethod]
        public void TestOracleDbTypeNameToClientTypeResolverForBlob()
        {
            // Setup
            var resolver = new OracleDbTypeNameToClientTypeResolver();

            // Act
            var result = resolver.Resolve("BLOB");

            // Assert
            Assert.AreEqual(typeof(byte[]), result);
        }

        [TestMethod]
        public void TestOracleDbTypeNameToClientTypeResolverForBoolean()
        {
            // Setup
            var resolver = new OracleDbTypeNameToClientTypeResolver();

            // Act
            var result = resolver.Resolve("BOOLEAN");

            // Assert
            Assert.AreEqual(typeof(bool), result);
        }

        [TestMethod]
        public void TestOracleDbTypeNameToClientTypeResolverForChar()
        {
            // Setup
            var resolver = new OracleDbTypeNameToClientTypeResolver();

            // Act
            var result = resolver.Resolve("CHAR");

            // Assert
            Assert.AreEqual(typeof(string), result);
        }

        [TestMethod]
        public void TestOracleDbTypeNameToClientTypeResolverForClob()
        {
            // Setup
            var resolver = new OracleDbTypeNameToClientTypeResolver();

            // Act
            var result = resolver.Resolve("CLOB");

            // Assert
            Assert.AreEqual(typeof(string), result);
        }

        [TestMethod]
        public void TestOracleDbTypeNameToClientTypeResolverForDate()
        {
            // Setup
            var resolver = new OracleDbTypeNameToClientTypeResolver();

            // Act
            var result = resolver.Resolve("DATE");

            // Assert
            Assert.AreEqual(typeof(DateTime), result);
        }

        [TestMethod]
        public void TestOracleDbTypeNameToClientTypeResolverForIntervalDayToSecond()
        {
            // Setup
            var resolver = new OracleDbTypeNameToClientTypeResolver();

            // Act
            var result = resolver.Resolve("INTERVAL DAY TO SECOND");

            // Assert
            Assert.AreEqual(typeof(TimeSpan), result);
        }

        [TestMethod]
        public void TestOracleDbTypeNameToClientTypeResolverForIntervalYearToMonth()
        {
            // Setup
            var resolver = new OracleDbTypeNameToClientTypeResolver();

            // Act
            var result = resolver.Resolve("INTERVAL YEAR TO MONTH");

            // Assert
            Assert.AreEqual(typeof(long), result);
        }

        [TestMethod]
        public void TestOracleDbTypeNameToClientTypeResolverForJson()
        {
            // Setup
            var resolver = new OracleDbTypeNameToClientTypeResolver();

            // Act
            var result = resolver.Resolve("JSON");

            // Assert
            Assert.AreEqual(typeof(string), result);
        }

        [TestMethod]
        public void TestOracleDbTypeNameToClientTypeResolverForLong()
        {
            // Setup
            var resolver = new OracleDbTypeNameToClientTypeResolver();

            // Act
            var result = resolver.Resolve("LONG");

            // Assert
            Assert.AreEqual(typeof(string), result);
        }

        [TestMethod]
        public void TestOracleDbTypeNameToClientTypeResolverForLongRaw()
        {
            // Setup
            var resolver = new OracleDbTypeNameToClientTypeResolver();

            // Act
            var result = resolver.Resolve("LONG RAW");

            // Assert
            Assert.AreEqual(typeof(byte[]), result);
        }

        [TestMethod]
        public void TestOracleDbTypeNameToClientTypeResolverForNChar()
        {
            // Setup
            var resolver = new OracleDbTypeNameToClientTypeResolver();

            // Act
            var result = resolver.Resolve("NCHAR");

            // Assert
            Assert.AreEqual(typeof(string), result);
        }

        [TestMethod]
        public void TestOracleDbTypeNameToClientTypeResolverForNClob()
        {
            // Setup
            var resolver = new OracleDbTypeNameToClientTypeResolver();

            // Act
            var result = resolver.Resolve("NCLOB");

            // Assert
            Assert.AreEqual(typeof(string), result);
        }

        [TestMethod]
        public void TestOracleDbTypeNameToClientTypeResolverForNumber()
        {
            // Setup
            var resolver = new OracleDbTypeNameToClientTypeResolver();

            // Act
            var result = resolver.Resolve("NUMBER");

            // Assert
            Assert.AreEqual(typeof(decimal), result);
        }

        [TestMethod]
        public void TestOracleDbTypeNameToClientTypeResolverForNVarchar2()
        {
            // Setup
            var resolver = new OracleDbTypeNameToClientTypeResolver();

            // Act
            var result = resolver.Resolve("NVARCHAR2");

            // Assert
            Assert.AreEqual(typeof(string), result);
        }

        [TestMethod]
        public void TestOracleDbTypeNameToClientTypeResolverForPlsInteger()
        {
            // Setup
            var resolver = new OracleDbTypeNameToClientTypeResolver();

            // Act
            var result = resolver.Resolve("PLS_INTEGER");

            // Assert
            Assert.AreEqual(typeof(decimal), result);
        }

        [TestMethod]
        public void TestOracleDbTypeNameToClientTypeResolverForRaw()
        {
            // Setup
            var resolver = new OracleDbTypeNameToClientTypeResolver();

            // Act
            var result = resolver.Resolve("RAW");

            // Assert
            Assert.AreEqual(typeof(byte[]), result);
        }

        [TestMethod]
        public void TestOracleDbTypeNameToClientTypeResolverForRef()
        {
            // Setup
            var resolver = new OracleDbTypeNameToClientTypeResolver();

            // Act
            var result = resolver.Resolve("REF");

            // Assert
            Assert.AreEqual(typeof(string), result);
        }

        [TestMethod]
        public void TestOracleDbTypeNameToClientTypeResolverForRowID()
        {
            // Setup
            var resolver = new OracleDbTypeNameToClientTypeResolver();

            // Act
            var result = resolver.Resolve("ROWID");

            // Assert
            Assert.AreEqual(typeof(string), result);
        }

        [TestMethod]
        public void TestOracleDbTypeNameToClientTypeResolverForTimestamp()
        {
            // Setup
            var resolver = new OracleDbTypeNameToClientTypeResolver();

            // Act
            var result = resolver.Resolve("TIMESTAMP");

            // Assert
            Assert.AreEqual(typeof(DateTime), result);
        }

        [TestMethod]
        public void TestOracleDbTypeNameToClientTypeResolverForTimestampWithLocalTimeZone()
        {
            // Setup
            var resolver = new OracleDbTypeNameToClientTypeResolver();

            // Act
            var result = resolver.Resolve("TIMESTAMP WITH LOCAL TIME ZONE");

            // Assert
            Assert.AreEqual(typeof(DateTime), result);
        }

        [TestMethod]
        public void TestOracleDbTypeNameToClientTypeResolverForTimestampWithTimeZone()
        {
            // Setup
            var resolver = new OracleDbTypeNameToClientTypeResolver();

            // Act
            var result = resolver.Resolve("TIMESTAMP WITH TIME ZONE");

            // Assert
            Assert.AreEqual(typeof(DateTimeOffset), result);
        }

        [TestMethod]
        public void TestOracleDbTypeNameToClientTypeResolverForURowID()
        {
            // Setup
            var resolver = new OracleDbTypeNameToClientTypeResolver();

            // Act
            var result = resolver.Resolve("UROWID");

            // Assert
            Assert.AreEqual(typeof(string), result);
        }

        [TestMethod]
        public void TestOracleDbTypeNameToClientTypeResolverForVarchar2()
        {
            // Setup
            var resolver = new OracleDbTypeNameToClientTypeResolver();

            // Act
            var result = resolver.Resolve("VARCHAR2");

            // Assert
            Assert.AreEqual(typeof(string), result);
        }

        [TestMethod]
        public void TestOracleDbTypeNameToClientTypeResolverForXmlType()
        {
            // Setup
            var resolver = new OracleDbTypeNameToClientTypeResolver();

            // Act
            var result = resolver.Resolve("XMLTYPE");

            // Assert
            Assert.AreEqual(typeof(string), result);
        }

        [TestMethod]
        public void TestOracleDbTypeNameToClientTypeResolverForOthers()
        {
            // Setup
            var resolver = new OracleDbTypeNameToClientTypeResolver();

            // Act
            var result = resolver.Resolve("OTHERS");

            // Assert
            Assert.AreEqual(typeof(object), result);
        }
    }
}
