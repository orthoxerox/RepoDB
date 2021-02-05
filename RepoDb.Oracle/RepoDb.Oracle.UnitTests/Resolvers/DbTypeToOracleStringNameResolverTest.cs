using Microsoft.VisualStudio.TestTools.UnitTesting;
using RepoDb.Resolvers;
using System.Data;

namespace RepoDb.Oracle.UnitTests.Resolvers
{
    [TestClass]
    public class DbTypeToOracleStringNameResolverTest
    {
        [TestMethod]
        public void TestDbTypeToOracleStringNameResolverInt64()
        {
            // Setup
            var resolver = new DbTypeToOracleStringNameResolver();

            // Assert
            Assert.AreEqual("NUMBER", resolver.Resolve(DbType.Int64));
        }

        [TestMethod]
        public void TestDbTypeToOracleStringNameResolverByte()
        {
            // Setup
            var resolver = new DbTypeToOracleStringNameResolver();

            // Assert
            Assert.AreEqual("NUMBER", resolver.Resolve(DbType.Byte));
        }

        [TestMethod]
        public void TestDbTypeToOracleStringNameResolverBinary()
        {
            // Setup
            var resolver = new DbTypeToOracleStringNameResolver();

            // Assert
            Assert.AreEqual("RAW", resolver.Resolve(DbType.Binary));
        }

        [TestMethod]
        public void TestDbTypeToOracleStringNameResolverBoolean()
        {
            // Setup
            var resolver = new DbTypeToOracleStringNameResolver();

            // Assert
            Assert.AreEqual("BOOLEAN", resolver.Resolve(DbType.Boolean));
        }

        [TestMethod]
        public void TestDbTypeToOracleStringNameResolverString()
        {
            // Setup
            var resolver = new DbTypeToOracleStringNameResolver();

            // Assert
            Assert.AreEqual("NVARCHAR2", resolver.Resolve(DbType.String));
        }

        [TestMethod]
        public void TestDbTypeToOracleStringNameResolverAnsiString()
        {
            // Setup
            var resolver = new DbTypeToOracleStringNameResolver();

            // Assert
            Assert.AreEqual("VARCHAR2", resolver.Resolve(DbType.AnsiString));
        }

        [TestMethod]
        public void TestDbTypeToOracleStringNameResolverAnsiStringFixedLength()
        {
            // Setup
            var resolver = new DbTypeToOracleStringNameResolver();

            // Assert
            Assert.AreEqual("CHAR", resolver.Resolve(DbType.AnsiStringFixedLength));
        }

        [TestMethod]
        public void TestDbTypeToOracleStringNameResolverStringFixedLength()
        {
            // Setup
            var resolver = new DbTypeToOracleStringNameResolver();

            // Assert
            Assert.AreEqual("NCHAR", resolver.Resolve(DbType.StringFixedLength));
        }

        [TestMethod]
        public void TestDbTypeToOracleStringNameResolverDate()
        {
            // Setup
            var resolver = new DbTypeToOracleStringNameResolver();

            // Assert
            Assert.AreEqual("DATE", resolver.Resolve(DbType.Date));
        }

        [TestMethod]
        public void TestDbTypeToOracleStringNameResolverDateTime()
        {
            // Setup
            var resolver = new DbTypeToOracleStringNameResolver();

            // Assert
            Assert.AreEqual("TIMESTAMP", resolver.Resolve(DbType.DateTime));
        }

        [TestMethod]
        public void TestDbTypeToOracleStringNameResolverDateTime2()
        {
            // Setup
            var resolver = new DbTypeToOracleStringNameResolver();

            // Assert
            Assert.AreEqual("TIMESTAMP", resolver.Resolve(DbType.DateTime2));
        }

        [TestMethod]
        public void TestDbTypeToOracleStringNameResolverDateTimeOffset()
        {
            // Setup
            var resolver = new DbTypeToOracleStringNameResolver();

            // Assert
            Assert.AreEqual("TIMESTAMP WITH TIME ZONE", resolver.Resolve(DbType.DateTimeOffset));
        }

        [TestMethod]
        public void TestDbTypeToOracleStringNameResolverDecimal()
        {
            // Setup
            var resolver = new DbTypeToOracleStringNameResolver();

            // Assert
            Assert.AreEqual("NUMBER", resolver.Resolve(DbType.Decimal));
        }

        [TestMethod]
        public void TestDbTypeToOracleStringNameResolverSingle()
        {
            // Setup
            var resolver = new DbTypeToOracleStringNameResolver();

            // Assert
            Assert.AreEqual("NUMBER", resolver.Resolve(DbType.Single));
        }

        [TestMethod]
        public void TestDbTypeToOracleStringNameResolverDouble()
        {
            // Setup
            var resolver = new DbTypeToOracleStringNameResolver();

            // Assert
            Assert.AreEqual("NUMBER", resolver.Resolve(DbType.Double));
        }

        [TestMethod]
        public void TestDbTypeToOracleStringNameResolverInt32()
        {
            // Setup
            var resolver = new DbTypeToOracleStringNameResolver();

            // Assert
            Assert.AreEqual("NUMBER", resolver.Resolve(DbType.Int32));
        }

        [TestMethod]
        public void TestDbTypeToOracleStringNameResolverInt16()
        {
            // Setup
            var resolver = new DbTypeToOracleStringNameResolver();

            // Assert
            Assert.AreEqual("NUMBER", resolver.Resolve(DbType.Int16));
        }

        [TestMethod]
        public void TestDbTypeToOracleStringNameResolverTime()
        {
            // Setup
            var resolver = new DbTypeToOracleStringNameResolver();

            // Assert
            Assert.AreEqual("INTERVAL DAY TO SECOND", resolver.Resolve(DbType.Time));
        }
    }
}
