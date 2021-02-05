using Microsoft.VisualStudio.TestTools.UnitTesting;
using Oracle.ManagedDataAccess.Client;
using RepoDb.Resolvers;
using System;

namespace RepoDb.Oracle.UnitTests.Resolvers
{
    [TestClass]
    public class OracleConvertFieldResolverTest
    {
        [TestInitialize]
        public void Initialize()
        {
            OracleBootstrap.Initialize();
        }

        [TestMethod]
        public void TestOracleConvertFieldResolverForInt32()
        {
            // Setup
            var setting = DbSettingMapper.Get<OracleConnection>();
            var resolver = new OracleConvertFieldResolver();
            var field = new Field("Field", typeof(int));

            // Act
            var result = resolver.Resolve(field, setting);

            // Assert
            Assert.AreEqual("CAST(\"Field\" AS NUMBER)", result);
        }

        [TestMethod]
        public void TestOracleConvertFieldResolverForInt64()
        {
            // Setup
            var setting = DbSettingMapper.Get<OracleConnection>();
            var resolver = new OracleConvertFieldResolver();
            var field = new Field("Field", typeof(long));

            // Act
            var result = resolver.Resolve(field, setting);

            // Assert
            Assert.AreEqual("CAST(\"Field\" AS NUMBER)", result);
        }

        [TestMethod]
        public void TestOracleConvertFieldResolverForInt16()
        {
            // Setup
            var setting = DbSettingMapper.Get<OracleConnection>();
            var resolver = new OracleConvertFieldResolver();
            var field = new Field("Field", typeof(short));

            // Act
            var result = resolver.Resolve(field, setting);

            // Assert
            Assert.AreEqual("CAST(\"Field\" AS NUMBER)", result);
        }

        [TestMethod]
        public void TestOracleConvertFieldResolverForDateTime()
        {
            // Setup
            var setting = DbSettingMapper.Get<OracleConnection>();
            var resolver = new OracleConvertFieldResolver();
            var field = new Field("Field", typeof(DateTime));

            // Act
            var result = resolver.Resolve(field, setting);

            // Assert
            Assert.AreEqual("CAST(\"Field\" AS TIMESTAMP)", result);
        }

        [TestMethod]
        public void TestOracleConvertFieldResolverForString()
        {
            // Setup
            var setting = DbSettingMapper.Get<OracleConnection>();
            var resolver = new OracleConvertFieldResolver();
            var field = new Field("Field", typeof(string));

            // Act
            var result = resolver.Resolve(field, setting);

            // Assert
            Assert.AreEqual("CAST(\"Field\" AS NVARCHAR2)", result);
        }

        [TestMethod]
        public void TestOracleConvertFieldResolverForByte()
        {
            // Setup
            var setting = DbSettingMapper.Get<OracleConnection>();
            var resolver = new OracleConvertFieldResolver();
            var field = new Field("Field", typeof(byte));

            // Act
            var result = resolver.Resolve(field, setting);

            // Assert
            Assert.AreEqual("CAST(\"Field\" AS NUMBER)", result);
        }

        [TestMethod]
        public void TestOracleConvertFieldResolverForDecimal()
        {
            // Setup
            var setting = DbSettingMapper.Get<OracleConnection>();
            var resolver = new OracleConvertFieldResolver();
            var field = new Field("Field", typeof(decimal));

            // Act
            var result = resolver.Resolve(field, setting);

            // Assert
            Assert.AreEqual("CAST(\"Field\" AS NUMBER)", result);
        }

        [TestMethod]
        public void TestOracleConvertFieldResolverForFloat()
        {
            // Setup
            var setting = DbSettingMapper.Get<OracleConnection>();
            var resolver = new OracleConvertFieldResolver();
            var field = new Field("Field", typeof(float));

            // Act
            var result = resolver.Resolve(field, setting);

            // Assert
            Assert.AreEqual("CAST(\"Field\" AS NUMBER)", result);
        }

        [TestMethod]
        public void TestOracleConvertFieldResolverForTimeSpan()
        {
            // Setup
            var setting = DbSettingMapper.Get<OracleConnection>();
            var resolver = new OracleConvertFieldResolver();
            var field = new Field("Field", typeof(TimeSpan));

            // Act
            var result = resolver.Resolve(field, setting);

            // Assert
            Assert.AreEqual("CAST(\"Field\" AS INTERVAL DAY TO SECOND)", result);
        }
    }
}
