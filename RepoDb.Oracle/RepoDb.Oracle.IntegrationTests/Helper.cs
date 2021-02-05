using Microsoft.VisualStudio.TestTools.UnitTesting;
using RepoDb.Extensions;
using RepoDb.Oracle.IntegrationTests.Models;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;

namespace RepoDb.Oracle.IntegrationTests
{
    public static class Helper
    {
        static Helper()
        {
            EpocDate = new DateTime(1970, 1, 1, 0, 0, 0);
        }

        #region Properties

        /// <summary>
        /// Gets the value of the Epoc date.
        /// </summary>
        public static DateTime EpocDate { get; }

        /// <summary>
        /// Gets the current <see cref="Random"/> object in used.
        /// </summary>
        public static Random Randomizer => new Random(1);

        #endregion

        #region Methods

        /// <summary>
        /// Asserts the properties equality of 2 types.
        /// </summary>
        /// <typeparam name="T1">The type of first object.</typeparam>
        /// <typeparam name="T2">The type of second object.</typeparam>
        /// <param name="t1">The instance of first object.</param>
        /// <param name="t2">The instance of second object.</param>
        public static void AssertPropertiesEquality<T1, T2>(T1 t1, T2 t2)
        {
            var propertiesOfType1 = typeof(T1).GetProperties();
            var propertiesOfType2 = typeof(T2).GetProperties();
            propertiesOfType1.AsList().ForEach(propertyOfType1 =>
            {
                if (propertyOfType1.Name == "Id")
                {
                    return;
                }
                var propertyOfType2 = propertiesOfType2.FirstOrDefault(p => p.Name == propertyOfType1.Name);
                if (propertyOfType2 == null)
                {
                    return;
                }
                var value1 = propertyOfType1.GetValue(t1);
                var value2 = propertyOfType2.GetValue(t2);
                if (value1 is Array array1 && value2 is Array array2)
                {
                    for (var i = 0; i < Math.Min(array1.Length, array2.Length); i++)
                    {
                        var v1 = array1.GetValue(i);
                        var v2 = array2.GetValue(i);
                        Assert.AreEqual(v1, v2,
                            $"Assert failed for '{propertyOfType1.Name}'. The values are '{value1} ({propertyOfType1.PropertyType.FullName})' and '{value2} ({propertyOfType2.PropertyType.FullName})'.");
                    }
                }
                else
                {
                    Assert.AreEqual(value1, value2,
                        $"Assert failed for '{propertyOfType1.Name}'. The values are '{value1} ({propertyOfType1.PropertyType.FullName})' and '{value2} ({propertyOfType2.PropertyType.FullName})'.");
                }
            });
        }

        /// <summary>
        /// Asserts the members equality of 2 object and <see cref="ExpandoObject"/>.
        /// </summary>
        /// <typeparam name="T">The type of first object.</typeparam>
        /// <param name="obj">The instance of first object.</param>
        /// <param name="expandoObj">The instance of second object.</param>
        public static void AssertMembersEquality(object obj, object expandoObj)
        {
            var dictionary = new ExpandoObject() as IDictionary<string, object>;
            foreach (var property in expandoObj.GetType().GetProperties())
            {
                dictionary.Add(property.Name, property.GetValue(expandoObj));
            }
            AssertMembersEquality(obj, dictionary);
        }

        /// <summary>
        /// Asserts the members equality of 2 object and <see cref="ExpandoObject"/>.
        /// </summary>
        /// <typeparam name="T">The type of first object.</typeparam>
        /// <param name="obj">The instance of first object.</param>
        /// <param name="expandoObj">The instance of second object.</param>
        public static void AssertMembersEquality(object obj, ExpandoObject expandoObj)
        {
            var dictionary = expandoObj as IDictionary<string, object>;
            AssertMembersEquality(obj, dictionary);
        }

        /// <summary>
        /// Asserts the members equality of 2 objects.
        /// </summary>
        /// <typeparam name="T">The type of first object.</typeparam>
        /// <param name="obj">The instance of first object.</param>
        /// <param name="dictionary">The instance of second object.</param>
        public static void AssertMembersEquality(object obj, IDictionary<string, object> dictionary)
        {
            var properties = obj.GetType().GetProperties();
            properties.AsList().ForEach(property =>
            {
                if (property.Name == "Id")
                {
                    return;
                }
                if (dictionary.ContainsKey(property.Name))
                {
                    var value1 = property.GetValue(obj);
                    var value2 = dictionary[property.Name];
                    if (value1 is Array array1 && value2 is Array array2)
                    {
                        for (var i = 0; i < Math.Min(array1.Length, array2.Length); i++)
                        {
                            var v1 = array1.GetValue(i);
                            var v2 = array2.GetValue(i);
                            Assert.AreEqual(v1, v2,
                                $"Assert failed for '{property.Name}'. The values are '{v1}' and '{v2}'.");
                        }
                    }
                    else
                    {
                        var propertyType = property.PropertyType.GetUnderlyingType();
                        if (propertyType == typeof(TimeSpan) && value2 is DateTime dt)
                        {
                            value2 = dt.TimeOfDay;
                        }
                        Assert.AreEqual(Convert.ChangeType(value1, propertyType), Convert.ChangeType(value2, propertyType),
                            $"Assert failed for '{property.Name}'. The values are '{value1}' and '{value2}'.");
                    }
                }
            });
        }

        #endregion

        #region CompleteTable

        /// <summary>
        /// 
        /// </summary>
        /// <param name="count"></param>
        /// <returns></returns>
        public static List<CompleteTable> CreateCompleteTables(int count)
        {
            var tables = new List<CompleteTable>();
            var now = DateTime.SpecifyKind(
                DateTime.UtcNow,
                DateTimeKind.Unspecified);
            var nowTZ = DateTimeOffset.UtcNow;
            var ts = new TimeSpan(12, 34, 56);

            for (var i = 0; i < count; i++)
            {
                tables.Add(new CompleteTable
                {
                    Id = (i + 1),
                    ColumnBFile = new[] {(byte)i},
                    ColumnBinaryDouble = i,
                    ColumnBinaryFloat = i,
                    ColumnBlob = new[] {(byte)i},
                    ColumnChar = $"ColumnChar{i}",
                    ColumnClob = $"ColumnClob{i}",
                    ColumnDate = now,
                    ColumnIntervalDayToSecond = ts,
                    ColumnIntervalYearToMonth = i,
                    // ColumnJson = $"{{\"ColumnJson\": {i}}}",
                    ColumnLong = $"ColumnLong{i}",
                    // ColumnLongRaw = new[] {(byte)i},
                    ColumnNChar = $"ColumnNChar{i}",
                    ColumnNClob = $"ColumnNClob{i}",
                    ColumnNumber = i,
                    ColumnNVarchar2 = $"ColumnNVarchar2{i}",
                    ColumnRaw = new[] {(byte)i},
                    ColumnRowID = null,
                    ColumnTimestamp = now,
                    ColumnTimestampWithLocalTZ = now,
                    ColumnTimestampWithTZ = nowTZ,
                    ColumnURowID = null,
                    ColumnVarchar2 = $"ColumnVarchar2{i}",
                    ColumnXmltype = $"<Xmltype>{i}</Xmltype>",
                });
            }
            return tables;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="table"></param>
        public static void UpdateCompleteTableProperties(CompleteTable table)
        {
            var now = DateTime.SpecifyKind(
                DateTime.UtcNow,
                DateTimeKind.Unspecified);
            var nowTZ = DateTimeOffset.UtcNow;
            var ts = new TimeSpan(65, 43, 21);

            table.ColumnBFile = new[] {(byte)2};
            table.ColumnBinaryDouble = 2;
            table.ColumnBinaryFloat = 2;
            table.ColumnBlob = new[] {(byte)2};
            table.ColumnChar = $"ColumnChar2";
            table.ColumnClob = $"ColumnClob2";
            table.ColumnDate = now;
            table.ColumnIntervalDayToSecond = ts;
            table.ColumnIntervalYearToMonth = 2;
            // table.ColumnJson = $"{{\"ColumnJson\": 2}}";
            table.ColumnLong = $"ColumnLong2";
            // table.ColumnLongRaw = new[] {(byte)2};
            table.ColumnNChar = $"ColumnNChar2";
            table.ColumnNClob = $"ColumnNClob2";
            table.ColumnNumber = 2;
            table.ColumnNVarchar2 = $"ColumnNVarchar22";
            table.ColumnRaw = new[] {(byte)2};
            table.ColumnRowID = null;
            table.ColumnTimestamp = now;
            table.ColumnTimestampWithLocalTZ = now;
            table.ColumnTimestampWithTZ = nowTZ;
            table.ColumnURowID = null;
            table.ColumnVarchar2 = $"ColumnVarchar22";
            table.ColumnXmltype = $"<Xmltype>2</Xmltype>";
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="count"></param>
        /// <returns></returns>
        public static List<dynamic> CreateCompleteTablesAsDynamics(int count)
        {
            var tables = new List<dynamic>();
            var now = DateTime.SpecifyKind(
                DateTime.UtcNow,
                DateTimeKind.Unspecified);
            var nowTZ = DateTimeOffset.UtcNow;
            var ts = new TimeSpan(12, 34, 56);
            for (var i = 0; i < count; i++)
            {
                tables.Add(new
                {
                    Id = (i + 1),
                    ColumnBFile = new[] {(byte)i},
                    ColumnBinaryDouble = i,
                    ColumnBinaryFloat = i,
                    ColumnBlob = new[] {(byte)i},
                    ColumnChar = $"ColumnChar{i}",
                    ColumnClob = $"ColumnClob{i}",
                    ColumnDate = now,
                    ColumnIntervalDayToSecond = ts,
                    ColumnIntervalYearToMonth = i,
                    // ColumnJson = $"{{\"ColumnJson\": {i}}}",
                    ColumnLong = $"ColumnLong{i}",
                    // ColumnLongRaw = new[] {(byte)i},
                    ColumnNChar = $"ColumnNChar{i}",
                    ColumnNClob = $"ColumnNClob{i}",
                    ColumnNumber = i,
                    ColumnNVarchar2 = $"ColumnNVarchar2{i}",
                    ColumnRaw = new[] {(byte)i},
                    //ColumnRowID = null,
                    ColumnTimestamp = now,
                    ColumnTimestampWithLocalTZ = now,
                    ColumnTimestampWithTZ = nowTZ,
                    //ColumnURowID = null,
                    ColumnVarchar2 = $"ColumnVarchar2{i}",
                    ColumnXmltype = $"<Xmltype>{i}</Xmltype>",
                });
            }
            return tables;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="table"></param>
        public static void UpdateCompleteTableAsDynamicProperties(dynamic table)
        {
            var now = DateTime.SpecifyKind(
                DateTime.UtcNow,
                DateTimeKind.Unspecified);
            var nowTZ = DateTimeOffset.UtcNow;
            var ts = new TimeSpan(65, 43, 21);

            table.ColumnBFile = new[] {(byte)2};
            table.ColumnBinaryDouble = 2;
            table.ColumnBinaryFloat = 2;
            table.ColumnBlob = new[] {(byte)2};
            table.ColumnChar = $"ColumnChar2";
            table.ColumnClob = $"ColumnClob2";
            table.ColumnDate = now;
            table.ColumnIntervalDayToSecond = ts;
            table.ColumnIntervalYearToMonth = 2;
            // table.ColumnJson = $"{{\"ColumnJson\": 2}}";
            table.ColumnLong = $"ColumnLong2";
            // table.ColumnLongRaw = new[] {(byte)2};
            table.ColumnNChar = $"ColumnNChar2";
            table.ColumnNClob = $"ColumnNClob2";
            table.ColumnNumber = 2;
            table.ColumnNVarchar2 = $"ColumnNVarchar22";
            table.ColumnRaw = new[] {(byte)2};
            table.ColumnTimestamp = now;
            table.ColumnTimestampWithLocalTZ = now;
            table.ColumnTimestampWithTZ = nowTZ;
            table.ColumnVarchar2 = $"ColumnVarchar22";
            table.ColumnXmltype = $"<Xmltype>2</Xmltype>";
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="count"></param>
        /// <returns></returns>
        public static List<ExpandoObject> CreateCompleteTablesAsExpandoObjects(int count)
        {
            var tables = new List<ExpandoObject>();
            var now = DateTime.SpecifyKind(
                DateTime.UtcNow,
                DateTimeKind.Unspecified);
            var nowTZ = DateTimeOffset.UtcNow;
            var ts = new TimeSpan(12, 34, 56);

            for (var i = 0; i < count; i++)
            {
                var item = new ExpandoObject() as IDictionary<string, object>;
                item["Id"] = (i + 1);
                item["ColumnBFile"] = new[] {(byte)i};
                item["ColumnBinaryDouble"] = i;
                item["ColumnBinaryFloat"] = i;
                item["ColumnBlob"] = new[] {(byte)i};
                item["ColumnChar"] = $"ColumnChar{i}";
                item["ColumnClob"] = $"ColumnClob{i}";
                item["ColumnDate"] = now;
                item["ColumnIntervalDayToSecond"] = ts;
                item["ColumnIntervalYearToMonth"] = i;
                // item["ColumnJson"] = $"{{\"ColumnJson\": {i}}}";
                item["ColumnLong"] = $"ColumnLong{i}";
                // item["ColumnLongRaw"] = new[] {(byte)i};
                item["ColumnNChar"] = $"ColumnNChar{i}";
                item["ColumnNClob"] = $"ColumnNClob{i}";
                item["ColumnNumber"] = i;
                item["ColumnNVarchar2"] = $"ColumnNVarchar2{i}";
                item["ColumnRaw"] = new[] {(byte)i};
                item["ColumnTimestamp"] = now;
                item["ColumnTimestampWithLocalTZ"] = now;
                item["ColumnTimestampWithTZ"] = nowTZ;
                item["ColumnVarchar2"] = $"ColumnVarchar2{i}";
                item["ColumnXmltype"] = $"<Xmltype>{i}</Xmltype>";
                tables.Add((ExpandoObject)item);
            }
            return tables;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="table"></param>
        public static void UpdateCompleteTableAsExpandoObjectProperties(CompleteTable table)
        {
            var now = DateTime.SpecifyKind(
                DateTime.UtcNow,
                DateTimeKind.Unspecified);
            var nowTZ = DateTimeOffset.UtcNow;
            var ts = new TimeSpan(65, 43, 21);

            var item = table as IDictionary<string, object>;
            item["ColumnBFile"] = new[] {(byte)2};
            item["ColumnBinaryDouble"] = 2;
            item["ColumnBinaryFloat"] = 2;
            item["ColumnBlob"] = new[] {(byte)2};
            item["ColumnChar"] = $"ColumnChar2";
            item["ColumnClob"] = $"ColumnClob2";
            item["ColumnDate"] = now;
            item["ColumnIntervalDayToSecond"] = ts;
            item["ColumnIntervalYearToMonth"] = 2;
            // item["ColumnJson"] = $"{{\"ColumnJson\": 2}}";
            item["ColumnLong"] = $"ColumnLong2";
            // item["ColumnLongRaw"] = new[] {(byte)2};
            item["ColumnNChar"] = $"ColumnNChar2";
            item["ColumnNClob"] = $"ColumnNClob2";
            item["ColumnNumber"] = 2;
            item["ColumnNVarchar2"] = $"ColumnNVarchar22";
            item["ColumnRaw"] = new[] {(byte)2};
            item["ColumnRowID"] = null;
            item["ColumnTimestamp"] = now;
            item["ColumnTimestampWithLocalTZ"] = now;
            item["ColumnTimestampWithTZ"] = nowTZ;
            item["ColumnURowID"] = null;
            item["ColumnVarchar2"] = $"ColumnVarchar22";
            item["ColumnXmltype"] = $"<Xmltype>2</Xmltype>";
        }

        #endregion

        #region NonIdentityCompleteTable

        /// <summary>
        /// 
        /// </summary>
        /// <param name="count"></param>
        /// <returns></returns>
        public static List<NonIdentityCompleteTable> CreateNonIdentityCompleteTables(int count)
        {
            var tables = new List<NonIdentityCompleteTable>();
            var now = DateTime.SpecifyKind(
                DateTime.UtcNow,
                DateTimeKind.Unspecified);
            var nowTZ = DateTimeOffset.UtcNow;
            var ts = new TimeSpan(12, 34, 56);
            for (var i = 0; i < count; i++)
            {
                tables.Add(new NonIdentityCompleteTable
                {
                    Id = (i + 1),
                    ColumnBFile = new[] {(byte)i},
                    ColumnBinaryDouble = i,
                    ColumnBinaryFloat = i,
                    ColumnBlob = new[] {(byte)i},
                    ColumnChar = $"ColumnChar{i}",
                    ColumnClob = $"ColumnClob{i}",
                    ColumnDate = now,
                    ColumnIntervalDayToSecond = ts,
                    ColumnIntervalYearToMonth = i,
                    // ColumnJson = $"{{\"ColumnJson\": {i}}}",
                    // ColumnLong = $"ColumnLong{i}",
                    ColumnLongRaw = new[] {(byte)i},
                    ColumnNChar = $"ColumnNChar{i}",
                    ColumnNClob = $"ColumnNClob{i}",
                    ColumnNumber = i,
                    ColumnNVarchar2 = $"ColumnNVarchar2{i}",
                    ColumnRaw = new[] {(byte)i},
                    ColumnRowID = null,
                    ColumnTimestamp = now,
                    ColumnTimestampWithLocalTZ = now,
                    ColumnTimestampWithTZ = nowTZ,
                    ColumnURowID = null,
                    ColumnVarchar2 = $"ColumnVarchar2{i}",
                    ColumnXmltype = $"<Xmltype>{i}</Xmltype>",
                });
            }
            return tables;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="table"></param>
        public static void UpdateNonIdentityCompleteTableProperties(NonIdentityCompleteTable table)
        {
            var now = DateTime.SpecifyKind(
                DateTime.UtcNow,
                DateTimeKind.Unspecified);
            var nowTZ = DateTimeOffset.UtcNow;
            var ts = new TimeSpan(65, 43, 21);

            table.ColumnBFile = new[] {(byte)2};
            table.ColumnBinaryDouble = 2;
            table.ColumnBinaryFloat = 2;
            table.ColumnBlob = new[] {(byte)2};
            table.ColumnChar = $"ColumnChar2";
            table.ColumnClob = $"ColumnClob2";
            table.ColumnDate = now;
            table.ColumnIntervalDayToSecond = ts;
            table.ColumnIntervalYearToMonth = 2;
            // table.ColumnJson = $"{{\"ColumnJson\": 2}}";
            // table.ColumnLong = $"ColumnLong2";
            table.ColumnLongRaw = new[] {(byte)2};
            table.ColumnNChar = $"ColumnNChar2";
            table.ColumnNClob = $"ColumnNClob2";
            table.ColumnNumber = 2;
            table.ColumnNVarchar2 = $"ColumnNVarchar22";
            table.ColumnRaw = new[] {(byte)2};
            table.ColumnRowID = null;
            table.ColumnTimestamp = now;
            table.ColumnTimestampWithLocalTZ = now;
            table.ColumnTimestampWithTZ = nowTZ;
            table.ColumnURowID = null;
            table.ColumnVarchar2 = $"ColumnVarchar22";
            table.ColumnXmltype = $"<Xmltype>2</Xmltype>";
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="count"></param>
        /// <returns></returns>
        public static List<dynamic> CreateNonIdentityCompleteTablesAsDynamics(int count)
        {
            var tables = new List<dynamic>();
            var now = DateTime.SpecifyKind(
                DateTime.UtcNow,
                DateTimeKind.Unspecified);
            var nowTZ = DateTimeOffset.UtcNow;
            var ts = new TimeSpan(12, 34, 56);
            for (var i = 0; i < count; i++)
            {
                tables.Add(new
                {
                    Id = (i + 1),
                    ColumnBFile = new[] {(byte)i},
                    ColumnBinaryDouble = i,
                    ColumnBinaryFloat = i,
                    ColumnBlob = new[] {(byte)i},
                    ColumnChar = $"ColumnChar{i}",
                    ColumnClob = $"ColumnClob{i}",
                    ColumnDate = now,
                    ColumnIntervalDayToSecond = ts,
                    ColumnIntervalYearToMonth = i,
                    // ColumnJson = $"{{\"ColumnJson\": {i}}}",
                    // ColumnLong = $"ColumnLong{i}",
                    ColumnLongRaw = new[] {(byte)i},
                    ColumnNChar = $"ColumnNChar{i}",
                    ColumnNClob = $"ColumnNClob{i}",
                    ColumnNumber = i,
                    ColumnNVarchar2 = $"ColumnNVarchar2{i}",
                    ColumnRaw = new[] {(byte)i},
                    //ColumnRowID = null,
                    ColumnTimestamp = now,
                    ColumnTimestampWithLocalTZ = now,
                    ColumnTimestampWithTZ = nowTZ,
                    //ColumnURowID = null,
                    ColumnVarchar2 = $"ColumnVarchar2{i}",
                    ColumnXmltype = $"<Xmltype>{i}</Xmltype>",
                });
            }
            return tables;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="table"></param>
        public static void UpdateNonIdentityCompleteTableAsDynamicProperties(dynamic table)
        {
            var now = DateTime.SpecifyKind(
                DateTime.UtcNow,
                DateTimeKind.Unspecified);
            var nowTZ = DateTimeOffset.UtcNow;
            var ts = new TimeSpan(65, 43, 21);

            table.ColumnBFile = new[] {(byte)2};
            table.ColumnBinaryDouble = 2;
            table.ColumnBinaryFloat = 2;
            table.ColumnBlob = new[] {(byte)2};
            table.ColumnChar = $"ColumnChar2";
            table.ColumnClob = $"ColumnClob2";
            table.ColumnDate = now;
            table.ColumnIntervalDayToSecond = ts;
            table.ColumnIntervalYearToMonth = 2;
            // table.ColumnJson = $"{{\"ColumnJson\": 2}}";
            // table.ColumnLong = $"ColumnLong2";
            table.ColumnLongRaw = new[] {(byte)2};
            table.ColumnNChar = $"ColumnNChar2";
            table.ColumnNClob = $"ColumnNClob2";
            table.ColumnNumber = 2;
            table.ColumnNVarchar2 = $"ColumnNVarchar22";
            table.ColumnRaw = new[] {(byte)2};
            table.ColumnTimestamp = now;
            table.ColumnTimestampWithLocalTZ = now;
            table.ColumnTimestampWithTZ = nowTZ;
            table.ColumnVarchar2 = $"ColumnVarchar22";
            table.ColumnXmltype = $"<Xmltype>2</Xmltype>";
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="count"></param>
        /// <returns></returns>
        public static List<ExpandoObject> CreateNonIdentityCompleteTablesAsExpandoObjects(int count)
        {
            var tables = new List<ExpandoObject>();
            var now = DateTime.SpecifyKind(
                DateTime.UtcNow,
                DateTimeKind.Unspecified);
            var nowTZ = DateTimeOffset.UtcNow;
            var ts = new TimeSpan(12, 34, 56);

            for (var i = 0; i < count; i++)
            {
                var item = new ExpandoObject() as IDictionary<string, object>;
                item["Id"] = (i + 1);
                item["ColumnBFile"] = new[] {(byte)i};
                item["ColumnBinaryDouble"] = i;
                item["ColumnBinaryFloat"] = i;
                item["ColumnBlob"] = new[] {(byte)i};
                item["ColumnChar"] = $"ColumnChar{i}";
                item["ColumnClob"] = $"ColumnClob{i}";
                item["ColumnDate"] = now;
                item["ColumnIntervalDayToSecond"] = ts;
                item["ColumnIntervalYearToMonth"] = i;
                // item["ColumnJson"] = $"{{\"ColumnJson\": {i}}}";
                // item["ColumnLong"] = $"ColumnLong{i}";
                item["ColumnLongRaw"] = new[] {(byte)i};
                item["ColumnNChar"] = $"ColumnNChar{i}";
                item["ColumnNClob"] = $"ColumnNClob{i}";
                item["ColumnNumber"] = i;
                item["ColumnNVarchar2"] = $"ColumnNVarchar2{i}";
                item["ColumnRaw"] = new[] {(byte)i};
                item["ColumnTimestamp"] = now;
                item["ColumnTimestampWithLocalTZ"] = now;
                item["ColumnTimestampWithTZ"] = nowTZ;
                item["ColumnVarchar2"] = $"ColumnVarchar2{i}";
                item["ColumnXmltype"] = $"<Xmltype>{i}</Xmltype>";
                tables.Add((ExpandoObject)item);
            }
            return tables;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="table"></param>
        public static void UpdateNonIdentityCompleteTableAsExpandoObjectProperties(CompleteTable table)
        {
            var now = DateTime.SpecifyKind(
                DateTime.UtcNow,
                DateTimeKind.Unspecified);
            var nowTZ = DateTimeOffset.UtcNow;
            var ts = new TimeSpan(65, 43, 21);

            var item = table as IDictionary<string, object>;
            item["ColumnBFile"] = new[] {(byte)2};
            item["ColumnBinaryDouble"] = 2;
            item["ColumnBinaryFloat"] = 2;
            item["ColumnBlob"] = new[] {(byte)2};
            item["ColumnChar"] = $"ColumnChar2";
            item["ColumnClob"] = $"ColumnClob2";
            item["ColumnDate"] = now;
            item["ColumnIntervalDayToSecond"] = ts;
            item["ColumnIntervalYearToMonth"] = 2;
            // item["ColumnJson"] = $"{{\"ColumnJson\": 2}}";
            // item["ColumnLong"] = $"ColumnLong2";
            item["ColumnLongRaw"] = new[] {(byte)2};
            item["ColumnNChar"] = $"ColumnNChar2";
            item["ColumnNClob"] = $"ColumnNClob2";
            item["ColumnNumber"] = 2;
            item["ColumnNVarchar2"] = $"ColumnNVarchar22";
            item["ColumnRaw"] = new[] {(byte)2};
            item["ColumnRowID"] = null;
            item["ColumnTimestamp"] = now;
            item["ColumnTimestampWithLocalTZ"] = now;
            item["ColumnTimestampWithTZ"] = nowTZ;
            item["ColumnURowID"] = null;
            item["ColumnVarchar2"] = $"ColumnVarchar22";
            item["ColumnXmltype"] = $"<Xmltype>2</Xmltype>";
        }

        #endregion
    }
}
