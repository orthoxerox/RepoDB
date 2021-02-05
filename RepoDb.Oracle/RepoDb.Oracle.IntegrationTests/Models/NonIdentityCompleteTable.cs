using System;

namespace RepoDb.Oracle.IntegrationTests.Models
{
    public class NonIdentityCompleteTable
    {
        public long Id { get; set; }
        public byte[] ColumnBFile { get; set; }
        public Nullable<decimal> ColumnBinaryDouble { get; set; }
        public Nullable<decimal> ColumnBinaryFloat { get; set; }
        public byte[] ColumnBlob { get; set; }
        public string ColumnChar { get; set; }
        public string ColumnClob { get; set; }
        public Nullable<DateTime> ColumnDate { get; set; }
        public Nullable<TimeSpan> ColumnIntervalDayToSecond { get; set; }
        public Nullable<long> ColumnIntervalYearToMonth { get; set; }
        // public string ColumnJson { get; set; }
        // public string ColumnLong { get; set; }
        public byte[] ColumnLongRaw { get; set; }
        public string ColumnNChar { get; set; }
        public string ColumnNClob { get; set; }
        public Nullable<decimal> ColumnNumber { get; set; }
        public string ColumnNVarchar2 { get; set; }
        public byte[] ColumnRaw { get; set; }
        public string ColumnRowID { get; set; }
        public Nullable<DateTime> ColumnTimestamp { get; set; }
        public Nullable<DateTime> ColumnTimestampWithLocalTZ { get; set; }
        public Nullable<DateTimeOffset> ColumnTimestampWithTZ { get; set; }
        public string ColumnURowID { get; set; }
        public string ColumnVarchar2 { get; set; }
        public string ColumnXmltype { get; set; }
    }
}
