using RepoDb.Interfaces;
using System.Data;

namespace RepoDb.Resolvers
{
    /// <summary>
    /// A class used to resolve the <see cref="DbType"/> into its equivalent database string name.
    /// </summary>
    public class DbTypeToOracleStringNameResolver : IResolver<DbType, string>
    {
        /// <summary>
        /// Returns the equivalent <see cref="DbType"/> of the .NET CLR Types.
        /// </summary>
        /// <param name="dbType">The type of the database.</param>
        /// <returns>The equivalent string name.</returns>
        public virtual string Resolve(DbType dbType) =>
            /*
https://docs.oracle.com/en/database/oracle/oracle-data-access-components/19.3.2/odpnt/featOraCommand.html
Table 3-8 OracleDbType Enumeration Values
Table 3-10 Inference of OracleDbType from DbType
            */
            dbType switch {
                DbType.Binary => "RAW",
                DbType.Boolean => "BOOLEAN",
                DbType.Byte => "NUMBER",
                // DbType.Currency => "NOT SUPPORTED",
                DbType.Date => "DATE",
                DbType.DateTime => "TIMESTAMP",
                DbType.DateTime2 => "TIMESTAMP",
                DbType.DateTimeOffset => "TIMESTAMP WITH TIME ZONE",
                DbType.Decimal => "NUMBER",
                DbType.Double => "NUMBER",
                DbType.Guid => "BLOB",
                DbType.Int16 => "NUMBER",
                DbType.Int32 => "NUMBER",
                DbType.Int64 => "NUMBER",
                // DbType.Object => "OBJECT", //Not Available in ODP.NET, Managed Driver and ODP.NET Core
                // DbType.Sbyte => "NOT SUPPORTED",
                DbType.Single => "NUMBER",
                DbType.AnsiString => "VARCHAR2",
                DbType.AnsiStringFixedLength => "CHAR",
                DbType.String => "NVARCHAR2",
                DbType.StringFixedLength => "NCHAR",
                DbType.Time => "INTERVAL DAY TO SECOND",
                // DbType.UInt16 => "NOT SUPPORTED",
                // DbType.UInt32 => "NOT SUPPORTED",
                // DbType.Uint64 => "NOT SUPPORTED",
                // DbType.VarNumeric => "NOT SUPPORTED",
                _ => throw new Exceptions.InvalidTypeException(dbType.ToString()),
            };
    }
}
