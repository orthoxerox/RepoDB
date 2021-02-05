using RepoDb.Interfaces;
using System;

namespace RepoDb.Resolvers
{
    /// <summary>
    /// A class used to resolve the Oracle Database Types into its equivalent .NET CLR Types.
    /// </summary>
    public class OracleDbTypeNameToClientTypeResolver : IResolver<string, Type>
    {
        /// <summary>
        /// Returns the equivalent .NET CLR Types of the Database Type.
        /// </summary>
        /// <param name="dbTypeName">The name of the database type.</param>
        /// <returns>The equivalent .NET CLR type.</returns>
        public virtual Type Resolve(string dbTypeName)
        {
            if (dbTypeName == null)
            {
                throw new NullReferenceException("The DB Type name must not be null.");
            }
            /*
https://docs.oracle.com/en/database/oracle/oracle-data-access-components/19.3.2/odpnt/featTypes.html
            */
            return (dbTypeName.ToUpperInvariant()) switch
            {
                "BFILE" => typeof(System.Byte[]),
                "BINARY_DOUBLE" => typeof(System.Decimal),
                "BINARY_FLOAT" => typeof(System.Decimal),
                "BINARY_INTEGER" => typeof(System.Decimal),
                "BLOB" => typeof(System.Byte[]),
                "BOOLEAN" => typeof(System.Boolean),
                "CHAR" => typeof(System.String),
                "CLOB" => typeof(System.String),
                "DATE" => typeof(System.DateTime),
                "INTERVAL DAY TO SECOND" => typeof(System.TimeSpan),
                "INTERVAL YEAR TO MONTH" => typeof(System.Int64),
                "JSON" => typeof(System.String),
                "LONG" => typeof(System.String),
                "LONG RAW" => typeof(System.Byte[]),
                "NCHAR" => typeof(System.String),
                "NCLOB" => typeof(System.String),
                "NUMBER" => typeof(System.Decimal),
                "NVARCHAR2" => typeof(System.String),
                "PLS_INTEGER" => typeof(System.Decimal),
                "RAW" => typeof(System.Byte[]),
                "REF" => typeof(System.String),
                // "REF CURSOR (PL/SQL only)" => typeof(Not Applicable),
                "ROWID" => typeof(System.String),
                "TIMESTAMP" => typeof(System.DateTime),
                "TIMESTAMP WITH LOCAL TIME ZONE" => typeof(System.DateTime),
                "TIMESTAMP WITH TIME ZONE" => typeof(System.DateTimeOffset),
                "UROWID" => typeof(System.String),
                "VARCHAR2" => typeof(System.String),
                "XMLTYPE" => typeof(System.String),
                _ => typeof(object),
            };
        }
    }
}
