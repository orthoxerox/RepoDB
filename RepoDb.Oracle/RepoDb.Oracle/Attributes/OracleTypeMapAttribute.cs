using Oracle.ManagedDataAccess.Client;
using System;

namespace RepoDb.Attributes
{
    /// <summary>
    /// An attribute used to define a mapping of .NET CLR <see cref="Type"/> into its equivalent <see cref="OracleDbType"/> value.
    /// </summary>
    public class OracleTypeMapAttribute : Attribute
    {
        /// <summary>
        /// Creates a new instance of <see cref="OracleTypeMapAttribute"/> class.
        /// </summary>
        /// <param name="dbType">A target <see cref="OracleDbType"/> value.</param>
        public OracleTypeMapAttribute(OracleDbType dbType)
        {
            DbType = dbType;
            ParameterType = typeof(OracleParameter);
        }

        /// <summary>
        /// Gets a <see cref="OracleDbType"/> that is currently mapped.
        /// </summary>
        public OracleDbType DbType { get; }

        /// <summary>
        /// Gets the represented <see cref="Type"/> of the <see cref="OracleParameter"/>.
        /// </summary>
        public Type ParameterType { get; }
    }
}
