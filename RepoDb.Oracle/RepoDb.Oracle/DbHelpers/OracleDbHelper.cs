using Oracle.ManagedDataAccess.Client;
using RepoDb.Extensions;
using RepoDb.Interfaces;
using RepoDb.Resolvers;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace RepoDb.DbHelpers
{
    /// <summary>
    /// A helper class for database specially for the direct access. This class is only meant for Oracle.
    /// </summary>
    public sealed class OracleDbHelper : IDbHelper
    {
        private readonly IDbSetting m_dbSetting = DbSettingMapper.Get<OracleConnection>();

        /// <summary>
        /// Creates a new instance of <see cref="OracleDbHelper"/> class.
        /// </summary>
        public OracleDbHelper()
            : this(new OracleDbTypeNameToClientTypeResolver())
        { }

        /// <summary>
        /// Creates a new instance of <see cref="OracleDbHelper"/> class.
        /// </summary>
        /// <param name="dbTypeResolver">The type resolver to be used.</param>
        public OracleDbHelper(IResolver<string, Type> dbTypeResolver)
        {
            DbTypeResolver = dbTypeResolver;
        }

        #region Properties

        /// <summary>
        /// Gets the type resolver used by this <see cref="OracleDbHelper"/> instance.
        /// </summary>
        public IResolver<string, Type> DbTypeResolver { get; }

        #endregion

        #region Helpers

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private string GetCommandText() //TODO 11g!
        {
            return @"
                SELECT ATC.COLUMN_NAME
	                , CASE WHEN ACC.CONSTRAINT_TYPE = 'P' THEN 1 ELSE 0 END AS ""IsPrimary""
	                , CASE WHEN ATIC.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END AS ""IsIdentity""
	                , CASE WHEN ATC.NULLABLE = 'Y' THEN 1 ELSE 0 END AS ""IsNullable""
	                , ATC.DATA_TYPE AS ""DataType""
                FROM ALL_TAB_COLS ATC
                LEFT JOIN (
                    SELECT OWNER, TABLE_NAME, CONSTRAINT_NAME, CONSTRAINT_TYPE, COLUMN_NAME
                    FROM ALL_CONS_COLUMNS ACC
                    INNER JOIN ALL_CONSTRAINTS AC
                    USING (OWNER, TABLE_NAME, CONSTRAINT_NAME)
                    WHERE CONSTRAINT_TYPE = 'P'
                ) ACC
                ON ATC.OWNER = ACC.OWNER
                AND ATC.TABLE_NAME = ACC.TABLE_NAME
                AND ATC.COLUMN_NAME = ACC.COLUMN_NAME
                LEFT JOIN ALL_TAB_IDENTITY_COLS ATIC
                ON ATC.OWNER = ATIC.OWNER
                AND ATC.TABLE_NAME = ATIC.TABLE_NAME
                AND ATC.COLUMN_NAME = ATIC.COLUMN_NAME
                WHERE ATC.TABLE_NAME = :TableName
	              AND ATC.OWNER = NVL(:Schema, sys_context('USERENV', 'CURRENT_SCHEMA'))
                  AND HIDDEN_COLUMN != 'YES'
                ORDER BY COLUMN_ID";
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="reader"></param>
        /// <returns></returns>
        private DbField ReaderToDbField(DbDataReader reader)
        {
            return new DbField(reader.GetString(0),
                !reader.IsDBNull(1) && reader.GetInt32(1) == 1,
                !reader.IsDBNull(2) && reader.GetInt32(2) == 1,
                !reader.IsDBNull(3) && reader.GetInt32(3) == 1,
                reader.IsDBNull(4) ? DbTypeResolver.Resolve("text") : DbTypeResolver.Resolve(reader.GetString(4)),
                null,
                null,
                null,
                reader.IsDBNull(4) ? "text" : reader.GetString(4));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        private async Task<DbField> ReaderToDbFieldAsync(DbDataReader reader,
            CancellationToken cancellationToken = default)
        {
            return new DbField(await reader.GetFieldValueAsync<string>(0, cancellationToken),
                !await reader.IsDBNullAsync(1, cancellationToken) && 1 == await reader.GetFieldValueAsync<int>(1, cancellationToken),
                !await reader.IsDBNullAsync(2, cancellationToken) && 1 == await reader.GetFieldValueAsync<int>(2, cancellationToken),
                !await reader.IsDBNullAsync(3, cancellationToken) && 1 == await reader.GetFieldValueAsync<int>(3, cancellationToken),
                await reader.IsDBNullAsync(4, cancellationToken) ? DbTypeResolver.Resolve("text") : DbTypeResolver.Resolve(await reader.GetFieldValueAsync<string>(4, cancellationToken)),
                null,
                null,
                null,
                await reader.IsDBNullAsync(4, cancellationToken) ? "text" : reader.GetString(4));
        }

        #endregion

        #region Methods

        #region GetFields

        /// <summary>
        /// Gets the list of <see cref="DbField"/> of the table.
        /// </summary>
        /// <param name="connection">The instance of the connection object.</param>
        /// <param name="tableName">The name of the target table.</param>
        /// <param name="transaction">The transaction object that is currently in used.</param>
        /// <returns>A list of <see cref="DbField"/> of the target table.</returns>
        public IEnumerable<DbField> GetFields(IDbConnection connection,
            string tableName,
            IDbTransaction transaction = null)
        {
            // Variables
            var commandText = GetCommandText();
            var param = new
            {
                Schema = DataEntityExtension.GetSchema(tableName, m_dbSetting),
                TableName = DataEntityExtension.GetTableName(tableName, m_dbSetting)
            };

            // Iterate and extract
            using (var reader = (DbDataReader)connection.ExecuteReader(commandText, param, transaction: transaction))
            {
                var dbFields = new List<DbField>();

                // Iterate the list of the fields
                while (reader.Read())
                {
                    dbFields.Add(ReaderToDbField(reader));
                }

                // Return the list of fields
                return dbFields;
            }
        }

        /// <summary>
        /// Gets the list of <see cref="DbField"/> of the table in an asynchronous way.
        /// </summary>
        /// <param name="connection">The instance of the connection object.</param>
        /// <param name="tableName">The name of the target table.</param>
        /// <param name="transaction">The transaction object that is currently in used.</param>
        /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
        /// <returns>A list of <see cref="DbField"/> of the target table.</returns>
        public async Task<IEnumerable<DbField>> GetFieldsAsync(IDbConnection connection,
            string tableName,
            IDbTransaction transaction = null,
            CancellationToken cancellationToken = default)
        {
            // Variables
            var commandText = GetCommandText();
            var param = new
            {
                Schema = DataEntityExtension.GetSchema(tableName, m_dbSetting),
                TableName = DataEntityExtension.GetTableName(tableName, m_dbSetting)
            };

            // Iterate and extract
            using (var reader = (DbDataReader)await connection.ExecuteReaderAsync(commandText, param, transaction: transaction,
                cancellationToken: cancellationToken))
            {
                var dbFields = new List<DbField>();

                // Iterate the list of the fields
                while (await reader.ReadAsync(cancellationToken))
                {
                    dbFields.Add(await ReaderToDbFieldAsync(reader, cancellationToken));
                }

                // Return the list of fields
                return dbFields;
            }
        }

        #endregion

        #region GetScopeIdentity

        /// <summary>
        /// Gets the newly generated identity from the database.
        /// </summary>
        /// <param name="connection">The instance of the connection object.</param>
        /// <param name="transaction">The transaction object that is currently in used.</param>
        /// <returns>The newly generated identity from the database.</returns>
        public object GetScopeIdentity(IDbConnection connection,
            IDbTransaction transaction = null)
        {
            throw new Exceptions.IdentityFieldNotFoundException("Oracle doesn't support retrieving the last generated identity");
        }

        /// <summary>
        /// Gets the newly generated identity from the database in an asynchronous way.
        /// </summary>
        /// <param name="connection">The instance of the connection object.</param>
        /// <param name="transaction">The transaction object that is currently in used.</param>
        /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
        /// <returns>The newly generated identity from the database.</returns>
        public Task<object> GetScopeIdentityAsync(IDbConnection connection,
            IDbTransaction transaction = null,
            CancellationToken cancellationToken = default)
        {
            throw new Exceptions.IdentityFieldNotFoundException("Oracle doesn't support retrieving the last generated identity");
        }

        #endregion

        #endregion
    }
}
