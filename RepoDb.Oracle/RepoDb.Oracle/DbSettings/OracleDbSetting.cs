using Oracle.ManagedDataAccess.Client;
using RepoDb.DbSettings;

namespace RepoDb.Oracle.DbSettings
{
    /// <summary>
    /// A setting class used for <see cref="OracleConnection"/> data provider.
    /// </summary>
    public sealed class OracleDbSetting : BaseDbSetting
    {
        /// <summary>
        /// Creates a new instance of <see cref="OracleDbSetting"/> class.
        /// </summary>
        public OracleDbSetting()
        {
            AreTableHintsSupported = true;
            AverageableType = typeof(double);
            ClosingQuote = "\"";
            DefaultSchema = null;
            IsDirectionSupported = true;
            IsExecuteReaderDisposable = true;
            IsMultiStatementExecutable = true;
            IsPreparable = true;
            IsUseUpsert = false;
            OpeningQuote = "\"";
            ParameterPrefix = ":";
        }
    }
}
