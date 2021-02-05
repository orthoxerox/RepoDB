using Oracle.ManagedDataAccess.Client;
using RepoDb.DbHelpers;
using RepoDb.Oracle.DbSettings;
using RepoDb.StatementBuilders;

namespace RepoDb
{
    /// <summary>
    /// A class used to initialize necessary objects that is connected to <see cref="OracleConnection"/> object.
    /// </summary>
    public static class OracleBootstrap
    {
        #region Properties

        /// <summary>
        /// Gets the value indicating whether the initialization is completed.
        /// </summary>
        public static bool IsInitialized { get; private set; }

        #endregion

        #region Methods

        /// <summary>
        /// Initializes all necessary settings for Oracle.
        /// </summary>
        public static void Initialize() //TODO 11g
        {
            // Skip if already initialized
            if (IsInitialized == true)
            {
                return;
            }

            // Map the DbSetting
            DbSettingMapper.Add(typeof(OracleConnection), new OracleDbSetting(), true);

            // Map the DbHelper
            DbHelperMapper.Add(typeof(OracleConnection), new OracleDbHelper(), true);

            // Map the Statement Builder
            StatementBuilderMapper.Add(typeof(OracleConnection), new OracleStatementBuilder(), true);

            //Workaround to ensure specific ODP.NET defaults are set correctly
            //OracleInternal.Common.ConfigBaseClass.m_BindByName
            // makes parameters actually use their names, not position for binding
            //TODO m_InitialLOBFetchSize and m_InitialLONGFetchSize are not initialized correctly by ODP.NET

            var configClass = typeof(OracleConnection).Assembly.GetType("OracleInternal.Common.ConfigBaseClass");
            var bindByName = configClass.GetField("m_BindByName", System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic);
            bindByName.SetValue(null, true);

            // Set the flag
            IsInitialized = true;
        }

        #endregion
    }
}
