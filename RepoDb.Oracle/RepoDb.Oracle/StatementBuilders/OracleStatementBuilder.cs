using Oracle.ManagedDataAccess.Client;
using RepoDb.Exceptions;
using RepoDb.Extensions;
using RepoDb.Interfaces;
using RepoDb.Resolvers;
using System;
using System.Collections.Generic;
using System.Linq;

namespace RepoDb.StatementBuilders
{
    /// <summary>
    /// A class used to build a SQL Statement for Oracle.
    /// </summary>
    public sealed class OracleStatementBuilder : BaseStatementBuilder
    {
        /// <summary>
        /// Creates a new instance of <see cref="OracleStatementBuilder"/> object.
        /// </summary>
        public OracleStatementBuilder()
            : this(DbSettingMapper.Get(typeof(OracleConnection)),
                  new OracleConvertFieldResolver(),
                  new ClientTypeToAverageableClientTypeResolver())
        { }

        /// <summary>
        /// Creates a new instance of <see cref="OracleStatementBuilder"/> class.
        /// </summary>
        /// <param name="dbSetting">The database settings object currently in used.</param>
        /// <param name="convertFieldResolver">The resolver used when converting a field in the database layer.</param>
        /// <param name="averageableClientTypeResolver">The resolver used to identity the type for average.</param>
        public OracleStatementBuilder(IDbSetting dbSetting,
            IResolver<Field, IDbSetting, string> convertFieldResolver = null,
            IResolver<Type, Type> averageableClientTypeResolver = null)
            : base(dbSetting,
                  convertFieldResolver,
                  averageableClientTypeResolver)
        { }

        #region "Trivial syntax changes only"
        //These implementations must be kept in sync with the ones in BaseStatementBuilder
        //The only difference is that Oracle expects hints after the first keyword and doesn't need a semicolon

        #region CreateAverage

        /// <inheritdoc/>
        /// <returns>A sql statement for average operation.</returns>
        public override string CreateAverage(QueryBuilder queryBuilder,
            string tableName,
            Field field,
            QueryGroup where = null,
            string hints = null)
        {
            // Ensure with guards
            GuardTableName(tableName);

            // Validate the hints
            GuardHints(hints);

            // Check the field
            if (field == null)
            {
                throw new NullReferenceException("The field cannot be null.");
            }
            else
            {
                field.Type = AverageableClientTypeResolver?.Resolve(field.Type ?? DbSetting.AverageableType);
            }

            // Initialize the builder
            var builder = queryBuilder ?? new QueryBuilder();

            // Build the query
            builder.Clear()
                .Select()
                .HintsFrom(hints)
                .Average(field, DbSetting, ConvertFieldResolver)
                .WriteText($"AS {"AverageValue".AsQuoted(DbSetting)}")
                .From()
                .TableNameFrom(tableName, DbSetting)
                .WhereFrom(where, DbSetting);

            // Return the query
            return builder.GetString();
        }

        #endregion

        #region CreateAverageAll

        /// <inheritdoc/>
        public override string CreateAverageAll(QueryBuilder queryBuilder,
            string tableName,
            Field field,
            string hints = null)
        {
            // Ensure with guards
            GuardTableName(tableName);

            // Validate the hints
            GuardHints(hints);

            // Check the field
            if (field == null)
            {
                throw new NullReferenceException("The field cannot be null.");
            }
            else
            {
                field.Type = AverageableClientTypeResolver?.Resolve(field.Type ?? DbSetting.AverageableType);
            }

            // Initialize the builder
            var builder = queryBuilder ?? new QueryBuilder();

            // Build the query
            builder.Clear()
                .Select()
                .HintsFrom(hints)
                .Average(field, DbSetting, ConvertFieldResolver)
                .WriteText($"AS {"AverageValue".AsQuoted(DbSetting)}")
                .From()
                .TableNameFrom(tableName, DbSetting);

            // Return the query
            return builder.GetString();
        }

        #endregion

        #region CreateCount

        /// <inheritdoc/>
        public override string CreateCount(QueryBuilder queryBuilder,
            string tableName,
            QueryGroup where = null,
            string hints = null)
        {
            // Ensure with guards
            GuardTableName(tableName);

            // Validate the hints
            GuardHints(hints);

            // Initialize the builder
            var builder = queryBuilder ?? new QueryBuilder();

            // Build the query
            builder.Clear()
                .Select()
                .HintsFrom(hints)
                .Count(null, DbSetting)
                .WriteText($"AS {"CountValue".AsQuoted(DbSetting)}")
                .From()
                .TableNameFrom(tableName, DbSetting)
                .WhereFrom(where, DbSetting);

            // Return the query
            return builder.GetString();
        }

        #endregion

        #region CreateCountAll

        /// <inheritdoc/>
        public override string CreateCountAll(QueryBuilder queryBuilder,
            string tableName,
            string hints = null)
        {
            // Ensure with guards
            GuardTableName(tableName);

            // Validate the hints
            GuardHints(hints);

            // Initialize the builder
            var builder = queryBuilder ?? new QueryBuilder();

            // Build the query
            builder.Clear()
                .Select()
                .HintsFrom(hints)
                .Count(null, DbSetting)
                .WriteText($"AS {"CountValue".AsQuoted(DbSetting)}")
                .From()
                .TableNameFrom(tableName, DbSetting);

            // Return the query
            return builder.GetString();
        }

        #endregion

        #region CreateMax

        /// <inheritdoc/>
        public override string CreateMax(QueryBuilder queryBuilder,
            string tableName,
            Field field,
            QueryGroup where = null,
            string hints = null)
        {
            // Ensure with guards
            GuardTableName(tableName);

            // Validate the hints
            GuardHints(hints);

            // Check the field
            if (field == null)
            {
                throw new NullReferenceException("The field cannot be null.");
            }

            // Initialize the builder
            var builder = queryBuilder ?? new QueryBuilder();

            // Build the query
            builder.Clear()
                .Select()
                .HintsFrom(hints)
                .Max(field, DbSetting)
                .WriteText($"AS {"MaxValue".AsQuoted(DbSetting)}")
                .From()
                .TableNameFrom(tableName, DbSetting)
                .WhereFrom(where, DbSetting);

            // Return the query
            return builder.GetString();
        }

        #endregion

        #region CreateMaxAll

        /// <inheritdoc/>
        public override string CreateMaxAll(QueryBuilder queryBuilder,
            string tableName,
            Field field,
            string hints = null)
        {
            // Ensure with guards
            GuardTableName(tableName);

            // Validate the hints
            GuardHints(hints);

            // Check the field
            if (field == null)
            {
                throw new NullReferenceException("The field cannot be null.");
            }

            // Initialize the builder
            var builder = queryBuilder ?? new QueryBuilder();

            // Build the query
            builder.Clear()
                .Select()
                .HintsFrom(hints)
                .Max(field, DbSetting)
                .WriteText($"AS {"MaxValue".AsQuoted(DbSetting)}")
                .From()
                .TableNameFrom(tableName, DbSetting);

            // Return the query
            return builder.GetString();
        }

        #endregion

        #region CreateMin

        /// <inheritdoc/>
        public override string CreateMin(QueryBuilder queryBuilder,
            string tableName,
            Field field,
            QueryGroup where = null,
            string hints = null)
        {
            // Ensure with guards
            GuardTableName(tableName);

            // Validate the hints
            GuardHints(hints);

            // Check the field
            if (field == null)
            {
                throw new NullReferenceException("The field cannot be null.");
            }

            // Initialize the builder
            var builder = queryBuilder ?? new QueryBuilder();

            // Build the query
            builder.Clear()
                .Select()
                .HintsFrom(hints)
                .Min(field, DbSetting)
                .WriteText($"AS {"MinValue".AsQuoted(DbSetting)}")
                .From()
                .TableNameFrom(tableName, DbSetting)
                .WhereFrom(where, DbSetting);

            // Return the query
            return builder.GetString();
        }

        #endregion

        #region CreateMinAll

        /// <inheritdoc/>
        public override string CreateMinAll(QueryBuilder queryBuilder,
            string tableName,
            Field field,
            string hints = null)
        {
            // Ensure with guards
            GuardTableName(tableName);

            // Validate the hints
            GuardHints(hints);

            // Check the field
            if (field == null)
            {
                throw new NullReferenceException("The field cannot be null.");
            }

            // Initialize the builder
            var builder = queryBuilder ?? new QueryBuilder();

            // Build the query
            builder.Clear()
                .Select()
                .HintsFrom(hints)
                .Min(field, DbSetting)
                .WriteText($"AS {"MinValue".AsQuoted(DbSetting)}")
                .From()
                .TableNameFrom(tableName, DbSetting);

            // Return the query
            return builder.GetString();
        }

        #endregion

        #region CreateSum

        /// <inheritdoc/>
        public override string CreateSum(QueryBuilder queryBuilder,
            string tableName,
            Field field,
            QueryGroup where = null,
            string hints = null)
        {
            // Ensure with guards
            GuardTableName(tableName);

            // Validate the hints
            GuardHints(hints);

            // Check the field
            if (field == null)
            {
                throw new NullReferenceException("The field cannot be null.");
            }

            // Initialize the builder
            var builder = queryBuilder ?? new QueryBuilder();

            // Build the query
            builder.Clear()
                .Select()
                .HintsFrom(hints)
                .Sum(field, DbSetting)
                .WriteText($"AS {"SumValue".AsQuoted(DbSetting)}")
                .From()
                .TableNameFrom(tableName, DbSetting)
                .WhereFrom(where, DbSetting);

            // Return the query
            return builder.GetString();
        }

        #endregion

        #region CreateSumAll

        /// <inheritdoc/>
        public override string CreateSumAll(QueryBuilder queryBuilder,
            string tableName,
            Field field,
            string hints = null)
        {
            // Ensure with guards
            GuardTableName(tableName);

            // Validate the hints
            GuardHints(hints);

            // Check the field
            if (field == null)
            {
                throw new NullReferenceException("The field cannot be null.");
            }

            // Initialize the builder
            var builder = queryBuilder ?? new QueryBuilder();

            // Build the query
            builder.Clear()
                .Select()
                .HintsFrom(hints)
                .Sum(field, DbSetting)
                .WriteText($"AS {"SumValue".AsQuoted(DbSetting)}")
                .From()
                .TableNameFrom(tableName, DbSetting);

            // Return the query
            return builder.GetString();
        }

        #endregion

        #region CreateQueryAll

        /// <inheritdoc/>
        public override string CreateQueryAll(QueryBuilder queryBuilder,
            string tableName,
            IEnumerable<Field> fields,
            IEnumerable<OrderField> orderBy = null,
            string hints = null)
        {
            // Guard the target table
            GuardTableName(tableName);

            // Validate the hints
            GuardHints(hints);

            // There should be fields
            if (fields?.Any() != true)
            {
                throw new NullReferenceException($"The list of queryable fields must not be null for '{tableName}'.");
            }

            // Validate the ordering
            if (orderBy != null)
            {
                // Check if the order fields are present in the given fields
                var unmatchesOrderFields = orderBy?.Where(orderField =>
                    fields.FirstOrDefault(f =>
                        string.Equals(orderField.Name, f.Name, StringComparison.OrdinalIgnoreCase)) == null);

                // Throw an error we found any unmatches
                if (unmatchesOrderFields.Any() == true)
                {
                    throw new MissingFieldsException($"The order fields '{unmatchesOrderFields.Select(field => field.AsField(DbSetting)).Join(", ")}' are not " +
                        $"present at the given fields '{fields.Select(field => field.Name).Join(", ")}'.");
                }
            }

            // Initialize the builder
            var builder = queryBuilder ?? new QueryBuilder();

            // Build the query
            builder.Clear()
                .Select()
                .HintsFrom(hints)
                .FieldsFrom(fields, DbSetting)
                .From()
                .TableNameFrom(tableName, DbSetting)
                .OrderByFrom(orderBy, DbSetting);

            // Return the query
            return builder.GetString();
        }

        #endregion

        #region CreateDelete

        /// <inheritdoc/>
        public override string CreateDelete(QueryBuilder queryBuilder,
            string tableName,
            QueryGroup where = null,
            string hints = null)
        {
            // Ensure with guards
            GuardTableName(tableName);

            // Validate the hints
            GuardHints(hints);

            // Initialize the builder
            var builder = queryBuilder ?? new QueryBuilder();

            // Build the query
            builder.Clear()
                .Delete()
                .HintsFrom(hints)
                .From()
                .TableNameFrom(tableName, DbSetting)
                .WhereFrom(where, DbSetting);

            // Return the query
            return builder.GetString();
        }

        #endregion

        #region CreateDeleteAll

        /// <inheritdoc/>
        public override string CreateDeleteAll(QueryBuilder queryBuilder,
            string tableName,
            string hints = null)
        {
            // Ensure with guards
            GuardTableName(tableName);

            // Validate the hints
            GuardHints(hints);

            // Initialize the builder
            var builder = queryBuilder ?? new QueryBuilder();

            // Build the query
            builder.Clear()
                .Delete()
                .HintsFrom(hints)
                .From()
                .TableNameFrom(tableName, DbSetting);

            // Return the query
            return builder.GetString();
        }

        #endregion

        #region CreateTruncate

        /// <inheritdoc/>
        public override string CreateTruncate(QueryBuilder queryBuilder,
            string tableName)
        {
            // Guard the target table
            GuardTableName(tableName);

            // Initialize the builder
            var builder = queryBuilder ?? new QueryBuilder();

            // Build the query
            builder.Clear()
                .Truncate()
                .Table()
                .TableNameFrom(tableName, DbSetting);

            // Return the query
            return builder.GetString();
        }

        #endregion

        #region CreateUpdate

        /// <inheritdoc/>
        public override string CreateUpdate(QueryBuilder queryBuilder,
            string tableName,
            IEnumerable<Field> fields,
            QueryGroup where = null,
            DbField primaryField = null,
            DbField identityField = null,
            string hints = null)
        {
            // Ensure with guards
            GuardTableName(tableName);
            GuardHints(hints);
            GuardPrimary(primaryField);
            GuardIdentity(identityField);

            // Gets the updatable fields
            var updatableFields = fields
                .Where(f => !string.Equals(f.Name, primaryField?.Name, StringComparison.OrdinalIgnoreCase) &&
                    !string.Equals(f.Name, identityField?.Name, StringComparison.OrdinalIgnoreCase));

            // Check if there are updatable fields
            if (updatableFields.Any() != true)
            {
                throw new EmptyException("The list of updatable fields cannot be null or empty.");
            }

            // Initialize the builder
            var builder = queryBuilder ?? new QueryBuilder();

            // Build the query
            builder.Clear()
                .Update()
                .HintsFrom(hints)
                .TableNameFrom(tableName, DbSetting)
                .Set()
                .FieldsAndParametersFrom(updatableFields, 0, DbSetting)
                .WhereFrom(where, DbSetting);

            // Return the query
            return builder.GetString();
        }

        #endregion

        #region CreateUpdateAll

        /// <inheritdoc/>
        public override string CreateUpdateAll(QueryBuilder queryBuilder,
            string tableName,
            IEnumerable<Field> fields,
            IEnumerable<Field> qualifiers,
            int batchSize = Constant.DefaultBatchOperationSize,
            DbField primaryField = null,
            DbField identityField = null,
            string hints = null)
        {
            // Ensure with guards
            GuardTableName(tableName);
            GuardHints(hints);
            GuardPrimary(primaryField);
            GuardIdentity(identityField);

            // Validate the multiple statement execution
            ValidateMultipleStatementExecution(batchSize);

            // Ensure the fields
            if (fields?.Any() != true)
            {
                throw new EmptyException($"The list of fields cannot be null or empty.");
            }

            // Check the qualifiers
            if (qualifiers?.Any() == true)
            {
                // Check if the qualifiers are present in the given fields
                var unmatchesQualifiers = qualifiers.Where(field =>
                    fields?.FirstOrDefault(f =>
                        string.Equals(field.Name, f.Name, StringComparison.OrdinalIgnoreCase)) == null);

                // Throw an error we found any unmatches
                if (unmatchesQualifiers.Any() == true)
                {
                    throw new InvalidQualifiersException($"The qualifiers '{unmatchesQualifiers.Select(field => field.Name).Join(", ")}' are not " +
                        $"present at the given fields '{fields.Select(field => field.Name).Join(", ")}'.");
                }
            }
            else
            {
                if (primaryField != null)
                {
                    // Make sure that primary is present in the list of fields before qualifying to become a qualifier
                    var isPresent = fields.FirstOrDefault(f =>
                        string.Equals(f.Name, primaryField.Name, StringComparison.OrdinalIgnoreCase)) != null;

                    // Throw if not present
                    if (isPresent == false)
                    {
                        throw new InvalidQualifiersException($"There are no qualifier field objects found for '{tableName}'. Ensure that the " +
                            $"primary field is present at the given fields '{fields.Select(field => field.Name).Join(", ")}'.");
                    }

                    // The primary is present, use it as a default if there are no qualifiers given
                    qualifiers = primaryField.AsField().AsEnumerable();
                }
                else
                {
                    // Throw exception, qualifiers are not defined
                    throw new NullReferenceException($"There are no qualifier field objects found for '{tableName}'.");
                }
            }

            // Gets the updatable fields
            fields = fields
                .Where(f => !string.Equals(f.Name, primaryField?.Name, StringComparison.OrdinalIgnoreCase) &&
                    !string.Equals(f.Name, identityField?.Name, StringComparison.OrdinalIgnoreCase) &&
                    qualifiers.FirstOrDefault(q => string.Equals(q.Name, f.Name, StringComparison.OrdinalIgnoreCase)) == null);

            // Check if there are updatable fields
            if (fields.Any() != true)
            {
                throw new EmptyException("The list of updatable fields cannot be null or empty.");
            }

            // Initialize the builder
            var builder = queryBuilder ?? new QueryBuilder();

            // Build the query
            builder.Clear();

            // Iterate the indexes
            for (var index = 0; index < batchSize; index++)
            {
                builder
                    .Update()
                    .HintsFrom(hints)
                    .TableNameFrom(tableName, DbSetting)
                    .Set()
                    .FieldsAndParametersFrom(fields, index, DbSetting)
                    .WhereFrom(qualifiers, index, DbSetting);
            }

            // Return the query
            return builder.GetString();
        }

        #endregion

        #endregion

        #region Limit/Offset
        //These methods differ only in the way Oracle implements limiting and skipping rows
        //TODO 11g

        #region CreateBatchQuery

        /// <inheritdoc/>
        public override string CreateBatchQuery(QueryBuilder queryBuilder,
            string tableName,
            IEnumerable<Field> fields,
            int page,
            int rowsPerBatch,
            IEnumerable<OrderField> orderBy = null,
            QueryGroup where = null,
            string hints = null)
        {
            // Ensure with guards
            GuardTableName(tableName);

            // Validate the hints
            GuardHints(hints);

            // There should be fields
            if (fields?.Any() != true)
            {
                throw new NullReferenceException($"The list of queryable fields must not be null for '{tableName}'.");
            }

            // Validate order by
            if (orderBy == null || orderBy.Any() != true)
            {
                throw new EmptyException("The argument 'orderBy' is required.");
            }

            // Validate the page
            if (page < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(page), "The page must be equals or greater than 0.");
            }

            // Validate the page
            if (rowsPerBatch < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(rowsPerBatch), "The rows per batch must be equals or greater than 1.");
            }

            // Skipping variables
            var skip = (page * rowsPerBatch);

            // Initialize the builder
            var builder = queryBuilder ?? new QueryBuilder();

            // Build the query
            builder.Clear()
                .Select()
                .HintsFrom(hints)
                .FieldsFrom(fields, DbSetting)
                .From()
                .TableNameFrom(tableName, DbSetting)
                .WhereFrom(where, DbSetting)
                .OrderByFrom(orderBy, DbSetting);

            if (skip == 0)
            {
                builder
                    .WriteText("FETCH FIRST")
                    .WriteText(rowsPerBatch.ToString())
                    .WriteText("ROWS ONLY");

            }
            else
            {
                builder
                    .Offset(skip)
                    .WriteText("ROWS FETCH NEXT")
                    .WriteText(rowsPerBatch.ToString())
                    .WriteText("ROWS ONLY");
            }

            // Return the query
            return builder.GetString();
        }

        #endregion

        #region CreateExists

        /// <inheritdoc/>
        public override string CreateExists(QueryBuilder queryBuilder,
            string tableName,
            QueryGroup where = null,
            string hints = null)
        {
            // Ensure with guards
            GuardTableName(tableName);

            // Validate the hints
            GuardHints(hints);

            // Initialize the builder
            var builder = queryBuilder ?? new QueryBuilder();

            // Build the query
            builder.Clear()
                .Select()
                .HintsFrom(hints)
                .WriteText("1 AS \"ExistsValue\"")
                .From()
                .TableNameFrom(tableName, DbSetting)
                .WhereFrom(where, DbSetting)
                .WriteText("FETCH FIRST 1 ROW ONLY");

            // Return the query
            return builder.GetString();
        }

        #endregion

        #region CreateQuery

        /// <inheritdoc/>
        public override string CreateQuery(QueryBuilder queryBuilder,
            string tableName,
            IEnumerable<Field> fields,
            QueryGroup where = null,
            IEnumerable<OrderField> orderBy = null,
            int? top = null,
            string hints = null)
        {
            // Ensure with guards
            GuardTableName(tableName);

            // Validate the hints
            GuardHints(hints);

            // There should be fields
            if (fields?.Any() != true)
            {
                throw new NullReferenceException($"The list of queryable fields must not be null for '{tableName}'.");
            }

            // Validate the ordering
            if (orderBy != null)
            {
                // Check if the order fields are present in the given fields
                var unmatchesOrderFields = orderBy.Where(orderField =>
                    fields.FirstOrDefault(f =>
                        string.Equals(orderField.Name, f.Name, StringComparison.OrdinalIgnoreCase)) == null);

                // Throw an error we found any unmatches
                if (unmatchesOrderFields.Any() == true)
                {
                    throw new MissingFieldsException($"The order fields '{unmatchesOrderFields.Select(field => field.Name).Join(", ")}' are not " +
                        $"present at the given fields '{fields.Select(field => field.Name).Join(", ")}'.");
                }
            }

            // Initialize the builder
            var builder = queryBuilder ?? new QueryBuilder();

            // Build the query
            builder.Clear()
                .Select()
                .HintsFrom(hints)
                .FieldsFrom(fields, DbSetting)
                .From()
                .TableNameFrom(tableName, DbSetting)
                .WhereFrom(where, DbSetting)
                .OrderByFrom(orderBy, DbSetting);
            if (top > 0)
            {
                builder
                    .WriteText("FETCH FIRST")
                    .WriteText(top.ToString())
                    .WriteText("ROWS ONLY");
            }

            // Return the query
            return builder.GetString();
        }

        #endregion

        #endregion

        #region CreateInsert

        /// <inheritdoc/>
        public override string CreateInsert(QueryBuilder queryBuilder,
            string tableName,
            IEnumerable<Field> fields = null,
            DbField primaryField = null,
            DbField identityField = null,
            string hints = null)
        {
            // Initialize the builder
            var builder = queryBuilder ?? new QueryBuilder();

            // Call the base for the checks
            base.CreateInsert(builder,
                tableName,
                fields,
                primaryField,
                identityField,
                hints);

            // Variables needed
            var databaseType = (string)null;

            // Check for the identity
            if (identityField != null)
            {
                var dbType = new ClientTypeToDbTypeResolver().Resolve(identityField.Type);
                if (dbType != null)
                {
                    databaseType = new DbTypeToOracleStringNameResolver().Resolve(dbType.Value);
                }
            }

            var insertableFields = fields
                .Where(f => !string.Equals(f.Name, identityField?.Name, StringComparison.OrdinalIgnoreCase));

            // Set the return value
            var result = identityField != null ?
                string.IsNullOrEmpty(databaseType) ?
                    identityField.Name.AsQuoted(DbSetting) :
                        $"CAST({identityField.Name.AsQuoted(DbSetting)} AS {databaseType})" :
                            primaryField != null ? primaryField.Name.AsQuoted(DbSetting) : "NULL";

            // Build the query
            builder.Clear()
                .Insert()
                .HintsFrom(hints)
                .Into()
                .TableNameFrom(tableName, DbSetting)
                .OpenParen()
                .FieldsFrom(insertableFields, DbSetting)
                .CloseParen()
                .Values()
                .OpenParen()
                .ParametersFrom(insertableFields, 0, DbSetting)
                .CloseParen()
                .Returning()
                .WriteText(result)
                .Into()
                .WriteText(":Result");

            // Return the query
            return builder.GetString();
        }

        #endregion

        #region CreateInsertAll

        /// <inheritdoc/>
        public override string CreateInsertAll(QueryBuilder queryBuilder,
            string tableName,
            IEnumerable<Field> fields = null,
            int batchSize = 1,
            DbField primaryField = null,
            DbField identityField = null,
            string hints = null)
        {
            // Initialize the builder
            var builder = queryBuilder ?? new QueryBuilder();

            // Call the base for the checks
            base.CreateInsertAll(builder,
                tableName,
                fields,
                batchSize,
                primaryField,
                identityField,
                hints);

            // Variables needed
            var insertableFields = fields
                .Where(f => !string.Equals(f.Name, identityField?.Name, StringComparison.OrdinalIgnoreCase));

            // Build the query
            builder.Clear()
                .WriteText("BEGIN");

            // Iterate the indexes
            for (var index = 0; index < batchSize; index++)
            {
                builder.Insert()
                    .HintsFrom(hints)
                    .Into()
                    .TableNameFrom(tableName, DbSetting)
                    .OpenParen()
                    .FieldsFrom(insertableFields, DbSetting)
                    .CloseParen()
                    .Values()
                    .OpenParen()
                    .ParametersFrom(insertableFields, index, DbSetting)
                    .CloseParen()
                    .End();
            }

            builder.WriteText("END")
                .End();


            // Return the query
            return builder.GetString();
        }

        #endregion

        #region CreateMerge

        /// <inheritdoc/>
        public override string CreateMerge(QueryBuilder queryBuilder,
            string tableName,
            IEnumerable<Field> fields,
            IEnumerable<Field> qualifiers = null,
            DbField primaryField = null,
            DbField identityField = null,
            string hints = null)
        {
            // Ensure with guards
            GuardTableName(tableName);
            GuardHints(hints);
            GuardPrimary(primaryField);
            GuardIdentity(identityField);

            // Verify the fields
            if (fields?.Any() != true)
            {
                throw new NullReferenceException($"The list of fields cannot be null or empty.");
            }

            // Set the qualifiers
            if (qualifiers?.Any() != true)
            {
                if (primaryField is null)
                {
                    throw new PrimaryFieldNotFoundException("The list of qualifiers is null or empty and no primary field is given");
                }

                qualifiers = primaryField.AsField().AsEnumerable();
            }

            // Initialize the builder
            var builder = queryBuilder ?? new QueryBuilder();

            // Remove the qualifiers from the fields
            var updatableFields = fields
                .Where(f =>
                    qualifiers?.Any(qf => string.Equals(qf.Name, f.Name, StringComparison.OrdinalIgnoreCase)) != true)
                .AsList();
            // Remove the identity from the fields
            var insertableFields = fields
                .Where(f => string.Equals(f.Name, identityField?.Name, StringComparison.OrdinalIgnoreCase) != true)
                .AsList();

            // Build the query
            builder.Clear()
                .Merge()
                .HintsFrom(hints)
                .Into()
                .TableNameFrom(tableName, DbSetting)
                //Source query
                .Using()
                .OpenParen()
                    .Select()
                    .ParametersAsFieldsFrom(fields, 0, DbSetting)
                    .From()
                    .TableNameFrom("DUAL", DbSetting)
                .CloseParen()
                .WriteText("_SOURCE".AsQuoted(DbSetting))
                //Condition
                .On()
                .OpenParen()
                .WriteText(string.Join(" AND ", qualifiers.Select(field => field.AsJoinQualifier(tableName.AsQuoted(DbSetting), "_SOURCE".AsQuoted(DbSetting), DbSetting))))
                .CloseParen()
                //Matched
                .When().Matched().Then().Update().Set()
                .FieldsAndAliasFieldsFrom(updatableFields, tableName.AsQuoted(DbSetting), "_SOURCE".AsQuoted(DbSetting), DbSetting)
                //NotMatched
                .When().Not().Matched().Then().Insert()
                .OpenParen()
                .FieldsFrom(insertableFields, DbSetting)
                .CloseParen()
                .Values()
                .OpenParen()
                .AsAliasFieldsFrom(insertableFields, "_SOURCE".AsQuoted(DbSetting), DbSetting)
                .CloseParen();

            // Return the query
            return builder.GetString();
        }

        #endregion

        #region CreateMergeAll

        /// <inheritdoc/>
        public override string CreateMergeAll(QueryBuilder queryBuilder,
            string tableName,
            IEnumerable<Field> fields,
            IEnumerable<Field> qualifiers,
            int batchSize = 10,
            DbField primaryField = null,
            DbField identityField = null,
            string hints = null)
        {
            // Ensure with guards
            GuardTableName(tableName);
            GuardHints(hints);
            GuardPrimary(primaryField);
            GuardIdentity(identityField);

            // Verify the fields
            if (fields?.Any() != true)
            {
                throw new NullReferenceException($"The list of fields cannot be null or empty.");
            }

            // Set the qualifiers
            if (qualifiers?.Any() != true)
            {
                if (primaryField is null)
                {
                    throw new PrimaryFieldNotFoundException("The list of qualifiers is null or empty and no primary field is given");
                }

                qualifiers = primaryField.AsField().AsEnumerable();
            }

            // Initialize the builder
            var builder = queryBuilder ?? new QueryBuilder();

            // Remove the qualifiers from the fields
            var updatableFields = fields
                .Where(f =>
                    qualifiers?.Any(qf => string.Equals(qf.Name, f.Name, StringComparison.OrdinalIgnoreCase)) != true)
                .AsList();
            // Remove the identity from the fields
            var insertableFields = fields
                .Where(f => string.Equals(f.Name, identityField?.Name, StringComparison.OrdinalIgnoreCase) != true)
                .AsList();

            // Build the query
            builder.Clear()
                .Merge()
                .HintsFrom(hints)
                .Into()
                .TableNameFrom(tableName, DbSetting)
                //Source query
                .Using()
                .OpenParen();

            for (var index = 0; index < batchSize; index++)
            {
                if (index > 0)
                {
                    _ = builder.WriteText("UNION ALL");
                }

                builder
                    .Select()
                    .ParametersAsFieldsFrom(fields, index, DbSetting)
                    .From()
                    .TableNameFrom("DUAL", DbSetting);
            }

            builder
                .CloseParen()
                .WriteText("_SOURCE".AsQuoted(DbSetting))
                //Condition
                .On()
                .OpenParen()
                .WriteText(string.Join(" AND ", qualifiers.Select(field => field.AsJoinQualifier(tableName.AsQuoted(DbSetting), "_SOURCE".AsQuoted(DbSetting), DbSetting))))
                .CloseParen()
                //Matched
                .When().Matched().Then().Update().Set()
                .FieldsAndAliasFieldsFrom(updatableFields, tableName.AsQuoted(DbSetting), "_SOURCE".AsQuoted(DbSetting), DbSetting)
                //NotMatched
                .When().Not().Matched().Then().Insert()
                .OpenParen()
                .FieldsFrom(insertableFields, DbSetting)
                .CloseParen()
                .Values()
                .OpenParen()
                .AsAliasFieldsFrom(insertableFields, "_SOURCE".AsQuoted(DbSetting), DbSetting)
                .CloseParen();

            // Return the query
            return builder.GetString();
        }

        #endregion


    }
}
