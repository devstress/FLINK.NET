using System;
using System.Collections.Generic;
using System.Linq;
using FlinkDotNet.Core.Api;
using Microsoft.Extensions.Logging;

namespace FlinkDotNet.Table.Api
{
    /// <summary>
    /// Table environment that manages table operations and SQL execution
    /// </summary>
    public interface ITableEnvironment
    {
        /// <summary>
        /// Creates a table from a data stream
        /// </summary>
        ITable FromDataStream<T>(DataStream<T> dataStream, params string[] fieldNames);

        /// <summary>
        /// Registers a table with a name for SQL queries
        /// </summary>
        void RegisterTable(string name, ITable table);

        /// <summary>
        /// Executes a SQL query and returns the result as a table
        /// </summary>
        ITable SqlQuery(string query);

        /// <summary>
        /// Gets a registered table by name
        /// </summary>
        ITable GetTable(string name);

        /// <summary>
        /// Lists all registered table names
        /// </summary>
        string[] ListTables();
    }

    /// <summary>
    /// Default implementation of the table environment
    /// </summary>
    public class TableEnvironment : ITableEnvironment
    {
        private readonly StreamExecutionEnvironment _streamEnv;
        private readonly ILogger<TableEnvironment>? _logger;
        private readonly Dictionary<string, ITable> _registeredTables;
        private readonly ISqlParser _sqlParser;

        public TableEnvironment(StreamExecutionEnvironment streamEnv, ILogger<TableEnvironment>? logger = null)
        {
            _streamEnv = streamEnv ?? throw new ArgumentNullException(nameof(streamEnv));
            _logger = logger;
            _registeredTables = new Dictionary<string, ITable>();
            _sqlParser = new BasicSqlParser();
        }

        public ITable FromDataStream<T>(DataStream<T> dataStream, params string[] fieldNames)
        {
            if (dataStream == null)
                throw new ArgumentNullException(nameof(dataStream));

            // Infer schema from type T
            var schema = SchemaInference.InferSchema<T>(fieldNames);
            return new StreamTable<T>(dataStream, schema, this);
        }

        public void RegisterTable(string name, ITable table)
        {
            if (string.IsNullOrEmpty(name))
                throw new ArgumentException("Table name cannot be null or empty", nameof(name));
            if (table == null)
                throw new ArgumentNullException(nameof(table));

            _registeredTables[name] = table;
            _logger?.LogInformation("Registered table: {TableName}", name);
        }

        public ITable SqlQuery(string query)
        {
            if (string.IsNullOrEmpty(query))
                throw new ArgumentException("SQL query cannot be null or empty", nameof(query));

            try
            {
                var parsedQuery = _sqlParser.Parse(query);
                return ExecuteQuery(parsedQuery);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Failed to execute SQL query: {Query}", query);
                throw new InvalidOperationException($"Failed to execute SQL query: {ex.Message}", ex);
            }
        }

        public ITable GetTable(string name)
        {
            if (_registeredTables.TryGetValue(name, out var table))
                return table;

            throw new ArgumentException($"Table '{name}' not found");
        }

        public string[] ListTables()
        {
            return _registeredTables.Keys.ToArray();
        }

        private ITable ExecuteQuery(ParsedSqlQuery parsedQuery)
        {
            // This is a simplified implementation - a full SQL engine would be much more complex
            switch (parsedQuery.Type)
            {
                case SqlQueryType.Select:
                    return ExecuteSelectQuery(parsedQuery);
                default:
                    throw new NotSupportedException($"SQL query type {parsedQuery.Type} is not supported yet");
            }
        }

        private ITable ExecuteSelectQuery(ParsedSqlQuery query)
        {
            // Get the source table
            if (query.FromTables.Count != 1)
                throw new NotSupportedException("Only single table queries are supported in this basic implementation");

            var sourceTable = GetTable(query.FromTables[0]);
            var result = sourceTable;

            // Apply WHERE clause
            if (!string.IsNullOrEmpty(query.WhereCondition))
            {
                result = result.Where(query.WhereCondition);
            }

            // Apply GROUP BY
            if (query.GroupByFields.Any())
            {
                var grouped = result.GroupBy(query.GroupByFields.ToArray());
                
                // Apply aggregations
                if (query.Aggregates.Any())
                {
                    result = grouped.Aggregate(query.Aggregates.ToArray());
                }
                else
                {
                    result = grouped.Count(); // Default aggregation
                }
            }

            // Apply SELECT (projection)
            if (query.SelectFields.Any() && !query.SelectFields.Contains("*"))
            {
                result = result.Select(query.SelectFields.ToArray());
            }

            return result;
        }

        internal StreamExecutionEnvironment StreamEnvironment => _streamEnv;
    }

    /// <summary>
    /// Utility class for inferring table schemas from .NET types
    /// </summary>
    public static class SchemaInference
    {
        public static TableSchema InferSchema<T>(string[] fieldNames)
        {
            var type = typeof(T);
            var properties = type.GetProperties();
            var fields = new List<TableField>();

            if (fieldNames.Length > 0)
            {
                // Use provided field names
                for (int i = 0; i < fieldNames.Length && i < properties.Length; i++)
                {
                    var prop = properties[i];
                    fields.Add(new TableField(fieldNames[i], prop.PropertyType, IsNullable(prop.PropertyType)));
                }
            }
            else
            {
                // Use property names
                foreach (var prop in properties)
                {
                    fields.Add(new TableField(prop.Name, prop.PropertyType, IsNullable(prop.PropertyType)));
                }
            }

            return new TableSchema(fields);
        }

        private static bool IsNullable(Type type)
        {
            return !type.IsValueType || (Nullable.GetUnderlyingType(type) != null);
        }
    }

    /// <summary>
    /// Simple SQL parser for basic query support
    /// </summary>
    public interface ISqlParser
    {
        ParsedSqlQuery Parse(string sql);
    }

    public class BasicSqlParser : ISqlParser
    {
        public ParsedSqlQuery Parse(string sql)
        {
            // This is a very basic parser - a production implementation would use a proper SQL parser
            var query = new ParsedSqlQuery { Type = SqlQueryType.Select };
            var sqlUpper = sql.ToUpperInvariant().Trim();

            if (!sqlUpper.StartsWith("SELECT"))
                throw new NotSupportedException("Only SELECT queries are supported");

            // Extract SELECT fields
            var selectStart = sqlUpper.IndexOf("SELECT") + 6;
            var fromIndex = sqlUpper.IndexOf("FROM");
            if (fromIndex == -1)
                throw new ArgumentException("FROM clause is required");

            var selectClause = sql.Substring(selectStart, fromIndex - selectStart).Trim();
            query.SelectFields = selectClause.Split(',').Select(f => f.Trim()).ToList();

            // Extract FROM table
            var fromStart = fromIndex + 4;
            var whereIndex = sqlUpper.IndexOf("WHERE");
            var groupByIndex = sqlUpper.IndexOf("GROUP BY");
            var orderByIndex = sqlUpper.IndexOf("ORDER BY");

            var fromEnd = new[] { whereIndex, groupByIndex, orderByIndex }.Where(i => i > -1).DefaultIfEmpty(sql.Length).Min();
            var fromClause = sql.Substring(fromStart, fromEnd - fromStart).Trim();
            query.FromTables = new List<string> { fromClause };

            // Extract WHERE clause
            if (whereIndex > -1)
            {
                var whereStart = whereIndex + 5;
                var whereEnd = new[] { groupByIndex, orderByIndex }.Where(i => i > whereIndex).DefaultIfEmpty(sql.Length).Min();
                query.WhereCondition = sql.Substring(whereStart, whereEnd - whereStart).Trim();
            }

            // Extract GROUP BY
            if (groupByIndex > -1)
            {
                var groupByStart = groupByIndex + 8;
                var groupByEnd = orderByIndex > groupByIndex ? orderByIndex : sql.Length;
                var groupByClause = sql.Substring(groupByStart, groupByEnd - groupByStart).Trim();
                query.GroupByFields = groupByClause.Split(',').Select(f => f.Trim()).ToList();
            }

            return query;
        }
    }

    public class ParsedSqlQuery
    {
        public SqlQueryType Type { get; set; }
        public List<string> SelectFields { get; set; } = new();
        public List<string> FromTables { get; set; } = new();
        public string? WhereCondition { get; set; }
        public List<string> GroupByFields { get; set; } = new();
        public List<IAggregateFunction> Aggregates { get; set; } = new();
    }

    public enum SqlQueryType
    {
        Select,
        Insert,
        Update,
        Delete
    }
}