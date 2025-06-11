using System;
using System.Linq;
using FlinkDotNet.Core.Api.Streaming;

namespace FlinkDotNet.Table.Api
{
    /// <summary>
    /// Implementation of ITable that wraps a DataStream
    /// </summary>
    internal class StreamTable<T> : ITable
    {
        private readonly DataStream<T> _dataStream;
        private readonly TableEnvironment _tableEnvironment;

        public TableSchema Schema { get; }

        public StreamTable(DataStream<T> dataStream, TableSchema schema, TableEnvironment tableEnvironment)
        {
            _dataStream = dataStream ?? throw new ArgumentNullException(nameof(dataStream));
            Schema = schema ?? throw new ArgumentNullException(nameof(schema));
            _tableEnvironment = tableEnvironment ?? throw new ArgumentNullException(nameof(tableEnvironment));
        }

        public ITable Select(params string[] fieldNames)
        {
            if (fieldNames == null || fieldNames.Length == 0)
                throw new ArgumentException("Field names cannot be null or empty");

            // Validate field names exist in schema
            foreach (var fieldName in fieldNames)
            {
                if (!Schema.HasField(fieldName))
                    throw new ArgumentException($"Field '{fieldName}' not found in table schema");
            }

            // Create new schema with selected fields
            var selectedFields = fieldNames.Select(name => Schema.GetField(name));
            var newSchema = new TableSchema(selectedFields);

            // For this basic implementation, we'll return a new table with the projected schema
            // In a full implementation, this would add a projection operator to the execution plan
            return new ProjectedTable<T>(this, newSchema, fieldNames);
        }

        public ITable Where(string condition)
        {
            if (string.IsNullOrEmpty(condition))
                throw new ArgumentException("Condition cannot be null or empty");

            // In a full implementation, this would parse the condition and add a filter operator
            return new FilteredTable<T>(this, condition);
        }

        public IGroupedTable GroupBy(params string[] fieldNames)
        {
            if (fieldNames == null || fieldNames.Length == 0)
                throw new ArgumentException("Group by fields cannot be null or empty");

            // Validate field names
            foreach (var fieldName in fieldNames)
            {
                if (!Schema.HasField(fieldName))
                    throw new ArgumentException($"Field '{fieldName}' not found in table schema");
            }

            return new GroupedStreamTable<T>(this, fieldNames);
        }

        public ITable Join(ITable other, string condition)
        {
            if (other == null)
                throw new ArgumentNullException(nameof(other));
            if (string.IsNullOrEmpty(condition))
                throw new ArgumentException("Join condition cannot be null or empty");

            // In a full implementation, this would create a join operator
            return new JoinedTable(this, other, condition);
        }

        public DataStream<TOut> ToDataStream<TOut>()
        {
            // In a full implementation, this would execute the table operations
            // and convert back to a DataStream
            if (typeof(TOut) == typeof(T))
            {
                return (DataStream<TOut>)(object)_dataStream;
            }

            throw new NotSupportedException($"Conversion from {typeof(T)} to {typeof(TOut)} is not supported yet");
        }

        public ITable Sql(string query)
        {
            return _tableEnvironment.SqlQuery(query);
        }

        internal DataStream<T> DataStream => _dataStream;
    }

    /// <summary>
    /// Table that represents a projection (SELECT with specific fields)
    /// </summary>
    internal class ProjectedTable<T> : ITable
    {
        private readonly ITable _sourceTable;

        public TableSchema Schema { get; }

        public ProjectedTable(ITable sourceTable, TableSchema schema, string[] projectedFields)
        {
            _sourceTable = sourceTable;
            Schema = schema;
        }

        public ITable Select(params string[] fieldNames) => _sourceTable.Select(fieldNames);
        public ITable Where(string condition) => new FilteredTable<T>(this, condition);
        public IGroupedTable GroupBy(params string[] fieldNames) => new GroupedStreamTable<T>(this, fieldNames);
        public ITable Join(ITable other, string condition) => new JoinedTable(this, other, condition);
        public DataStream<TOut> ToDataStream<TOut>() => _sourceTable.ToDataStream<TOut>();
        public ITable Sql(string query) => _sourceTable.Sql(query);
    }

    /// <summary>
    /// Table that represents a filter (WHERE clause)
    /// </summary>
    internal class FilteredTable<T> : ITable
    {
        private readonly ITable _sourceTable;
        private readonly string _condition;

        public TableSchema Schema => _sourceTable.Schema;

        public FilteredTable(ITable sourceTable, string condition)
        {
            _sourceTable = sourceTable;
            _condition = condition;
        }

        public ITable Select(params string[] fieldNames) => _sourceTable.Select(fieldNames);
        public ITable Where(string condition) => new FilteredTable<T>(_sourceTable, $"({_condition}) AND ({condition})");
        public IGroupedTable GroupBy(params string[] fieldNames) => new GroupedStreamTable<T>(this, fieldNames);
        public ITable Join(ITable other, string condition) => new JoinedTable(this, other, condition);
        public DataStream<TOut> ToDataStream<TOut>() => _sourceTable.ToDataStream<TOut>();
        public ITable Sql(string query) => _sourceTable.Sql(query);
    }

    /// <summary>
    /// Grouped table implementation
    /// </summary>
    internal class GroupedStreamTable<T> : IGroupedTable
    {
        private readonly ITable _sourceTable;
        private readonly string[] _groupByFields;

        public GroupedStreamTable(ITable sourceTable, string[] groupByFields)
        {
            _sourceTable = sourceTable;
            _groupByFields = groupByFields;
        }

        public ITable Aggregate(params IAggregateFunction[] aggregates)
        {
            // Create new schema with group by fields and aggregate results
            var groupFields = _groupByFields.Select(name => _sourceTable.Schema.GetField(name));
            var aggFields = aggregates.Select(agg => new TableField(agg.Alias, typeof(object))); // Simplified
            var newSchema = new TableSchema(groupFields.Concat(aggFields));

            return new AggregatedTable(_sourceTable, newSchema, _groupByFields, aggregates);
        }

        public ITable Count() => Aggregate(Aggregates.Count());
        public ITable Sum(string fieldName) => Aggregate(Aggregates.Sum(fieldName));
        public ITable Avg(string fieldName) => Aggregate(Aggregates.Avg(fieldName));
        public ITable Min(string fieldName) => Aggregate(Aggregates.Min(fieldName));
        public ITable Max(string fieldName) => Aggregate(Aggregates.Max(fieldName));
    }

    /// <summary>
    /// Table that represents an aggregation result
    /// </summary>
    internal class AggregatedTable : ITable
    {
        private readonly ITable _sourceTable;

        public TableSchema Schema { get; }

        public AggregatedTable(ITable sourceTable, TableSchema schema, string[] groupByFields, IAggregateFunction[] aggregates)
        {
            _sourceTable = sourceTable;
            Schema = schema;
        }

        public ITable Select(params string[] fieldNames) => new ProjectedTable<object>(this, Schema, fieldNames);
        public ITable Where(string condition) => new FilteredTable<object>(this, condition);
        public IGroupedTable GroupBy(params string[] fieldNames) => new GroupedStreamTable<object>(this, fieldNames);
        public ITable Join(ITable other, string condition) => new JoinedTable(this, other, condition);
        public DataStream<TOut> ToDataStream<TOut>() => _sourceTable.ToDataStream<TOut>();
        public ITable Sql(string query) => _sourceTable.Sql(query);
    }

    /// <summary>
    /// Table that represents a join operation
    /// </summary>
    internal class JoinedTable : ITable
    {
        private readonly ITable _leftTable;
        private readonly ITable _rightTable;

        public TableSchema Schema { get; }

        public JoinedTable(ITable leftTable, ITable rightTable, string condition)
        {
            _leftTable = leftTable;
            _rightTable = rightTable;

            // Combine schemas (simplified - would need to handle name conflicts)
            var combinedFields = _leftTable.Schema.Fields.Concat(_rightTable.Schema.Fields);
            Schema = new TableSchema(combinedFields);
        }

        public ITable Select(params string[] fieldNames) => new ProjectedTable<object>(this, Schema, fieldNames);
        public ITable Where(string condition) => new FilteredTable<object>(this, condition);
        public IGroupedTable GroupBy(params string[] fieldNames) => new GroupedStreamTable<object>(this, fieldNames);
        public ITable Join(ITable other, string condition) => new JoinedTable(this, other, condition);
        public DataStream<TOut> ToDataStream<TOut>() => _leftTable.ToDataStream<TOut>();
        public ITable Sql(string query) => _leftTable.Sql(query);
    }
}