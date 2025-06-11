using System;
using System.Collections.Generic;
using System.Linq;

namespace FlinkDotNet.Table.Api
{
    /// <summary>
    /// Represents a table in Flink.NET's Table API
    /// </summary>
    public interface ITable
    {
        /// <summary>
        /// Gets the schema of the table
        /// </summary>
        TableSchema Schema { get; }

        /// <summary>
        /// Selects specific columns from the table
        /// </summary>
        ITable Select(params string[] fieldNames);

        /// <summary>
        /// Filters rows based on a predicate
        /// </summary>
        ITable Where(string condition);

        /// <summary>
        /// Groups the table by the specified fields
        /// </summary>
        IGroupedTable GroupBy(params string[] fieldNames);

        /// <summary>
        /// Joins this table with another table
        /// </summary>
        ITable Join(ITable other, string condition);

        /// <summary>
        /// Converts the table to a data stream
        /// </summary>
        FlinkDotNet.Core.Api.Streaming.DataStream<T> ToDataStream<T>();

        /// <summary>
        /// Executes a SQL query on this table
        /// </summary>
        ITable Sql(string query);
    }

    /// <summary>
    /// Represents a grouped table that supports aggregation operations
    /// </summary>
    public interface IGroupedTable
    {
        /// <summary>
        /// Aggregates the grouped table
        /// </summary>
        ITable Aggregate(params IAggregateFunction[] aggregates);

        /// <summary>
        /// Computes the count for each group
        /// </summary>
        ITable Count();

        /// <summary>
        /// Computes the sum for each group
        /// </summary>
        ITable Sum(string fieldName);

        /// <summary>
        /// Computes the average for each group
        /// </summary>
        ITable Avg(string fieldName);

        /// <summary>
        /// Computes the minimum for each group
        /// </summary>
        ITable Min(string fieldName);

        /// <summary>
        /// Computes the maximum for each group
        /// </summary>
        ITable Max(string fieldName);
    }

    /// <summary>
    /// Base interface for aggregate functions
    /// </summary>
    public interface IAggregateFunction
    {
        string Name { get; }
        string FieldName { get; }
        string Alias { get; }
    }

    /// <summary>
    /// Represents the schema of a table
    /// </summary>
    public class TableSchema
    {
        public IReadOnlyList<TableField> Fields { get; }

        public TableSchema(IEnumerable<TableField> fields)
        {
            Fields = fields.ToList().AsReadOnly();
        }

        public TableField GetField(string name)
        {
            return Fields.FirstOrDefault(f => f.Name == name) 
                ?? throw new ArgumentException($"Field '{name}' not found in schema");
        }

        public bool HasField(string name)
        {
            return Fields.Any(f => f.Name == name);
        }
    }

    /// <summary>
    /// Represents a field in a table schema
    /// </summary>
    public class TableField
    {
        public string Name { get; }
        public Type DataType { get; }
        public bool IsNullable { get; }

        public TableField(string name, Type dataType, bool isNullable = true)
        {
            Name = name ?? throw new ArgumentNullException(nameof(name));
            DataType = dataType ?? throw new ArgumentNullException(nameof(dataType));
            IsNullable = isNullable;
        }
    }

    /// <summary>
    /// Factory for creating common aggregate functions
    /// </summary>
    public static class Aggregates
    {
        public static IAggregateFunction Count() => new CountAggregate();
        public static IAggregateFunction Sum(string fieldName, string? alias = null) 
            => new SumAggregate(fieldName, alias ?? $"SUM_{fieldName}");
        public static IAggregateFunction Avg(string fieldName, string? alias = null) 
            => new AvgAggregate(fieldName, alias ?? $"AVG_{fieldName}");
        public static IAggregateFunction Min(string fieldName, string? alias = null) 
            => new MinAggregate(fieldName, alias ?? $"MIN_{fieldName}");
        public static IAggregateFunction Max(string fieldName, string? alias = null) 
            => new MaxAggregate(fieldName, alias ?? $"MAX_{fieldName}");
    }

    // Aggregate function implementations
    internal class CountAggregate : IAggregateFunction
    {
        public string Name => "COUNT";
        public string FieldName => "*";
        public string Alias => "COUNT";
    }

    internal class SumAggregate : IAggregateFunction
    {
        public string Name => "SUM";
        public string FieldName { get; }
        public string Alias { get; }

        public SumAggregate(string fieldName, string alias)
        {
            FieldName = fieldName;
            Alias = alias;
        }
    }

    internal class AvgAggregate : IAggregateFunction
    {
        public string Name => "AVG";
        public string FieldName { get; }
        public string Alias { get; }

        public AvgAggregate(string fieldName, string alias)
        {
            FieldName = fieldName;
            Alias = alias;
        }
    }

    internal class MinAggregate : IAggregateFunction
    {
        public string Name => "MIN";
        public string FieldName { get; }
        public string Alias { get; }

        public MinAggregate(string fieldName, string alias)
        {
            FieldName = fieldName;
            Alias = alias;
        }
    }

    internal class MaxAggregate : IAggregateFunction
    {
        public string Name => "MAX";
        public string FieldName { get; }
        public string Alias { get; }

        public MaxAggregate(string fieldName, string alias)
        {
            FieldName = fieldName;
            Alias = alias;
        }
    }
}