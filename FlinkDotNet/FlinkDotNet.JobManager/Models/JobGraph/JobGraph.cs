using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
// Metrics definitions
using FlinkDotNet.JobManager.Models;
// Assuming FlinkDotNet.Proto.Internal for Proto definitions
using FlinkDotNet.Proto.Internal;


// JobVertex, JobEdge, VertexType, ShuffleMode, KeyingInfo etc. are expected to be in their own files
// within the FlinkDotNet.JobManager.Models.JobGraph namespace.

namespace FlinkDotNet.JobManager.Models.JobGraph
{
    public class JobGraph
    {
        public Guid JobId { get; }
        public string JobName { get; }
        public List<JobVertex> Vertices { get; } = new();
        public List<JobEdge> Edges { get; } = new();

        // --- Fields from 'main' (user's changes) ---
        public DateTime SubmissionTime { get; set; }
        public string Status { get; set; } // Examples: SUBMITTED, RUNNING, FAILED, COMPLETED
        
        public ConcurrentDictionary<string, TaskMetrics> TaskInstanceMetrics { get; } = new();

        // --- Fields from 'feature/comprehensive-flink-core-design' (Jules's conceptual changes) ---
        public Dictionary<string, string> SerializerTypeRegistrations { get; set; } = new();


        public JobGraph(string jobName)
        {
            JobId = Guid.NewGuid();
            JobName = jobName;
            SubmissionTime = DateTime.UtcNow;
            Status = "SUBMITTED"; 
        }

        public void AddVertex(JobVertex vertex)
        {
            Vertices.Add(vertex);
        }

        public void AddEdge(JobEdge edge)
        {
            // Ensure the edge connects vertices already in this graph (optional check)
            if (Vertices.Any(v => v.Id == edge.SourceVertexId) && Vertices.Any(v => v.Id == edge.TargetVertexId))
            {
                Edges.Add(edge);

                // Maintain lists of edge IDs on the connected vertices
                Vertices.First(v => v.Id == edge.SourceVertexId).AddOutputEdgeId(edge.Id);
                Vertices.First(v => v.Id == edge.TargetVertexId).AddInputEdgeId(edge.Id);
            }
            else
            {
                // Handle error: edge connects unknown vertices
                throw new ArgumentException("Edge connects vertices not present in the graph.");
            }
        }

        public List<JobVertex> GetSourceVertices() => Vertices.Where(v => v.Type == VertexType.Source).ToList();
        public List<JobVertex> GetSinkVertices() => Vertices.Where(v => v.Type == VertexType.Sink).ToList();

        /// <summary>
        /// Converts this JobGraph model to its Protobuf representation.
        /// </summary>
        /// <returns>The Protobuf JobGraph message.</returns>
        public Proto.Internal.JobGraph ToProto()
        {
            var protoJobGraph = new Proto.Internal.JobGraph
            {
                JobName = this.JobName,
                // JobConfiguration = { this.JobConfiguration }, // Assuming JobConfiguration is a map or repeated field
                SubmissionTime = Google.Protobuf.WellKnownTypes.Timestamp.FromDateTime(this.SubmissionTime.ToUniversalTime()),
                Status = this.Status
            };

            foreach (var vertex in Vertices)
            {
                protoJobGraph.Vertices.Add(vertex.ToProto());
            }

            foreach (var edge in Edges)
            {
                protoJobGraph.Edges.Add(edge.ToProto());
            }

            foreach (var registration in SerializerTypeRegistrations)
            {
                 protoJobGraph.SerializerTypeRegistrations.Add(registration.Key, registration.Value);
            }

            return protoJobGraph;
        }

        /// <summary>
        /// Creates a JobGraph model from its Protobuf representation.
        /// </summary>
        /// <param name="protoJobGraph">The Protobuf JobGraph message.</param>
        /// <returns>A new JobGraph instance.</returns>
        public static JobGraph FromProto(Proto.Internal.JobGraph protoJobGraph)
        {
            var jobGraph = new JobGraph(protoJobGraph.JobName)
            {
                // JobId is Guid, proto is string. Assuming JobGraph constructor creates a new Guid or one is assigned.
                // If protoJobGraph.JobId needs to be preserved and is a valid Guid:
                // jobGraph.JobId = Guid.Parse(protoJobGraph.JobId), // This would require JobId setter or different constructor
                Status = protoJobGraph.Status,
                SubmissionTime = protoJobGraph.SubmissionTime.ToDateTime()
            };

            foreach (var protoVertex in protoJobGraph.Vertices)
            {
                jobGraph.AddVertex(JobVertex.FromProto(protoVertex)); // Assumes JobVertex has FromProto
            }

            foreach (var protoEdge in protoJobGraph.Edges)
            {
                // Need to find source and target vertex objects already added to the graph
                // This is problematic if JobEdge.FromProto expects JobVertex objects.
                // JobEdge.FromProto should ideally take IDs and the JobGraph can resolve them.
                // For now, let's assume JobEdge.FromProto can handle it or we adapt.
                // A simple JobEdge.FromProto would just take the protoEdge and convert scalar properties.
                // The JobGraph would then be responsible for linking (e.g. vertex.AddInputEdgeId).

                // Simplified: Assuming JobEdge.FromProto creates an edge with IDs,
                // and JobGraph.AddEdge then correctly updates vertex edge lists.
                var modelEdge = JobEdge.FromProto(protoEdge); // Assumes JobEdge has FromProto
                jobGraph.AddEdge(modelEdge);

                // Post-addition, ensure vertices are linked with edge IDs
                var sourceVertex = jobGraph.Vertices.FirstOrDefault(v => v.Id.ToString() == protoEdge.SourceVertexId);
                var targetVertex = jobGraph.Vertices.FirstOrDefault(v => v.Id.ToString() == protoEdge.TargetVertexId);

                sourceVertex?.AddOutputEdgeId(Guid.Parse(protoEdge.Id));
                targetVertex?.AddInputEdgeId(Guid.Parse(protoEdge.Id));

            }

            foreach (var kvp in protoJobGraph.SerializerTypeRegistrations)
            {
                jobGraph.SerializerTypeRegistrations.Add(kvp.Key, kvp.Value);
            }


            return jobGraph;
        }
    }
}

#nullable disable
