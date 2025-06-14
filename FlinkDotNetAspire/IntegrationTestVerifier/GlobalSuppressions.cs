// This file is used by Code Analysis to maintain SuppressMessage
// attributes that are applied to this project.
// Project-level suppressions either have no target or are given
// a specific target and scoped to a namespace, type, member, etc.

using System.Diagnostics.CodeAnalysis;

// Test verifier specific suppressions
[assembly: SuppressMessage("Design", "S1144:Remove the unused private method", Justification = "Diagnostic and analysis methods are kept for debugging and future use")]