# SonarCloud Configuration

This directory contains FlinkDotNetAspire projects which are simulation, tooling, and integration test projects that don't require the same quality gates as the core FlinkDotNet library.

## SonarCloud Exclusions

All projects in this directory are excluded from SonarCloud coverage and reliability requirements through:

1. **Directory.Build.props**: Contains project-wide SonarCloud exclusion properties

### Properties Used

- `SonarQubeExclude=true`: Excludes the entire project from SonarCloud analysis
- `ExcludeFromCodeCoverage=true`: Excludes from code coverage requirements  
- `SonarQubeTestProject=true`: Marks as test/tooling project

### Why These Exclusions?

FlinkDotNetAspire contains:
- **FlinkJobSimulator**: Simulation code for testing the FlinkDotNet ecosystem
- **IntegrationTestVerifier**: Integration testing tooling 
- **FlinkDotNetAspire.AppHost.AppHost**: Aspire host configuration
- **FlinkDotNetAspire.AppHost.ServiceDefaults**: Aspire service defaults/tooling

These are supporting tools and simulation environments, not production library code, so they don't need the same quality gates as the core FlinkDotNet library.
