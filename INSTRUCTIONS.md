# Copilot Agent Code Quality Instructions

## Overview
This document establishes strict quality standards that **MUST** be followed by all Copilot agents working on the FLINK.NET codebase. These rules are **mandatory** and code changes that do not meet these requirements will be **rejected**.

## ✅ Pre-Submission Requirements (MANDATORY)

### 0. Clean Build Policy (CRITICAL)
- **ALWAYS use clean builds** - Incremental builds mask warnings due to caching
- **NEVER trust incremental build warning counts** - they can show `0 Warning(s)` falsely
- **Build caching hides actual warnings** - clean builds reveal true warning state

### 1. Zero Warnings Policy
- **ALL code must build with ZERO warnings** across all solutions
- **ALL SonarAnalyzer warnings must be resolved**
- No exceptions - warnings indicate code quality issues that must be addressed

### 2. Test Requirements  
- **ALL existing tests MUST pass** - no test failures are acceptable
- **Integration tests MUST pass** in the FlinkDotNetAspire solution
- **Unit tests MUST pass** across all test projects
- **Architecture tests MUST pass** to ensure design compliance
- **Stress tests MUST pass** when applicable

### 3. Build Verification
- **ALL solutions must build successfully**:
  - `FlinkDotNet/FlinkDotNet.sln` - Core framework
  - `FlinkDotNet.WebUI/FlinkDotNet.WebUI.sln` - Web interface  
  - `FlinkDotNetAspire/FlinkDotNetAspire.sln` - Cloud-native deployment

## 🔧 Quality Verification Process

### Step 1: Clean Build Verification (MANDATORY)

⚠️ **CRITICAL: Always use clean builds** - Incremental builds can mask warnings due to build caching!

```bash
# MANDATORY: Clean builds to avoid caching issues
dotnet clean FlinkDotNet/FlinkDotNet.sln
dotnet build FlinkDotNet/FlinkDotNet.sln --verbosity normal

dotnet clean FlinkDotNet.WebUI/FlinkDotNet.WebUI.sln  
dotnet build FlinkDotNet.WebUI/FlinkDotNet.WebUI.sln --verbosity normal

dotnet clean FlinkDotNetAspire/FlinkDotNetAspire.sln
dotnet build FlinkDotNetAspire/FlinkDotNetAspire.sln --verbosity normal
```

**Expected Result:** `0 Warning(s), 0 Error(s)` for ALL solutions

⚠️ **WARNING**: Incremental builds may show `0 Warning(s)` due to caching even when warnings exist. ALWAYS clean before building to get accurate warning detection.

### Step 2: Test Execution
```bash
# Run all unit tests
dotnet test FlinkDotNet/FlinkDotNet.sln --verbosity normal

# Run integration tests  
dotnet test FlinkDotNetAspire/FlinkDotNetAspire.IntegrationTests/FlinkDotNetAspire.IntegrationTests.csproj --verbosity normal
```

**Expected Result:** All tests pass with no failures

### Step 3: Warning Analysis
If warnings exist, categorize and fix systematically:

#### Quick Fixes (Priority 1)
- **S125**: Remove commented out code
- **S1481**: Remove unused variables  
- **S4487**: Remove unused private fields
- **S1144**: Remove unused private fields/methods

#### Code Quality Fixes (Priority 2)  
- **S2325**: Make methods static where appropriate
- **S3881**: Fix IDisposable patterns
- **S2930**: Properly dispose resources
- **S3903**: Move classes to proper namespaces

#### Design Improvements (Priority 3)
- **S107**: Reduce parameter count (max 7 parameters)
- **S3776**: Reduce cognitive complexity (max 15)
- **S138**: Split long methods (max 80 lines)
- **S927**: Rename parameters to match interface

## 🚫 Submission Blockers

### Automatic Rejection Criteria
Code submissions will be **automatically rejected** if:

1. **Build warnings exist** - Even a single warning blocks submission
2. **Test failures occur** - Any failing test blocks submission  
3. **Build errors exist** - Code must compile successfully
4. **Integration tests fail** - Cloud deployment readiness required

### Exception Policy
**NO EXCEPTIONS** - These requirements apply to:
- All feature implementations
- All bug fixes  
- All refactoring work
- All documentation updates that involve code changes

## 🛠️ SonarAnalyzer Integration

### Local Warning Detection
The repository includes SonarAnalyzer.CSharp package at the root level via `Directory.Build.props`:

```xml
<ItemGroup>
  <PackageReference Include="SonarAnalyzer.CSharp" Version="9.35.0.94459">
    <PrivateAssets>all</PrivateAssets>
    <IncludeAssets>analyzers</IncludeAssets>
  </PackageReference>
</ItemGroup>
```

### Warning Categories to Monitor
- **S125**: Commented out code
- **S138**: Methods too long (>80 lines)
- **S107**: Too many parameters (>7)
- **S927**: Parameter naming inconsistencies
- **S1144**: Unused private members
- **S1450**: Fields that should be local variables
- **S1481**: Unused local variables
- **S2139**: Exception handling issues
- **S2325**: Methods that should be static
- **S2930**: Resource disposal issues
- **S3776**: Cognitive complexity too high (>15)
- **S3881**: IDisposable pattern violations
- **S3903**: Classes not in named namespaces
- **S4487**: Unread private fields
- **S6608**: Use indexing instead of LINQ First()

## 📊 Quality Metrics Tracking

### Current Status
- **FlinkDotNet Solution**: Target 0 warnings (currently 76)
- **WebUI Solution**: ✅ 0 warnings achieved
- **Aspire Solution**: ✅ 0 warnings achieved

### Progress Tracking
Document warning reduction progress:
```
Phase 1: Quick wins (S125, S1481, S4487) - Target: 30+ warnings
Phase 2: Code quality (S2325, S3881, S2930) - Target: 25+ warnings  
Phase 3: Design improvements (S107, S3776, S138) - Target: 20+ warnings
Phase 4: Final cleanup - Target: 0 warnings
```

## 🔄 Continuous Integration Alignment

### CI/CD Pipeline Requirements
- Code Analysis workflow must pass
- Stress Test workflow must pass
- All automated quality gates must be green

### Redis Configuration
For Aspire integration tests, use proper dependency injection:
```csharp
// ✅ Correct approach
builder.AddRedisClient("redis");

// ❌ Avoid manual connection strings
ConnectionMultiplexer.Connect("localhost:6379")
```

## 📝 Enforcement Mechanism

### Pre-Commit Checks
Copilot agents **MUST** run these checks before any submission:

1. **CLEAN BUILD VERIFICATION** - Avoid caching issues:
   ```bash
   # MANDATORY clean builds - incremental builds hide warnings!
   dotnet clean FlinkDotNet/FlinkDotNet.sln && dotnet build FlinkDotNet/FlinkDotNet.sln --verbosity normal
   dotnet clean FlinkDotNet.WebUI/FlinkDotNet.WebUI.sln && dotnet build FlinkDotNet.WebUI/FlinkDotNet.WebUI.sln --verbosity normal
   dotnet clean FlinkDotNetAspire/FlinkDotNetAspire.sln && dotnet build FlinkDotNetAspire/FlinkDotNetAspire.sln --verbosity normal
   ```
2. `git status --porcelain` - Verify clean working directory
3. Test execution commands (see Step 2 above)
4. Warning count verification (MUST be 0 after clean builds)

### Submission Protocol
1. ✅ **Complete all fixes** - Address every warning and test failure
2. ✅ **Verify zero warnings** - Confirm `0 Warning(s)` in build output
3. ✅ **Confirm test success** - All tests must pass
4. ✅ **Document changes** - Update progress in PR description
5. ✅ **Commit and push** - Use report_progress tool

### Violation Consequences  
- **Immediate rejection** of code submissions
- **Required rework** before re-submission
- **Escalation** for repeated violations

## 🎯 Success Criteria

### Definition of Done
A code submission is considered complete **ONLY** when:

- [ ] All 3 solutions build with 0 warnings, 0 errors
- [ ] All unit tests pass (100% success rate)
- [ ] All integration tests pass (100% success rate)  
- [ ] All architecture tests pass (100% success rate)
- [ ] Code follows established patterns and conventions
- [ ] Changes are minimal and surgical (avoid unnecessary modifications)

### Quality Verification
```bash
# Final verification command - CLEAN BUILDS REQUIRED
echo "=== QUALITY GATE VERIFICATION ===" 
echo "WARNING: Using clean builds to avoid caching issues..."

echo "Cleaning and building all solutions..."
dotnet clean FlinkDotNet/FlinkDotNet.sln
dotnet build FlinkDotNet/FlinkDotNet.sln --verbosity normal 2>&1 | grep "Warning(s)"

dotnet clean FlinkDotNet.WebUI/FlinkDotNet.WebUI.sln
dotnet build FlinkDotNet.WebUI/FlinkDotNet.WebUI.sln --verbosity normal 2>&1 | grep "Warning(s)"  

dotnet clean FlinkDotNetAspire/FlinkDotNetAspire.sln
dotnet build FlinkDotNetAspire/FlinkDotNetAspire.sln --verbosity normal 2>&1 | grep "Warning(s)"

echo "Running all tests..."
dotnet test FlinkDotNet/FlinkDotNet.sln --verbosity minimal
dotnet test FlinkDotNetAspire/FlinkDotNetAspire.IntegrationTests/FlinkDotNetAspire.IntegrationTests.csproj --verbosity minimal

echo "=== VERIFICATION COMPLETE ==="
```

Expected output: `0 Warning(s)` for all builds and `Passed!` for all test runs.

⚠️ **CRITICAL**: If incremental builds were used previously, they may have shown `0 Warning(s)` incorrectly due to build caching. Only clean builds provide accurate warning detection.

---

**These instructions are binding and non-negotiable. Code quality is not optional.**