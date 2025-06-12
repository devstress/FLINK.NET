# Mandatory Code Quality Rules for Copilot Agents

## 🚨 CRITICAL REQUIREMENTS - ZERO TOLERANCE

### Rule #0: Clean Build Requirement (FUNDAMENTAL)
**VIOLATION = IMMEDIATE REJECTION**
- **ALWAYS use clean builds** - Incremental builds hide warnings due to caching
- **NEVER trust incremental build warning counts** - they show false `0 Warning(s)`
- **Build caching masks actual warnings** - only clean builds reveal true state

### Rule #1: Zero Warnings Policy (ACHIEVED ✅)
**VIOLATION = IMMEDIATE REJECTION**
- **ALL core solutions MUST build with 0 warnings**:
  - ✅ **FlinkDotNet.sln**: 0 warnings, 0 errors 
  - ✅ **FlinkDotNet.WebUI.sln**: 0 warnings, 0 errors
  - ✅ **FlinkDotNetAspire.sln**: 0 warnings, 0 errors (own projects)
- **ALL SonarAnalyzer warnings MUST be resolved**  
- **NO exceptions, NO partial fixes, NO deferrals**

Verification command:
```bash
# MANDATORY: Clean build to avoid caching issues
dotnet clean FlinkDotNet/FlinkDotNet.sln
dotnet build FlinkDotNet/FlinkDotNet.sln --verbosity normal 2>&1 | grep "Warning(s)"
# Expected output: "0 Warning(s)" ✅ ACHIEVED

dotnet clean FlinkDotNet.WebUI/FlinkDotNet.WebUI.sln  
dotnet build FlinkDotNet.WebUI/FlinkDotNet.WebUI.sln --verbosity normal 2>&1 | grep "Warning(s)"
# Expected output: "0 Warning(s)" ✅ ACHIEVED

dotnet build FlinkDotNetAspire/FlinkDotNetAspire.IntegrationTests --verbosity normal 2>&1 | grep "Warning(s)"
# Expected output: "0 Warning(s)" ✅ ACHIEVED
```

### Rule #2: Test Success Requirement (100% PASS RATE MANDATORY)
**VIOLATION = IMMEDIATE REJECTION**
- **ALL unit tests MUST pass (100% success rate)** across all test projects:
  - FlinkDotNet.JobManager.Tests (~120 tests)
  - FlinkDotNet.Core.Tests
  - FlinkDotNet.Architecture.Tests (~7 tests)
  - FlinkDotNet.Connectors.*.Tests (all connector test projects)
  - FlinkDotNet.Storage.*.Tests (all storage test projects)
  - FlinkDotNet.Common.Constants.Tests
- **ALL integration tests MUST pass (100% success rate)**:
  - FlinkDotNetAspire.IntegrationTests must build without errors (no CS0400)
  - FlinkDotNetAspire.IntegrationTests must execute with 100% pass rate
- **ALL stress tests MUST pass**:
  - Local stress test verification MUST match CI workflow exactly
  - Performance criteria must be met
- **NO test failures are acceptable** in any category
- **NO build errors in test projects** - All test projects must compile successfully

Verification commands:
```bash
# Unit Tests (MANDATORY 100% pass rate)
dotnet test FlinkDotNet/FlinkDotNet.sln --verbosity minimal
# Expected output: All test projects pass, no failures

# Integration Tests (MANDATORY 100% pass rate)  
dotnet test FlinkDotNetAspire/FlinkDotNetAspire.IntegrationTests/FlinkDotNetAspire.IntegrationTests.csproj --verbosity minimal
# Expected output: All integration tests pass, no build errors

# Stress Tests (MANDATORY success)
./scripts/run-local-stress-tests.ps1
./scripts/test-local-stress-workflow-alignment.ps1
# Expected output: All stress tests SUCCESS, performance criteria met
```

### Rule #3: Build Success Requirement
**VIOLATION = IMMEDIATE REJECTION**  
- **ALL 3 solutions MUST build successfully**
- **NO build errors allowed**
- **FlinkDotNet, WebUI, and Aspire solutions must all compile**

## 🔧 Enforcement Mechanisms

### Pre-Submission Checklist (MANDATORY)
Before using `report_progress`, copilot agents **MUST** complete:

- [ ] 🧹 **CLEAN BUILD FIRST**: `dotnet clean` all solutions to avoid caching
- [ ] ✅ Clean build all solutions: `dotnet clean && dotnet build --verbosity normal` 
- [ ] ✅ Verify 0 warnings in clean build outputs (incremental builds lie!)
- [ ] ✅ **Run unit tests**: `dotnet test FlinkDotNet/FlinkDotNet.sln --verbosity minimal` (100% pass required)
- [ ] ✅ **Run integration tests**: `dotnet test FlinkDotNetAspire/FlinkDotNetAspire.IntegrationTests/FlinkDotNetAspire.IntegrationTests.csproj --verbosity minimal` (100% pass required)
- [ ] ✅ **Run stress tests**: `./scripts/run-local-stress-tests.ps1` (SUCCESS required)
- [ ] ✅ **Verify local/CI alignment**: `./scripts/test-local-stress-workflow-alignment.ps1` (SUCCESS required)
- [ ] ✅ **Confirm 100% test pass rate** for ALL test categories (unit, integration, stress)
- [ ] ✅ Check git status: `git status --porcelain`
- [ ] ✅ Verify no unwanted files staged

⚠️ **CRITICAL**: Never trust incremental build warning counts - they can show `0 Warning(s)` due to caching even when warnings exist!

### Automated Quality Gates

#### Gate 1: Clean Build Warning Detection
```bash
# CRITICAL: Clean build required - incremental builds hide warnings!
dotnet clean FlinkDotNet/FlinkDotNet.sln
WARNINGS=$(dotnet build FlinkDotNet/FlinkDotNet.sln --verbosity normal 2>&1 | grep -o "[0-9]* Warning(s)" | grep -o "[0-9]*")
if [ "$WARNINGS" != "0" ]; then
    echo "❌ QUALITY GATE FAILED: $WARNINGS warnings detected (after clean build)"
    echo "⚠️  Note: Incremental builds may have shown 0 warnings incorrectly due to caching"
    exit 1
fi
```

#### Gate 2: Test Validation (ALL TEST TYPES MANDATORY)
```bash
# Unit Tests - Must pass for ALL unit test projects (100% pass rate)
dotnet test FlinkDotNet/FlinkDotNet.sln --logger "console;verbosity=minimal" | grep -q "Failed: 0"
if [ $? -ne 0 ]; then
    echo "❌ QUALITY GATE FAILED: Unit test failures detected"
    echo "All unit test projects must pass: JobManager.Tests, Core.Tests, Architecture.Tests, Connectors.*.Tests, Storage.*.Tests"
    exit 1  
fi

# Integration Tests - Must build and pass (100% pass rate, no build errors)
dotnet test FlinkDotNetAspire/FlinkDotNetAspire.IntegrationTests/FlinkDotNetAspire.IntegrationTests.csproj --logger "console;verbosity=minimal" | grep -q "Failed: 0"
if [ $? -ne 0 ]; then
    echo "❌ QUALITY GATE FAILED: Integration test failures or build errors detected (check for CS0400 errors)"
    exit 1  
fi

# Stress test verification (local must match CI workflow)
./scripts/run-local-stress-tests.ps1
if [ $? -ne 0 ]; then
    echo "❌ QUALITY GATE FAILED: Stress test verification failed"
    exit 1  
fi

# Local/CI workflow alignment verification
./scripts/test-local-stress-workflow-alignment.ps1
if [ $? -ne 0 ]; then
    echo "❌ QUALITY GATE FAILED: Local stress tests don't match CI workflow"
    exit 1  
fi
```

#### Gate 3: Clean Build Validation
```bash
# CRITICAL: Clean build required for accurate results
dotnet clean FlinkDotNet/FlinkDotNet.sln
dotnet build FlinkDotNet/FlinkDotNet.sln --verbosity quiet
if [ $? -ne 0 ]; then
    echo "❌ QUALITY GATE FAILED: Build errors detected (after clean build)"
    exit 1
fi
```

## 📋 Warning Categories (Zero Tolerance)

### Phase 1: Immediate Fixes Required
- **S125**: Commented out code → Remove completely
- **S1481**: Unused variables → Remove declarations  
- **S4487**: Unused private fields → Remove or implement usage
- **S1144**: Unused private members → Remove completely

### Phase 2: Code Quality Fixes Required
- **S2325**: Non-static methods → Make static where appropriate
- **S3881**: IDisposable violations → Implement proper disposal patterns
- **S2930**: Resource leaks → Add proper disposal
- **S3903**: Namespace violations → Move to proper namespaces

### Phase 3: Design Improvements Required  
- **S107**: Too many parameters → Refactor to parameter objects
- **S3776**: High complexity → Split into smaller methods
- **S138**: Long methods → Break into smaller functions
- **S927**: Parameter naming → Align with interface contracts

## 🔄 Quality Assurance Process

### Step-by-Step Verification

1. **Clean Environment (CRITICAL for accurate warnings)**
   ```bash
   # MANDATORY: Clean all solutions to avoid build caching issues
   dotnet clean FlinkDotNet/FlinkDotNet.sln
   dotnet clean FlinkDotNet.WebUI/FlinkDotNet.WebUI.sln
   dotnet clean FlinkDotNetAspire/FlinkDotNetAspire.sln
   ```
   ⚠️ **WARNING**: Skipping clean builds can result in false `0 Warning(s)` reports due to caching!

2. **Build Verification (After Clean)** 
   ```bash
   # MANDATORY: Build after clean to get accurate warning detection
   dotnet build FlinkDotNet/FlinkDotNet.sln --verbosity normal
   dotnet build FlinkDotNet.WebUI/FlinkDotNet.WebUI.sln --verbosity normal
   dotnet build FlinkDotNetAspire/FlinkDotNetAspire.sln --verbosity normal
   ```

3. **Warning Inspection (Post-Clean Build)**
   ```bash
   # Each clean build MUST show: "0 Warning(s), 0 Error(s)"
   # ANY warnings = IMMEDIATE FAILURE
   # Previous incremental builds may have shown "0 Warning(s)" incorrectly due to caching
   ```

4. **Test Execution (ALL TEST TYPES MANDATORY)**
   ```bash
   # Unit Tests - 100% pass rate required across all test projects
   dotnet test FlinkDotNet/FlinkDotNet.sln --verbosity normal
   
   # Integration Tests - 100% pass rate required, no build errors
   dotnet test FlinkDotNetAspire/FlinkDotNetAspire.IntegrationTests/FlinkDotNetAspire.IntegrationTests.csproj --verbosity normal
   
   # Stress Tests - Performance criteria must be met
   ./scripts/run-local-stress-tests.ps1
   ./scripts/test-local-stress-workflow-alignment.ps1
   ```

5. **Final Validation**
   ```bash
   git status --porcelain  # Should show only intended changes
   git diff --numstat     # Verify minimal change scope
   ```

## ⚡ Rapid Warning Resolution

### Common Quick Fixes

#### S125 (Commented Code)
```csharp
// ❌ Remove this:
// var oldCode = something;
// oldCode.DoSomething();

// ✅ Clean code with no comments
```

#### S1481 (Unused Variables)
```csharp
// ❌ Remove unused variables:
public void Method() {
    var unused = GetValue(); // Remove this line
    DoWork();
}

// ✅ Clean implementation:
public void Method() {
    DoWork();
}
```

#### S4487 (Unused Fields)  
```csharp
// ❌ Remove or implement:
private readonly string _unusedField; // Remove completely

// ✅ Or implement proper usage:
private readonly string _configValue;
public string GetConfig() => _configValue;
```

#### S2325 (Static Methods)
```csharp
// ❌ Make static:
public string FormatValue(string input) {
    return input.ToUpper();
}

// ✅ Static implementation:
public static string FormatValue(string input) {
    return input.ToUpper();  
}
```

## 🎯 Success Criteria

### Definition of Complete
A submission is **COMPLETE** only when:

✅ **FlinkDotNet.sln**: 0 warnings, 0 errors, ALL unit tests pass (100% rate)
✅ **WebUI.sln**: 0 warnings, 0 errors, builds successfully  
✅ **Aspire.sln**: 0 warnings, 0 errors, ALL integration tests pass (100% rate)
✅ **Unit Tests**: ALL 7 test projects pass (JobManager, Core, Architecture, Connectors, Storage, Constants)
✅ **Integration Tests**: FlinkDotNetAspire.IntegrationTests builds and runs successfully (no CS0400 errors)
✅ **Stress tests**: Local verification matches CI workflow exactly, performance criteria met
✅ **Code changes**: Minimal, surgical, no unnecessary modifications  
✅ **Git status**: Clean, only intended files modified

### Failure Conditions
❌ **ANY warnings** in ANY solution = COMPLETE FAILURE  
❌ **ANY unit test failures** = COMPLETE FAILURE (across all 7+ test projects)
❌ **ANY integration test failures** = COMPLETE FAILURE (build errors or test failures)
❌ **ANY build errors** = COMPLETE FAILURE  
❌ **Stress test failures** = COMPLETE FAILURE
❌ **Local/CI workflow mismatch** = COMPLETE FAILURE
❌ **Excessive code changes** = COMPLETE FAILURE  

## 📞 Escalation Process

### Quality Gate Failures
1. **Stop immediately** when ANY quality gate fails
2. **Fix the specific issue** causing the failure  
3. **Re-run complete verification** process
4. **Only proceed** when ALL gates pass

### Persistent Issues
1. **Document the specific warning/error**
2. **Research the root cause thoroughly** 
3. **Implement the minimal fix required**
4. **Verify fix resolves issue completely**

## 🏆 Excellence Standards

### Code Quality Mindset
- **Zero warnings** is not optional - it's the minimum standard
- **All tests passing** is not optional - it's a basic requirement
- **Clean builds** are not optional - they're fundamental  

### Professional Standards
- **Systematic approach** to warning resolution
- **Comprehensive testing** before any submission
- **Minimal impact changes** that solve specific problems
- **Documentation** of quality improvements

---

**These rules are NON-NEGOTIABLE. Quality is not a suggestion - it's a requirement.**