# Simplified Quality Rules for Copilot Agents

## 🚨 SIMPLIFIED ENFORCEMENT - SINGLE SOURCE OF TRUTH

### Rule #1: Run All Workflows Requirement (FUNDAMENTAL)
**VIOLATION = IMMEDIATE REJECTION**

The **ONLY** quality requirement is that the local run-all-workflows scripts must pass completely:

#### Windows:
```cmd
run-all-workflows.cmd
```

#### Linux:
```bash
./run-all-workflows.sh
```

**Success Criteria:**
- ✅ All 4 workflows complete successfully: Unit Tests, SonarCloud, Stress Tests, Integration Tests
- ✅ Exit code 0 from the run-all-workflows script
- ✅ No workflow failures reported in the summary

**The run-all-workflows scripts replicate the exact GitHub Actions workflows locally, ensuring 100% CI alignment.**

### Rule #2: Workflow Synchronization Requirement (MANDATORY)
**VIOLATION = IMMEDIATE REJECTION**

The run-all-workflows files MUST stay synchronized with GitHub workflow files. Any changes to `.github/workflows/*.yml` require corresponding updates to `run-all-workflows.cmd` and `run-all-workflows.sh`.

**Verification:**
```bash
# Check for workflow synchronization
./scripts/validate-workflow-sync.ps1
```

**Sync Requirements:**
- ✅ All build steps must match between GitHub workflows and run-all-workflows
- ✅ All environment variables must match
- ✅ All test execution commands must match  
- ✅ All dependencies and prerequisites must match

## 🔧 Enforcement Mechanisms

### Pre-Submission Checklist (MANDATORY)
Before using `report_progress`, copilot agents **MUST** complete:

- [ ] ✅ **RUN ALL WORKFLOWS**: `run-all-workflows.cmd` (Windows) or `./run-all-workflows.sh` (Linux)
- [ ] ✅ **VERIFY SUCCESS**: All 4 workflows complete with exit code 0
- [ ] ✅ **CHECK SYNC**: `./scripts/validate-workflow-sync.ps1` passes
- [ ] ✅ **GIT STATUS**: `git status --porcelain` shows only intended changes

**That's it!** The run-all-workflows scripts handle all quality validation:
- Build verification with warning detection
- Unit test execution (100% pass rate required)
- Integration test execution (100% pass rate required) 
- Stress test verification (performance criteria required)
- SonarCloud analysis and coverage submission

### Automated Quality Gate
```bash
# Single command enforcement
if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" ]]; then
    run-all-workflows.cmd
else
    ./run-all-workflows.sh
fi

if [ $? -ne 0 ]; then
    echo "❌ QUALITY GATE FAILED: run-all-workflows failed"
    exit 1
fi

# Verify workflow synchronization
./scripts/validate-workflow-sync.ps1
if [ $? -ne 0 ]; then
    echo "❌ QUALITY GATE FAILED: Workflow sync validation failed"
    exit 1
fi

echo "✅ ALL QUALITY GATES PASSED"
```

## 🎯 Success Criteria

### Definition of Complete
A submission is **COMPLETE** only when:

✅ **run-all-workflows.cmd** (Windows) OR **run-all-workflows.sh** (Linux) exits with code 0
✅ **validate-workflow-sync.ps1** passes (ensures GitHub workflow alignment)
✅ **Git status**: Clean, only intended files modified

### Failure Conditions
❌ **ANY workflow failure** in run-all-workflows = COMPLETE FAILURE
❌ **Workflow sync validation failure** = COMPLETE FAILURE  
❌ **Non-zero exit code** from run-all-workflows = COMPLETE FAILURE
❌ **Excessive code changes** = COMPLETE FAILURE

## 📞 Escalation Process

### Quality Gate Failures
1. **Stop immediately** when run-all-workflows fails
2. **Check workflow-logs/ directory** for specific failure details
3. **Fix the specific issue** causing the workflow failure
4. **Re-run run-all-workflows** until it passes
5. **Only proceed** when exit code is 0

### Workflow Sync Issues
1. **Run validate-workflow-sync.ps1** to identify sync issues
2. **Update run-all-workflows files** to match GitHub workflow changes
3. **Verify alignment** by running validation again
4. **Test locally** to ensure changes work correctly

---

**These rules are NON-NEGOTIABLE. The run-all-workflows scripts are the single source of truth for quality validation.**







