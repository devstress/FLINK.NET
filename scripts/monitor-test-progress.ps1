param(
    [string]$LogDir,
    [switch]$SkipSonar,
    [switch]$SkipStress,
    [switch]$SkipReliability
)

$tests = @(
    @{ Name = 'Unit Tests'; Log = 'unit-tests.log'; Status = 'unit-tests.status'; Skip = $false },
    @{ Name = 'Integration Tests'; Log = 'integration-tests.log'; Status = $null; Skip = $false },
    @{ Name = 'Stress Tests'; Log = 'stress-tests.log'; Status = 'stress-tests.status'; Skip = $SkipStress },
    @{ Name = 'Reliability Tests'; Log = 'reliability-tests.log'; Status = 'reliability-tests.status'; Skip = $SkipReliability },
    @{ Name = 'SonarCloud'; Log = 'sonarcloud.log'; Status = 'sonarcloud.status'; Skip = $SkipSonar }
)

Write-Host "=== Monitoring Test Progress ==="

$done = @{}
while ($true) {
    $allDone = $true
    $index = 1
    foreach ($t in $tests) {
        if ($t.Skip) { $index++; continue }
        $logPath = Join-Path $LogDir $t.Log
        $statusPath = if ($t.Status) { Join-Path $LogDir $t.Status } else { $null }
        $percent = 0
        if ($statusPath -and (Test-Path $statusPath)) { $percent = 100 }
        elseif (Test-Path $logPath) {
            $content = Get-Content $logPath -Raw -ErrorAction SilentlyContinue
            if ($content -match 'completed successfully' -or $content -match 'PASSED') { $percent = 100 }
            elseif ($content -match 'Running' -or $content -match 'Building') { $percent = 50 }
            elseif ($content.Length -gt 0) { $percent = 10 }
        }
        if ($percent -lt 100) { $allDone = $false }
        Write-Progress -Id $index -Activity $t.Name -PercentComplete $percent
        if ($percent -eq 100 -and -not $done[$t.Name]) {
            $done[$t.Name] = $true
            Write-Host "$($t.Name) completed"
        }
        $index++
    }
    if ($allDone) { break }
    Start-Sleep -Seconds 2
}

Write-Host "=== All tests completed ==="

