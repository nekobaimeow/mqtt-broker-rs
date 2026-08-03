$c = Get-Content 'C:\Users\trade\.jcode\sessions\session_hippo_1785723547961_bc09ae903ccfd61c.journal.jsonl'
Write-Output "LINES: $($c.Count)"
$toolNames = @()
$writes = @()
$bash = @()
foreach ($line in $c) {
    if ($line -match '"name":"([a-z_]+)"') { $toolNames += $matches[1] }
    if ($line -match 'apply_patch') { $writes += 'patch' }
    if ($line -match '"file_path":"([^"]+)"') { $writes += "FILE: $($matches[1])" }
    if ($line -match 'cargo (build|test)') { $bash += "CARGO: $($matches[1])" }
}
Write-Output "TOOL USES:"
$toolNames | Group-Object | Sort-Object Count -Descending | Select-Object -First 10 | ForEach-Object { Write-Output "  $($_.Name): $($_.Count)" }
Write-Output "FILES TOUCHED:"
$writes | Select-Object -Unique | ForEach-Object { Write-Output "  $_" }
Write-Output "LAST 3 EVENTS:"
$c | Select-Object -Last 3 | ForEach-Object {
    if ($_ -match '"role":"(\w+)"') { $r = $matches[1] } else { $r = '?' }
    if ($_ -match '"name":"([a-z_]+)"') { $n = $matches[1] } else { $n = '-' }
    Write-Output "  [$r] $n"
}
