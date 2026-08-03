$c = Get-Content 'C:\Users\trade\.jcode\sessions\session_bird_1785727606223_001f9159f7ca702e.journal.jsonl'
Write-Output "LINES: $($c.Count)"
$toolNames = @()
foreach ($line in $c) {
    if ($line -match '"name":"([a-z_]+)"') { $toolNames += $matches[1] }
}
Write-Output "TOOLS:"
$toolNames | Group-Object | Sort-Object Count -Descending | ForEach-Object { Write-Output "  $($_.Name): $($_.Count)" }
Write-Output ""
Write-Output "LAST 5 EVENTS:"
$c | Select-Object -Last 5 | ForEach-Object {
    if ($_ -match '"role":"(\w+)"') { $r = $matches[1] } else { $r = '?' }
    if ($_ -match '"name":"([a-z_]+)"') { $n = $matches[1] } else { $n = '-' }
    Write-Output "  [$r] $n"
}
Write-Output ""
Write-Output "APPLY_PATCH INPUTS (preview):"
foreach ($line in $c) {
    if ($line -match '"name":"apply_patch"') {
        $start = $line.IndexOf('"input":{')
        if ($start -ge 0) {
            $snippet = $line.Substring($start, [Math]::Min(400, $line.Length - $start))
            Write-Output "---"
            Write-Output $snippet
        }
    }
}
