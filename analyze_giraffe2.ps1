$c = Get-Content 'C:\Users\trade\.jcode\sessions\session_giraffe_1785733450163_3d638d7cdc6a1fc2.journal.jsonl'
Write-Output "=== ALL TOOL RESULTS (last 4 events) ==="
$c | Select-Object -Last 4 | ForEach-Object {
    if ($_ -match '"role":"(\w+)"') { $r = $matches[1] } else { $r = '?' }
    if ($_ -match '"name":"([a-z_]+)"') { $n = $matches[1] } else { $n = '-' }
    Write-Output "--- [$r] $n ---"
    if ($_ -match '"content":"((?:[^"\\]|\\.)*)"') {
        $ct = $matches[1]
        $ct = $ct -replace '\\n', ' ' -replace '\\"', '"'
        if ($ct.Length -gt 800) { $ct = $ct.Substring(0, 800) + '...' }
        Write-Output $ct
    }
}
