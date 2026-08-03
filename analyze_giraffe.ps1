$c = Get-Content 'C:\Users\trade\.jcode\sessions\session_giraffe_1785733450163_3d638d7cdc6a1fc2.journal.jsonl'
Write-Output "LINES: $($c.Count)"
$toolNames = @()
foreach ($line in $c) {
    if ($line -match '"name":"([a-z_]+)"') { $toolNames += $matches[1] }
}
Write-Output "TOOLS:"
$toolNames | Group-Object | Sort-Object Count -Descending | ForEach-Object { Write-Output "  $($_.Name): $($_.Count)" }
Write-Output ""
Write-Output "apply_patch inputs (preview):"
foreach ($line in $c) {
    if ($line -match '"name":"apply_patch"') {
        $start = $line.IndexOf('"patch_text"')
        if ($start -ge 0) {
            $snippet = $line.Substring($start, [Math]::Min(350, $line.Length - $start))
            Write-Output "---"
            Write-Output $snippet
        }
    }
}
Write-Output ""
Write-Output "bash commands (preview):"
foreach ($line in $c) {
    if ($line -match '"name":"bash"') {
        if ($line -match '"command":"((?:[^"\\]|\\.)*)"') {
            $cmd = $matches[1]
            Write-Output "  > $cmd"
        }
    }
}
