$c = Get-Content 'C:\Users\trade\.jcode\sessions\session_hippo_1785723547961_bc09ae903ccfd61c.journal.jsonl'
$lines = $c | Select-Object -Last 20
foreach ($line in $lines) {
    if ($line -match '"name":"apply_patch"') {
        $start = $line.IndexOf('"input":{')
        if ($start -ge 0) {
            $snippet = $line.Substring($start, [Math]::Min(500, $line.Length - $start))
            Write-Output "=== PATCH INPUT ==="
            Write-Output $snippet
            Write-Output ""
        }
    }
}
