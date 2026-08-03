$c = Get-Content 'C:\Users\trade\.jcode\sessions\session_hippo_1785723547961_bc09ae903ccfd61c.journal.jsonl'
$i = 0
foreach ($line in $c) {
    $i++
    if ($line -match '"name":"write"' -or $line -match '"name":"apply_patch"' -or $line -match '"name":"bash"') {
        if ($line -match '"input":\{') {
            # extract input preview
            $short = $line
            if ($short.Length -gt 600) { $short = $short.Substring(0, 600) + '...' }
            Write-Output "== LINE $i =="
            Write-Output $short
            Write-Output ""
        }
    }
}
