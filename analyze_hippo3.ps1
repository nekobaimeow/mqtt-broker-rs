$c = Get-Content 'C:\Users\trade\.jcode\sessions\session_hippo_1785723547961_bc09ae903ccfd61c.journal.jsonl'
$i = 0
foreach ($line in $c) {
    $i++
    if ($line -match '"name":"write"') {
        # extract the write input: content preview
        if ($line -match '"content":"((?:[^"\\]|\\.)*)"') {
            $content = $matches[1]
            $preview = $content.Substring(0, [Math]::Min(200, $content.Length))
            Write-Output "== WRITE LINE $i content-preview: $preview ..."
        } else {
            Write-Output "== WRITE LINE $i (no content match)"
        }
    }
    if ($line -match '"name":"bash"') {
        if ($line -match '"command":"((?:[^"\\]|\\.)*)"') {
            $cmd = $matches[1]
            if ($cmd -match 'cargo') { Write-Output "== BASH LINE $i cargo: $cmd" }
        }
    }
}
