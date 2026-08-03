# kill leftover processes from today's jcode attempts (started 2:00+)
$targets = Get-Process | Where-Object { ($_.ProcessName -match 'cmd|powershell|conhost|jcode') -and $_.StartTime -gt (Get-Date '2026-08-03 01:59:00') }
foreach ($p in $targets) {
    try { Stop-Process -Id $p.Id -Force -ErrorAction Stop; Write-Output "KILLED $($p.Id) $($p.ProcessName)" } catch { Write-Output "SKIP $($p.Id) $($p.ProcessName): $($_.Exception.Message)" }
}
Start-Sleep -Seconds 2
Remove-Item 'C:\Users\trade\mqtt-broker-rs\jcode_sys.log' -ErrorAction SilentlyContinue
Remove-Item 'C:\Users\trade\mqtt-broker-rs\mio_broker\10s' -ErrorAction SilentlyContinue
Write-Output 'CLEANUP_DONE'
