# force-kill all cmd/conhost/powershell started by our SSH sessions (2:0x-2:1x + 10:1x today)
$cutoff = (Get-Date).AddHours(-12)
$targets = Get-Process | Where-Object { $_.ProcessName -match 'cmd|conhost' -and $_.StartTime -gt $cutoff }
foreach ($p in $targets) {
    try { Stop-Process -Id $p.Id -Force -ErrorAction Stop; Write-Output "KILLED $($p.Id) $($p.ProcessName)" } catch { }
}
# also any stray jcode
Get-Process -Name 'jcode' -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2
if (Test-Path 'C:\Users\trade\mqtt-broker-rs\jcode_sys.log') { Remove-Item 'C:\Users\trade\mqtt-broker-rs\jcode_sys.log' -Force -ErrorAction SilentlyContinue }
if (Test-Path 'C:\Users\trade\mqtt-broker-rs\mio_broker\10s') { Remove-Item 'C:\Users\trade\mqtt-broker-rs\mio_broker\10s' -Force -ErrorAction SilentlyContinue }
# verify log is now writable
try {
    Set-Content -Path 'C:\Users\trade\mqtt-broker-rs\jcode_sys.log' -Value 'WIRETEST' -ErrorAction Stop
    Write-Output 'LOG_WRITABLE'
} catch {
    Write-Output "LOG_BLOCKED: $($_.Exception.Message)"
}
