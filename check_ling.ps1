$f = Get-Item 'C:\Users\trade\mqtt-broker-rs\jcode_sys_ling.log' -ErrorAction SilentlyContinue
if ($f) { Write-Output "SIZE: $($f.Length)  TIME: $($f.LastWriteTime)" } else { Write-Output 'NO LOG' }
$p = Get-Process -Name jcode -ErrorAction SilentlyContinue
if ($p) { $p | ForEach-Object { Write-Output "PID: $($_.Id)" } } else { Write-Output 'NO JCODE PROC' }
