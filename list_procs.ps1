Get-Process | Where-Object { $_.ProcessName -match 'cmd|powershell|jcode|conhost' } | Select-Object Id, ProcessName, StartTime | Format-Table -AutoSize
