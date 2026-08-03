@echo off
cd /d C:\Users\trade\mqtt-broker-rs\mio_broker
set JCODE_NO_TELEMETRY=1
set DO_NOT_TRACK=1
C:\Users\trade\.local\bin\jcode.exe --provider-profile zen-free --model ling-3.0-flash-free --tools read,grep,apply_patch,bash run "TASK: Smoke test. 1) Use apply_patch to create a new file test_smoke.txt with content SMOKE_OK. V4A format: *** Begin Patch then *** Update File: test_smoke.txt then @@ then +SMOKE_OK then *** End Patch. 2) Use bash: cd /d C:\Users\trade\mqtt-broker-rs\mio_broker && echo BASH_OK. Reply DONE only after both succeed."
