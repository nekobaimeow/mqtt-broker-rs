@echo off
cd /d C:\Users\trade\mqtt-broker-rs\mio_broker
set JCODE_NO_TELEMETRY=1
set DO_NOT_TRACK=1
C:\Users\trade\.local\bin\jcode.exe --provider-profile zen-free --model north-mini-code-free --tools read,grep,apply_patch,bash run "TASK: Smoke test. 1) Create a new file test_patch.txt containing exactly: HELLO_SKILL_TEST. 2) Use bash: cd /d C:\Users\trade\mqtt-broker-rs\mio_broker && echo done. 3) If you loaded a skill about apply_patch format, mention SKILL_LOADED in your final reply, otherwise reply NOT_LOADED. Reply with the word that applies."
