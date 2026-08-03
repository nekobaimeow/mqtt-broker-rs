---
name: apply-patch-and-bash-windows
description: "How to use apply_patch and bash tools correctly on Windows. Read this BEFORE editing any file. Mandatory for all coding tasks on this machine."
---

<Purpose>
This machine runs jcode on Windows. The apply_patch and bash tools have specific syntax requirements that MUST be followed or every edit fails with confusing errors. Read this skill before touching any file.
</Purpose>

<Use_When>
- ANY time you need to edit a file (src/main.rs or anything else)
- ANY time you run a shell command
</Use_When>

<apply_patch_CRITICAL_Format>
The apply_patch tool REQUIRES the V4A patch format. Every patch MUST contain BOTH:

1. A `*** Update File: <relative-or-absolute-path>` line
2. `@@` context marker lines (can be empty: `@@` alone works)

Correct format:

*** Begin Patch
*** Update File: src/main.rs
@@
-    drops: u64,
+    drops: u64,
+    sys_start: Instant,
*** End Patch

RULES:
- The `*** Update File:` line is MANDATORY. Without it the tool returns "Error: No valid patch directives found".
- Context lines (no prefix) help locate the change; use a few around each hunk.
- Lines to remove start with `-`, lines to add start with `+`.
- You may have multiple hunks in one patch, each with its own `@@` line.
- NEVER try to replace the whole file in one patch. Small, surgical hunks only.
- NEVER use the write tool to overwrite an entire existing source file — that destroys the file. Only write is for NEW files.
- If a hunk fails to match, READ the file again and re-check the exact text, then retry with correct context.
</apply_patch_CRITICAL_Format>

<bash_CRITICAL_Paths>
The bash tool on this machine runs under Windows. Use ONLY Windows-style paths:

- CORRECT: cd /d C:\Users\trade\mqtt-broker-rs\mio_broker && cargo build
- WRONG (Git-Bash style): cd /d/C/Users/trade/mqtt-broker-rs/mio_broker — this FAILS, the command is not found.

RULES:
- Absolute Windows paths start with a drive letter: C:\Users\... or D:\...
- Use backslashes, not forward slashes, in paths.
- To cd to a directory: cd /d C:\path\to\dir
- cargo is available on PATH; run: cargo build 2>&1 | tail -5
- When running cargo, ALWAYS cd into the crate directory first (C:\Users\trade\mqtt-broker-rs\mio_broker).
- If you see "系统找不到指定的文件" (system cannot find the file) or "'cd' is not recognized", you used a Git-Bash path. Fix it to Windows style.
</bash_CRITICAL_Paths>

<Workflow>
1. READ the target file section first (use read with offset/limit).
2. Plan the exact hunks. Small steps.
3. Apply ONE patch at a time via apply_patch.
4. After each patch, re-read the changed region to confirm.
5. When done editing, run cargo build to verify. Fix errors iteratively.
6. Never report success without cargo build passing AND re-reading your changes.
</Workflow>
