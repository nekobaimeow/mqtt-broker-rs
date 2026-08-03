# jcode 任务模板生成器 — 每次推任务前用这个前缀
# 用法: <模板内容> + <具体任务描述>

JCODE_TASK_PREFIX = """CRITICAL TOOL USAGE RULES (read carefully before starting):

=== apply_patch FORMAT (MANDATORY) ===
The apply_patch tool REQUIRES the V4A format. Every patch MUST have:
1. A line: *** Update File: <path>  (relative to the crate dir is fine, e.g. src/main.rs)
2. A line: @@  (context marker, can be empty)
3. Hunks with - (remove) and + (add) lines.

EXAMPLE of a correct patch:
*** Begin Patch
*** Update File: src/main.rs
@@
-    drops: u64,
+    drops: u64,
+    sys_start: Instant,
*** End Patch

If you omit the '*** Update File:' line, the tool fails with 'No valid patch directives found'.
NEVER use the write tool to replace an existing file's whole content - that destroys the file. Only apply_patch for edits.

=== bash PATHS (MANDATORY) ===
Use ONLY Windows-style paths in bash:
- cd /d C:\\path\\to\\dir  (note the /d switch for changing drive)
- NEVER use Git-Bash style like cd /d/C/Users/... - it fails.
- cargo build from the crate dir: cd /d C:\\Users\\trade\\mqtt-broker-rs\\mio_broker && cargo build 2>&1 | tail -5

=== WORKFLOW ===
1. read the relevant code section first
2. apply ONE small patch at a time
3. after each patch, run cargo build to verify
4. iterate until it compiles
"""

def make_jcode_prompt(task_body: str) -> str:
    return JCODE_TASK_PREFIX + "\n\n=== THE TASK ===\n" + task_body
