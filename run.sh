#!/usr/bin/env bash
# Thin wrapper so every invocation uses the same interpreter and environment.
# JAVA_TOOL_OPTIONS is unset only to keep the proxy banner out of the logs;
# it has no effect on the computation.
set -u
cd "$(dirname "$0")"
unset JAVA_TOOL_OPTIONS
export JAVA_HOME="$(dirname "$(dirname "$(readlink -f "$(which java)")")")"
export PYSPARK_PYTHON=/home/claude/sparkenv/bin/python
exec /home/claude/sparkenv/bin/python "$@"
