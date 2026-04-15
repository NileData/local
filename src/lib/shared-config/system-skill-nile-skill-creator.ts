import type { SkillDefinition } from "./system-skills.js";

const NILE_SKILL_REQUIREMENTS_AUDITOR_AGENT = `---
name: nile-skill-requirements-auditor
description: Use proactively for the first stage of Nile skill creation. Normalize the request into hard requirements, required files, required Nile tools, mandatory validations, approval gates, and required custom agents.
model: sonnet
---

You handle only the requirements audit stage for Nile skill creation.

- Inputs:
  - the user's requested Nile skill behavior
  - any existing skill files, scripts, references, or agents that may be reused
- Inspect the target workflow and identify:
  - trigger conditions and a precise skill description
  - whether the task is complex enough to require subagents
  - which \`.claude/agents/*.md\` files are required
  - which bundled \`scripts/\`, \`references/\`, or other assets are required
  - which existing Nile tools must be reused instead of approximated
  - mandatory validation steps, approval gates, and clarification points
  - forbidden fallbacks and anti-patterns
- Treat any new Nile skill or any multi-file skill update as a complex task unless it is strictly a one-file wording correction.
- Never replace required subagent delegation with a checklist, todo list, or plan-only response.
- For data workflow skills, enforce these rules in the audit output:
  - Parquet is the authoritative import artifact.
  - CSV is only for QA, ad hoc inspection, and debugging.
  - Never use \`import_from_paste\` as a substitute for \`import_from_s3\`.
  - Never substitute a direct inline payload import for the S3 flow.
  - Required table-existence checks, approval gates, missing-credential blockers, and prefix-collision handling must be explicit.
- Return a concise build spec the parent can act on immediately:
  - skill name and description
  - required files to create or update
  - required agents
  - required validations and approval gates
  - blockers or missing inputs
- Do not create files in this stage.
- If blocked, return the exact missing inputs or changed parameters needed for a follow-up call so the parent can re-run this subagent once instead of solving the issue inline.
`;

const NILE_SKILL_ASSET_BUILDER_AGENT = `---
name: nile-skill-asset-builder
description: Use proactively for the build stage of Nile skill creation. Create or update the skill files, bundled scripts, references, and any required custom agents so the workflow is executable as-written.
model: sonnet
---

You handle only the build stage for Nile skill creation.

- Inputs:
  - the approved build spec from the requirements audit
  - the target workspace and files to create or update
- Complete the file work inside this subagent. Do not return a sketch, checklist, or partial outline for the parent to finish.
- Create or update the required files directly:
  - \`SKILL.md\`
  - bundled \`scripts/\`, \`references/\`, or other resources
  - required \`.claude/agents/*.md\` files when the target skill depends on them
- Prefer Python scripts for repeated or brittle tasks instead of prompt-only approximations.
- Reuse existing Nile tools, internal catalog tools, external catalog tools, and other platform tools whenever the target skill needs them.
- For S3 import helpers, start from \`.claude/skills/nile-skill-creator/scripts/upload_tables_to_s3.py\` unless the target workflow already has a better repo-local equivalent.
- For data workflow skills, keep these rules in the generated files:
  - Parquet is required for \`import_from_s3\` unless the user explicitly overrides it.
  - CSV mirrors are QA-only.
  - Never use \`import_from_paste\`.
  - Never use inline payload imports as a substitute for the S3 flow.
  - If target tables already exist, do not overwrite, append, or auto-suffix; ask for a new prefix.
  - If bucket, prefix, or AWS credentials are missing, stop and report the blocker.
- If you encounter a blocker that prevents file completion, return the exact changed parameters, missing paths, or missing inputs needed for a follow-up call so the parent can call this subagent again once.
`;

const NILE_SKILL_REVIEWER_AGENT = `---
name: nile-skill-reviewer
description: Use proactively for the final review stage of Nile skill creation. Verify that the generated skill uses mandatory Agent delegation, preserves Nile import contracts, includes validations and approvals, and does not regress into checklist-only behavior.
model: sonnet
---

You handle only the review stage for Nile skill creation.

- Inputs:
  - the generated skill files
  - any bundled scripts or custom agent files created for the skill
- Review the actual files, not a summary.
- Verify:
  - required \`Agent(...)\` delegation is present for complex tasks
  - required custom agents exist under \`.claude/agents/\`
  - the skill does not substitute a todo list, status checklist, or plan-only response for real subagent launches
  - a one-line pre-launch note is required before each subagent launch
  - data workflow skills keep Parquet-first S3 import rules and never fall back to \`import_from_paste\`
  - validation, approval, clarification, and collision checks are explicit
  - existing Nile tools are reused where appropriate
- If you find a fixable issue inside your scope, patch it directly instead of handing the work back unfinished.
- If you still cannot complete review because a file is missing or inputs are inconsistent, return the exact changed parameters or missing files needed for a follow-up review call.
- Return concise findings and a go/no-go result.
`;

const NILE_SKILL_UPLOAD_HELPER = String.raw`#!/usr/bin/env python3
"""Upload local table artifacts to S3 using boto3 first, then AWS CLI.

This helper is intended as a generic Nile import contract reference:
- Prefer Parquet for Nile import_from_s3 flows.
- CSV is allowed for QA/debug mirrors, but should not be the default import artifact.
- Return exact s3:// URIs so they can be passed directly into Nile import flows.
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

SUPPORTED_SUFFIXES = {".parquet", ".csv"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", required=True, help="Destination S3 bucket")
    parser.add_argument("--prefix", required=True, help="Destination S3 key prefix")
    parser.add_argument(
        "artifacts",
        nargs="+",
        help="One or more local table artifact files to upload",
    )
    return parser.parse_args()


def content_type_for_suffix(path: Path) -> str:
    if path.suffix.lower() == ".parquet":
        return "application/vnd.apache.parquet"
    return "text/csv"


def normalize_artifacts(paths: list[str]) -> list[Path]:
    artifacts = [Path(raw).resolve() for raw in paths]

    missing = [str(path) for path in artifacts if not path.is_file()]
    if missing:
        raise SystemExit("Missing files: " + ", ".join(missing))

    unsupported = [
        str(path) for path in artifacts if path.suffix.lower() not in SUPPORTED_SUFFIXES
    ]
    if unsupported:
        raise SystemExit(
            "Unsupported file types: "
            + ", ".join(unsupported)
            + ". Supported suffixes: .parquet, .csv"
        )

    return artifacts


def build_targets(artifacts: list[Path], bucket: str, prefix: str) -> list[dict[str, str]]:
    normalized_prefix = prefix.strip("/")
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    targets: list[dict[str, str]] = []

    for artifact in artifacts:
        key = f"{normalized_prefix}/{artifact.stem}_{timestamp}{artifact.suffix.lower()}"
        targets.append(
            {
                "local_path": str(artifact),
                "filename": artifact.name,
                "content_type": content_type_for_suffix(artifact),
                "s3_key": key,
                "s3_uri": f"s3://{bucket}/{key}",
            }
        )

    return targets


def upload_with_boto3(targets: list[dict[str, str]], bucket: str) -> dict[str, object]:
    try:
        import boto3
    except ImportError as exc:  # pragma: no cover - depends on runtime
        raise RuntimeError("boto3 is not installed") from exc

    client = boto3.client("s3")
    for target in targets:
        client.upload_file(
            target["local_path"],
            bucket,
            target["s3_key"],
            ExtraArgs={"ContentType": target["content_type"]},
        )

    return {"upload_method": "boto3", "artifacts": targets}


def upload_with_aws_cli(targets: list[dict[str, str]]) -> dict[str, object]:
    if shutil.which("aws") is None:
        raise RuntimeError("aws CLI not found in PATH")

    for target in targets:
        subprocess.run(
            [
                "aws",
                "s3",
                "cp",
                target["local_path"],
                target["s3_uri"],
                "--content-type",
                target["content_type"],
            ],
            check=True,
            capture_output=True,
            text=True,
        )

    return {"upload_method": "aws-cli", "artifacts": targets}


def main() -> int:
    args = parse_args()
    artifacts = normalize_artifacts(args.artifacts)
    targets = build_targets(artifacts, args.bucket, args.prefix)

    errors: list[str] = []
    for uploader in (
        lambda: upload_with_boto3(targets, args.bucket),
        lambda: upload_with_aws_cli(targets),
    ):
        try:
            result = uploader()
        except Exception as exc:  # pragma: no cover - depends on runtime
            errors.append(str(exc))
            continue

        payload = {
            "bucket": args.bucket,
            "prefix": args.prefix.strip("/"),
            **result,
        }
        json.dump(payload, sys.stdout, indent=2)
        sys.stdout.write("\n")
        return 0

    raise SystemExit(
        "S3 upload failed for both boto3 and AWS CLI. " + " | ".join(errors)
    )


if __name__ == "__main__":
    raise SystemExit(main())
`;

const NILE_SKILL_UPLOAD_WRAPPER = `#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "\${BASH_SOURCE[0]}")" && pwd)
exec python3 "\${script_dir}/upload_tables_to_s3.py" "$@"
`;

export const SYSTEM_NILE_SKILL_CREATOR_SKILL: SkillDefinition = {
  name: "nile-skill-creator",
  description: "Create, tighten, or promote Nile App skills. Use when the user asks for a new skill, a SKILL.md workflow, a built-in/system skill, or a complex Nile data workflow skill that needs Agent-based delegation, bundled scripts, validations, approvals, or Parquet/S3 import rules.",
  modes: ["local"],
  content: `# Nile Skill Creator

Use this skill when the user asks to create, revise, tighten, promote, or debug a Nile App skill, especially when the target skill is multi-stage, relies on Nile tools, or needs bundled scripts, references, or custom subagents.

## Core Rules

- Use real Claude subagents via \`Agent(...)\`, not a todo list, checklist, or plan-only response.
- Treat subagent delegation as mandatory for complex work.
- Any new Nile skill or any multi-file skill update counts as complex unless it is strictly a one-file wording correction.
- A visible progress summary is optional, but it never replaces the subagent itself.
- Before each subagent launch, send the user exactly one short sentence describing the next task. Keep it to a single line.
- If the required custom subagents are not installed under \`.claude/agents/\`, stop and report the missing paths as a blocker. Do not approximate the workflow with a checklist.
- Subagents should complete their own stage. They should not bounce work back to the parent as a vague error. If a retry is needed, they must say exactly how the parent should call them again with changed inputs or parameters.

## Required Custom Subagents

This skill requires these files to exist:

- \`.claude/agents/nile-skill-requirements-auditor.md\`
- \`.claude/agents/nile-skill-asset-builder.md\`
- \`.claude/agents/nile-skill-reviewer.md\`

If any are missing, stop immediately and report the blocker.

## Required Workflow

### 1. Audit requirements

Launch \`Agent("nile-skill-requirements-auditor", ...)\` first.

Required result:
- exact skill name and description
- required files to create or update
- required custom agents
- required validations, approvals, and clarifications
- required Nile tools to reuse
- blockers and forbidden fallbacks

### 2. Build the skill and resources

Launch \`Agent("nile-skill-asset-builder", ...)\` after the audit.

Use it to create or update the actual files:
- \`SKILL.md\`
- bundled \`scripts/\`, \`references/\`, or other resources
- any required \`.claude/agents/*.md\` definitions for the target workflow

Do not leave file creation as an inline parent-thread checklist.

### 3. Review before finishing

Launch \`Agent("nile-skill-reviewer", ...)\` before returning the final answer.

The review must confirm that:
- mandatory subagent delegation is preserved
- required agents exist
- validations and approval gates are explicit
- forbidden import fallbacks are absent
- the created skill reuses Nile tools instead of inventing prompt-only approximations

## Skill Authoring Requirements

- Prefer existing Nile tools, including internal catalog tools, external catalog tools, import tools, and other platform tools.
- Write Python scripts for repeated or brittle tasks and run them instead of approximating them with prompts.
- Bundle concrete helpers and references when the workflow would otherwise become vague or repetitive.
- Keep \`SKILL.md\` focused on workflow rules and decision points; move repeated code or contracts into bundled files.

## Data Workflow Rules

When the target skill creates or operates a Nile data workflow, these rules are mandatory:

- Parquet is the authoritative table artifact for imports.
- CSV is only a strictly quoted mirror for manual QA, ad hoc inspection, and debugging.
- Never substitute \`import_from_paste\` for \`import_from_s3\`.
- Never substitute a direct inline payload import for the S3 flow.
- Parquet is the required S3 import artifact unless the user explicitly overrides that rule.
- If bucket, prefix, or AWS credentials are missing, stop and report the blocker instead of choosing another import path.
- If any of the target tables already exists, do not overwrite, append, or auto-generate a suffix. Ask the user for a new prefix.
- When asking for a prefix, include the conflicting table names and explain that the new prefix will be prepended consistently across all related tables.
- Add validation, clarification, and approval gates where the workflow can fail or produce bad data.
- Surface common error patterns to the user instead of silently continuing.

## Required S3 Upload Contract

Use [scripts/upload_tables_to_s3.py](scripts/upload_tables_to_s3.py) first when an S3 upload is required.

- The Python uploader uploads Parquet or CSV files, but Parquet should be used for imports.
- Prefer \`boto3\` instead of AWS CLI.
- Do not return early just because AWS CLI is unavailable; the Python uploader tries \`boto3\` first and only falls back to AWS CLI when needed.
- [scripts/upload_tables_to_s3.sh](scripts/upload_tables_to_s3.sh) is the thin shell wrapper for shell-driven workflows.
- After upload, pass the returned exact \`s3://...\` URIs into Nile \`import_from_s3\`.

## Output Contract

When you finish, return:

- the skill files that were created or updated
- any bundled scripts, references, or agents that were added
- any remaining blockers or required user input
- a short note describing how the generated skill is expected to trigger and what it specializes in
`,
  files: [
    {
      path: "scripts/upload_tables_to_s3.py",
      content: NILE_SKILL_UPLOAD_HELPER,
    },
    {
      path: "scripts/upload_tables_to_s3.sh",
      content: NILE_SKILL_UPLOAD_WRAPPER,
    },
  ],
  agents: [
    {
      name: "nile-skill-requirements-auditor",
      content: NILE_SKILL_REQUIREMENTS_AUDITOR_AGENT,
    },
    {
      name: "nile-skill-asset-builder",
      content: NILE_SKILL_ASSET_BUILDER_AGENT,
    },
    {
      name: "nile-skill-reviewer",
      content: NILE_SKILL_REVIEWER_AGENT,
    },
  ],
};
