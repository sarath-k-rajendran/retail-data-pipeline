"""Glue Workflow run-properties handoff.

When a job is started as part of a Glue Workflow trigger, Glue auto-injects
--WORKFLOW_NAME and --WORKFLOW_RUN_ID into its arguments. We use the
workflow's run-properties bag (a small key/value store scoped to one
workflow execution) to pass batch_id/load_date/load_mode from Bronze
downstream to Silver and Gold, so they always process the exact batch Bronze
just landed instead of needing that state re-typed as a job parameter for
every stage.

For standalone ad-hoc "Run job" testing outside a workflow (no
WORKFLOW_NAME/WORKFLOW_RUN_ID present), callers fall back to explicit
--batch_id/--load_date job parameters -- see each job's docstring.
"""
from __future__ import annotations

import boto3


def get_workflow_context(args: dict) -> dict | None:
    name = args.get("WORKFLOW_NAME")
    run_id = args.get("WORKFLOW_RUN_ID")
    if name and run_id:
        return {"workflow_name": name, "run_id": run_id}
    return None


def put_run_properties(workflow_name: str, run_id: str, properties: dict) -> None:
    client = boto3.client("glue")
    client.put_workflow_run_properties(
        Name=workflow_name, RunId=run_id,
        RunProperties={k: str(v) for k, v in properties.items() if v is not None},
    )


def get_run_properties(workflow_name: str, run_id: str) -> dict:
    client = boto3.client("glue")
    resp = client.get_workflow_run_properties(Name=workflow_name, RunId=run_id)
    return resp.get("RunProperties", {})


def get_run_property(run_properties: dict, key: str) -> str:
    """Looks up a value by `key`, tolerating either `key` or `--key` as the
    stored property name. Bronze/Silver/Gold's own internal handoff always
    writes keys WITHOUT a leading '--' (see put_run_properties above), but a
    human setting a Workflow's "Run properties" by hand in the Console (a
    freeform Key/Value table, found at Workflows > select workflow > Edit
    workflow > Run properties -- seeds the properties bag every run of that
    workflow starts with) will naturally type the key WITH '--', matching
    every Job Parameter field elsewhere in the Console. Both are accepted."""
    return run_properties.get(key) or run_properties.get(f"--{key}") or ""
