#!/usr/bin/env python3
"""Glue Workflow wiring for DE Global Partners (SDD §9.1, §9.4).

DAG
        schedule (cron 06:00 UTC)
                |
             [Job 1] ingest_sqlserver_to_bronze
                | SUCCEEDED
             [Job 2] bronze_to_silver
                | SUCCEEDED
        +-------+-------+
        |               |            (parallel)
     [Job 3]         [Job 4]
   clv_daily         marts
        |               |
        +---- AND ------+  (both SUCCEEDED)
                |
             [Job 5] publish_and_qc

Failure branch (SDD §9.4): an EventBridge rule on "Glue Job State Change" with state in
{FAILED, TIMEOUT, STOPPED} for any of the five jobs -> SNS topic (alerts). Reload is a
re-run of the workflow with run property batch_date=<date>; jobs overwrite only that
partition (idempotent), so reprocessing is safe.

This script separates a PURE, unit-tested spec (build_workflow_spec) from the boto3 apply
(apply_spec). Credential boundary: the Glue execution Role ARN is supplied by Gerardo via
--role-arn (his IAM); this script never creates IAM or handles secrets.

  python glue/workflow/create_workflow.py --config config/project_config.yaml --dry-run
  python glue/workflow/create_workflow.py --config config/project_config.yaml \
         --role-arn arn:aws:iam::<acct>:role/global-partners-dev-glue [--alert-email you@x.com]
  python glue/workflow/create_workflow.py --config config/project_config.yaml \
         --role-arn ... --reload 2024-01-15        # reprocess one batch_date
"""
from __future__ import annotations
import argparse
import json
import sys

try:
    import yaml
except Exception:
    yaml = None

FAIL_STATES = ["FAILED", "TIMEOUT", "STOPPED"]


def load_cfg(path):
    if not yaml:
        raise SystemExit("pyyaml required: pip install pyyaml")
    with open(path) as f:
        return yaml.safe_load(f)


# ------------------------------------------------------------------ pure spec
def build_workflow_spec(cfg, role_arn="<ROLE_ARN>"):
    """Return the full workflow topology as plain dicts (no AWS calls)."""
    g = cfg["glue"]
    j = cfg["jobs"]
    scripts_bucket = cfg["buckets"]["scripts"]
    wf = g["workflow"]
    script_uri = lambda f: f"s3://{scripts_bucket}/{cfg['workflow_scripts_prefix']}/{f}"
    lib_zip = f"s3://{scripts_bucket}/{cfg['workflow_lib_zip']}"
    temp_dir = f"s3://{scripts_bucket}/tmp/"

    common_args = {
        "--job-bookmark-option": "job-bookmark-enable",   # incremental (SDD §9.2)
        "--enable-metrics": "true",
        "--enable-continuous-cloudwatch-log": "true",
        "--datalake-formats": "delta",                    # Delta on Glue 4.0
        "--extra-py-files": lib_zip,                       # ship glue/lib
        "--TempDir": temp_dir,
        "--config": f"s3://{scripts_bucket}/config/project_config.yaml",
    }

    def job(key):
        m = j[key]
        return {
            "Name": m["name"],
            "Role": role_arn,
            "GlueVersion": g["glue_version"],
            "WorkerType": g["worker_type"],
            "NumberOfWorkers": g["num_workers"],
            "SecurityConfiguration": g["security_config"],
            "ExecutionProperty": {"MaxConcurrentRuns": 3},   # allow reloads alongside daily
            "Command": {"Name": "glueetl", "PythonVersion": "3",
                        "ScriptLocation": script_uri(m["script"])},
            "DefaultArguments": common_args,
        }

    jobs = {k: job(k) for k in
            ["ingest_bronze", "bronze_silver", "gold_clv_daily", "gold_marts", "publish_qc"]}
    N = {k: j[k]["name"] for k in j}   # short handle -> full job name

    def cond(job_name, state="SUCCEEDED"):
        return {"LogicalOperator": "EQUALS", "JobName": job_name, "State": state}

    triggers = [
        {"Name": f"{wf}-t0-schedule", "Type": "SCHEDULED", "WorkflowName": wf,
         "Schedule": g["schedule_cron"], "StartOnCreation": True,
         "Actions": [{"JobName": N["ingest_bronze"]}]},
        {"Name": f"{wf}-t1-bronze-to-silver", "Type": "CONDITIONAL", "WorkflowName": wf,
         "Predicate": {"Logical": "ANY", "Conditions": [cond(N["ingest_bronze"])]},
         "Actions": [{"JobName": N["bronze_silver"]}], "StartOnCreation": True},
        {"Name": f"{wf}-t2-silver-to-gold", "Type": "CONDITIONAL", "WorkflowName": wf,
         "Predicate": {"Logical": "ANY", "Conditions": [cond(N["bronze_silver"])]},
         "Actions": [{"JobName": N["gold_clv_daily"]}, {"JobName": N["gold_marts"]}],  # parallel
         "StartOnCreation": True},
        {"Name": f"{wf}-t3-publish", "Type": "CONDITIONAL", "WorkflowName": wf,
         "Predicate": {"Logical": "AND",
                       "Conditions": [cond(N["gold_clv_daily"]), cond(N["gold_marts"])]},
         "Actions": [{"JobName": N["publish_qc"]}], "StartOnCreation": True},
    ]

    failure_rule = {
        "Name": f"{wf}-on-failure",
        "EventPattern": {
            "source": ["aws.glue"],
            "detail-type": ["Glue Job State Change"],
            "detail": {"jobName": list(N.values()), "state": FAIL_STATES},
        },
    }

    workflow = {"Name": wf,
                "Description": "DE Global Partners daily CLV pipeline (SDD §9.1)",
                "DefaultRunProperties": {"batch_date": cfg["workflow_run_properties"]["batch_date"]}}

    return {"workflow": workflow, "jobs": jobs, "triggers": triggers,
            "failure_rule": failure_rule, "sns_topic": cfg["sns"]["topic"]}


# ------------------------------------------------------------------ apply
def _exists(fn, **kw):
    try:
        fn(**kw); return True
    except Exception:
        return False


def apply_spec(spec, cfg, role_arn, alert_email, dry):
    region = cfg["region"]
    tag = "[dry-run] would " if dry else "[apply] "
    if not dry:
        import boto3
        glue = boto3.client("glue", region_name=region)
        sns = boto3.client("sns", region_name=region)
        events = boto3.client("events", region_name=region)

    # 1) SNS topic (+ optional email subscription — Gerardo confirms via email)
    topic = spec["sns_topic"]
    print(f"{tag}ensure SNS topic {topic}")
    topic_arn = f"arn:aws:sns:{region}:<acct>:{topic}"
    if not dry:
        topic_arn = sns.create_topic(Name=topic)["TopicArn"]
        if alert_email:
            sns.subscribe(TopicArn=topic_arn, Protocol="email", Endpoint=alert_email)
            print(f"  -> subscription requested for {alert_email} (confirm via email)")

    # 2) Workflow
    wf = spec["workflow"]
    print(f"{tag}ensure Glue workflow {wf['Name']}")
    if not dry:
        if _exists(glue.get_workflow, Name=wf["Name"]):
            glue.update_workflow(Name=wf["Name"], Description=wf["Description"],
                                 DefaultRunProperties=wf["DefaultRunProperties"])
        else:
            glue.create_workflow(**wf)

    # 3) Jobs
    for key, jd in spec["jobs"].items():
        print(f"{tag}ensure Glue job {jd['Name']}")
        if not dry:
            if _exists(glue.get_job, JobName=jd["Name"]):
                params = {k: v for k, v in jd.items() if k != "Name"}
                glue.update_job(JobName=jd["Name"], JobUpdate=params)
            else:
                glue.create_job(**jd)

    # 4) Triggers
    for tr in spec["triggers"]:
        print(f"{tag}ensure trigger {tr['Name']} ({tr['Type']})")
        if not dry:
            if _exists(glue.get_trigger, Name=tr["Name"]):
                upd = {"Actions": tr["Actions"]}
                if "Predicate" in tr: upd["Predicate"] = tr["Predicate"]
                if "Schedule" in tr: upd["Schedule"] = tr["Schedule"]
                glue.update_trigger(Name=tr["Name"], TriggerUpdate=upd)
            else:
                glue.create_trigger(**tr)

    # 5) Failure branch: EventBridge rule -> SNS
    fr = spec["failure_rule"]
    print(f"{tag}ensure EventBridge failure rule {fr['Name']} -> SNS {topic} "
          f"(states {'/'.join(FAIL_STATES)})")
    if not dry:
        events.put_rule(Name=fr["Name"], EventPattern=json.dumps(fr["EventPattern"]),
                        State="ENABLED")
        # allow EventBridge to publish to the topic
        sns.set_topic_attributes(TopicArn=topic_arn, AttributeName="Policy",
            AttributeValue=json.dumps({"Version": "2012-10-17", "Statement": [{
                "Effect": "Allow", "Principal": {"Service": "events.amazonaws.com"},
                "Action": "sns:Publish", "Resource": topic_arn}]}))
        events.put_targets(Rule=fr["Name"],
                           Targets=[{"Id": "sns", "Arn": topic_arn}])
    print(f"{tag}ATTACH failure alerting complete")


def reload_run(cfg, role_arn, batch_date, dry):
    """Reprocess a single batch_date: set run property + start the workflow."""
    wf = cfg["glue"]["workflow"]
    print(f"[reload] batch_date={batch_date} on workflow {wf}")
    if dry:
        print("  [dry-run] would update DefaultRunProperties.batch_date then start_workflow_run")
        return
    import boto3
    glue = boto3.client("glue", region_name=cfg["region"])
    glue.update_workflow(Name=wf, DefaultRunProperties={"batch_date": batch_date})
    run_id = glue.start_workflow_run(Name=wf)["RunId"]
    print(f"  started workflow run {run_id} (idempotent per-partition overwrite)")


def main(argv):
    p = argparse.ArgumentParser()
    p.add_argument("--config", required=True)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--role-arn", default=None, help="Glue execution role ARN (Gerardo's IAM)")
    p.add_argument("--alert-email", default=None, help="optional SNS email subscription")
    p.add_argument("--reload", default=None, metavar="YYYY-MM-DD",
                   help="reprocess a single batch_date")
    a = p.parse_args(argv)
    cfg = load_cfg(a.config)

    if a.reload:
        if not a.dry_run and not a.role_arn:
            raise SystemExit("--role-arn required for a live reload")
        return reload_run(cfg, a.role_arn, a.reload, a.dry_run)

    if not a.dry_run and not a.role_arn:
        raise SystemExit("--role-arn required for apply (Gerardo owns IAM). Use --dry-run to preview.")

    role = a.role_arn or "<ROLE_ARN>"
    spec = build_workflow_spec(cfg, role_arn=role)
    print(f"=== Glue Workflow wiring ({'DRY-RUN' if a.dry_run else 'APPLY'}) "
          f"workflow={spec['workflow']['Name']} region={cfg['region']} ===")
    apply_spec(spec, cfg, role, a.alert_email, a.dry_run)
    print("\nDAG: schedule -> J1 -> J2 -> {J3 || J4} -> J5 ; any FAILED/TIMEOUT/STOPPED -> SNS")
    print("Reload:  --reload YYYY-MM-DD  (idempotent per-partition overwrite)")


if __name__ == "__main__":
    main(sys.argv[1:])
