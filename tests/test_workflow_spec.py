"""Workflow-topology tests (SDD §9.1). Pure spec — no AWS calls, always CI-safe."""
import yaml
from glue.workflow.create_workflow import build_workflow_spec, FAIL_STATES


def _cfg():
    with open("config/project_config.yaml") as f:
        return yaml.safe_load(f)


def _trig(spec, needle):
    return next(t for t in spec["triggers"] if needle in t["Name"])


def test_schedule_starts_job1():
    spec = build_workflow_spec(_cfg())
    t = _trig(spec, "schedule")
    assert t["Type"] == "SCHEDULED"
    assert t["Schedule"].startswith("cron(")
    assert t["Actions"][0]["JobName"].endswith("ingest-bronze")


def test_linear_bronze_then_silver():
    spec = build_workflow_spec(_cfg())
    t = _trig(spec, "bronze-to-silver")
    conds = t["Predicate"]["Conditions"]
    assert conds[0]["JobName"].endswith("ingest-bronze")
    assert conds[0]["State"] == "SUCCEEDED"
    assert t["Actions"][0]["JobName"].endswith("bronze-silver")


def test_silver_fans_out_to_two_gold_jobs_in_parallel():
    spec = build_workflow_spec(_cfg())
    t = _trig(spec, "silver-to-gold")
    names = {a["JobName"] for a in t["Actions"]}
    assert any(n.endswith("gold-clv-daily") for n in names)
    assert any(n.endswith("gold-marts") for n in names)
    assert len(t["Actions"]) == 2                       # parallel fan-out


def test_publish_waits_for_both_gold_jobs():
    spec = build_workflow_spec(_cfg())
    t = _trig(spec, "publish")
    assert t["Predicate"]["Logical"] == "AND"           # both must succeed
    watched = {c["JobName"] for c in t["Predicate"]["Conditions"]}
    assert any(n.endswith("gold-clv-daily") for n in watched)
    assert any(n.endswith("gold-marts") for n in watched)
    assert t["Actions"][0]["JobName"].endswith("publish-qc")


def test_failure_branch_covers_all_five_jobs():
    spec = build_workflow_spec(_cfg())
    fr = spec["failure_rule"]
    assert fr["EventPattern"]["detail"]["state"] == FAIL_STATES
    assert len(fr["EventPattern"]["detail"]["jobName"]) == 5   # J1..J5
    assert fr["EventPattern"]["source"] == ["aws.glue"]


def test_all_five_jobs_defined_with_security_and_delta():
    spec = build_workflow_spec(_cfg())
    assert len(spec["jobs"]) == 5
    for jd in spec["jobs"].values():
        assert jd["SecurityConfiguration"]                     # encryption
        assert jd["DefaultArguments"]["--datalake-formats"] == "delta"
        assert jd["DefaultArguments"]["--job-bookmark-option"] == "job-bookmark-enable"
        assert jd["Command"]["ScriptLocation"].startswith("s3://")
