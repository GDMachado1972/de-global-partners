#!/usr/bin/env python3
"""boto3 infra bootstrap for DE Global Partners (SDD §6, §9.3, §10).

Provisions the *non-credential* infrastructure and EMITS (does not create) the pieces
Gerardo owns. Respecting the credential boundary:
  - CREATES: S3 zones (block-public-access, versioning, SSE-KMS, Bronze lifecycle),
    a customer-managed KMS CMK + alias, a Glue security configuration, and an Athena
    workgroup pinned to engine v3.
  - EMITS ONLY (you review & apply): the IAM role/policy JSON for the Glue jobs, and
    the exact `aws secretsmanager` command to create the SQL Server secret. This script
    never creates IAM or writes secret values.

Idempotent (safe to re-run). Use --dry-run to preview with zero AWS calls.

  python infra/bootstrap_infra.py --config config/project_config.yaml --dry-run
  python infra/bootstrap_infra.py --config config/project_config.yaml        # apply
  python infra/bootstrap_infra.py --config config/project_config.yaml --create-iam  # opt-in
"""
from __future__ import annotations
import argparse
import json
import sys

try:
    import yaml
except Exception:
    yaml = None


def load_cfg(path):
    with open(path) as f:
        if yaml:
            return yaml.safe_load(f)
        # minimal fallback parser not implemented; require pyyaml
        raise SystemExit("pyyaml required: pip install pyyaml")


def log(dry, msg):
    print(("[dry-run] would " if dry else "[apply] ") + msg)


# ---------------------------------------------------------------- KMS
def ensure_kms(kms, alias, dry):
    if dry:
        log(dry, f"create KMS CMK + alias {alias}"); return "arn:aws:kms:...:key/DRYRUN"
    aliases = {a["AliasName"]: a for a in kms.list_aliases().get("Aliases", [])}
    if alias in aliases:
        print(f"[skip] KMS alias {alias} exists")
        return aliases[alias].get("TargetKeyId")
    key = kms.create_key(Description="DE Global Partners dev CMK",
                         KeyUsage="ENCRYPT_DECRYPT")["KeyMetadata"]["KeyId"]
    kms.create_alias(AliasName=alias, TargetKeyId=key)
    print(f"[ok] KMS CMK created + alias {alias}")
    return key


# ---------------------------------------------------------------- S3
def ensure_bucket(s3, name, region, kms_alias, dry, bronze_ia_days=None):
    if dry:
        log(dry, f"create S3 bucket {name} (PAB on, versioning, SSE-KMS via {kms_alias}"
                 + (f", Bronze->IA @ {bronze_ia_days}d)" if bronze_ia_days else ")"))
        return
    exists = True
    try:
        s3.head_bucket(Bucket=name)
    except Exception:
        exists = False
    if not exists:
        kw = {} if region == "us-east-1" else {
            "CreateBucketConfiguration": {"LocationConstraint": region}}
        s3.create_bucket(Bucket=name, **kw)
    s3.put_public_access_block(Bucket=name, PublicAccessBlockConfiguration={
        "BlockPublicAcls": True, "IgnorePublicAcls": True,
        "BlockPublicPolicy": True, "RestrictPublicBuckets": True})
    s3.put_bucket_versioning(Bucket=name, VersioningConfiguration={"Status": "Enabled"})
    s3.put_bucket_encryption(Bucket=name, ServerSideEncryptionConfiguration={
        "Rules": [{"ApplyServerSideEncryptionByDefault": {
            "SSEAlgorithm": "aws:kms", "KMSMasterKeyID": kms_alias}}]})
    if bronze_ia_days:
        s3.put_bucket_lifecycle_configuration(Bucket=name, LifecycleConfiguration={
            "Rules": [{"ID": "bronze-to-ia", "Status": "Enabled", "Filter": {"Prefix": ""},
                       "Transitions": [{"Days": bronze_ia_days, "StorageClass": "STANDARD_IA"}]}]})
    print(f"[ok] S3 bucket {name} configured")


# ---------------------------------------------------------------- Glue security config
def ensure_glue_seccfg(glue, name, kms_alias, dry):
    if dry:
        log(dry, f"create Glue security configuration {name} (S3+CloudWatch+bookmarks SSE-KMS)")
        return
    existing = [c["Name"] for c in glue.get_security_configurations().get("SecurityConfigurations", [])]
    if name in existing:
        print(f"[skip] Glue security config {name} exists"); return
    glue.create_security_configuration(Name=name, EncryptionConfiguration={
        "S3Encryption": [{"S3EncryptionMode": "SSE-KMS", "KmsKeyArn": kms_alias}],
        "CloudWatchEncryption": {"CloudWatchEncryptionMode": "SSE-KMS", "KmsKeyArn": kms_alias},
        "JobBookmarksEncryption": {"JobBookmarksEncryptionMode": "CSE-KMS", "KmsKeyArn": kms_alias}})
    print(f"[ok] Glue security config {name} created")


# ---------------------------------------------------------------- Athena workgroup v3
def ensure_athena_wg(athena, wg, results_bucket, kms_alias, engine, dry):
    out = f"s3://{results_bucket}/athena/"
    if dry:
        log(dry, f"create Athena workgroup {wg} ({engine}, output {out}, SSE-KMS)"); return
    existing = [w["Name"] for w in athena.list_work_groups().get("WorkGroups", [])]
    conf = {"ResultConfiguration": {"OutputLocation": out,
                "EncryptionConfiguration": {"EncryptionOption": "SSE_KMS", "KmsKey": kms_alias}},
            "EnforceWorkGroupConfiguration": True,
            "PublishCloudWatchMetricsEnabled": True,
            "EngineVersion": {"SelectedEngineVersion": engine}}
    if wg in existing:
        athena.update_work_group(WorkGroup=wg, ConfigurationUpdates=conf)
        print(f"[ok] Athena workgroup {wg} updated ({engine})")
    else:
        athena.create_work_group(Name=wg, Configuration=conf)
        print(f"[ok] Athena workgroup {wg} created ({engine})")


# ---------------------------------------------------------------- IAM + Secrets (emit)
def emit_iam_policy(cfg, out_path):
    b = cfg["buckets"]
    arns = []
    for k in ("bronze", "silver", "gold", "athena_results", "scripts", "logs"):
        arns += [f"arn:aws:s3:::{b[k]}", f"arn:aws:s3:::{b[k]}/*"]
    policy = {"Version": "2012-10-17", "Statement": [
        {"Sid": "S3Zones", "Effect": "Allow",
         "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
         "Resource": arns},
        {"Sid": "KMS", "Effect": "Allow",
         "Action": ["kms:Decrypt", "kms:GenerateDataKey", "kms:DescribeKey"],
         "Resource": "*", "Condition": {"StringEquals": {
             "kms:ViaService": f"s3.{cfg['region']}.amazonaws.com"}}},
        {"Sid": "SecretsRead", "Effect": "Allow", "Action": ["secretsmanager:GetSecretValue"],
         "Resource": f"arn:aws:secretsmanager:{cfg['region']}:*:secret:{cfg['secrets']['sqlserver']}*"},
        {"Sid": "GlueAthenaCW", "Effect": "Allow", "Action": [
            "glue:*", "athena:*", "logs:CreateLogGroup", "logs:CreateLogStream",
            "logs:PutLogEvents", "cloudwatch:PutMetricData", "sns:Publish"],
         "Resource": "*"}]}
    with open(out_path, "w") as f:
        json.dump(policy, f, indent=2)
    print(f"[emit] Glue job IAM policy written to {out_path}")
    print("       -> Gerardo: review, then attach to a role trusting glue.amazonaws.com.")


def emit_secret_instructions(cfg):
    name = cfg["secrets"]["sqlserver"]
    print("\n[emit] Secrets Manager — create & populate yourself (credential boundary):")
    print(f'  aws secretsmanager create-secret --name "{name}" \\')
    print(f'    --secret-string \'{{"username":"<USER>","password":"<PASS>",'
          f'"host":"<HOST>","port":"1433","database":"<DB>"}}\' \\')
    print(f'    --region {cfg["region"]}')
    print("  (This script never sends secret values.)")


def main(argv):
    p = argparse.ArgumentParser()
    p.add_argument("--config", required=True)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--create-iam", action="store_true",
                   help="(not implemented on purpose) IAM is owned by Gerardo; policy is emitted only")
    p.add_argument("--iam-out", default="infra/glue_job_policy.json")
    args = p.parse_args(argv)
    cfg = load_cfg(args.config)
    region, dry = cfg["region"], args.dry_run
    kms_alias = cfg["kms"]["alias"]

    print(f"=== DE Global Partners infra bootstrap ({'DRY-RUN' if dry else 'APPLY'}) "
          f"region={region} ===")
    if dry:
        s3 = kms = glue = athena = None
    else:
        import boto3
        s3 = boto3.client("s3", region_name=region)
        kms = boto3.client("kms", region_name=region)
        glue = boto3.client("glue", region_name=region)
        athena = boto3.client("athena", region_name=region)

    ensure_kms(kms, kms_alias, dry)
    b = cfg["buckets"]
    ia = cfg["lifecycle"]["bronze_ia_days"]
    for key, name in b.items():
        ensure_bucket(s3, name, region, kms_alias, dry,
                      bronze_ia_days=ia if key == "bronze" else None)
    ensure_glue_seccfg(glue, cfg["glue"]["security_config"], kms_alias, dry)
    ensure_athena_wg(athena, cfg["athena"]["workgroup"], b["athena_results"],
                     kms_alias, cfg["athena"]["engine_version"], dry)

    # always emit the owner-controlled pieces
    emit_iam_policy(cfg, args.iam_out)
    emit_secret_instructions(cfg)
    print("\nDone. Next: create the Glue connection to SQL Server and populate the secret,"
          " then run the Glue Workflow (see glue/workflow/).")


if __name__ == "__main__":
    main(sys.argv[1:])
