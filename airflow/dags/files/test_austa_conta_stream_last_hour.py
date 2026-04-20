#!/usr/bin/env python3
"""
Teste manual (boto3 + opcional SSH para EC2 Spark):

1) Inspeciona S3 em raw/.../austa.TASY.AUSTA_CONTA/ e aplica o mesmo criterio que
   --partition-from-latest-avro (AVRO com maior LastModified; hora na key).

2) Calcula heuristica: com target 128 MB, quantos ficheiros de saida se esperam
   (~ceil(total_bytes / 128MiB)); se tens dezenas de ficheiros ~10–30 MB, o prefixo
   provavelmente ainda nao foi compactado.

3) Opcional: execucao remota como a DAG Airflow (ssh … spark-submit …), com saida
   em stdout para veres compact_meta / compact_summary no ecra.

4) Opcional: preflight SSH — verifica se o script existe no Spark e se o SHA-256
   bate com o raw_avro_compaction_job.py ao lado deste ficheiro no repo (deploy).

Variaveis uteis: SPARK_HOST (default 177.71.255.159), SPARK_SSH_KEY_PATH, SPARK_MASTER_URL.
PEM local default: ~/.ssh/dlk-austa-sa.pem (sobrescreve com env ou --ssh-key).

Exemplos:
  python3 test_austa_conta_stream_last_hour.py
  python3 test_austa_conta_stream_last_hour.py --preflight-ssh
  python3 test_austa_conta_stream_last_hour.py --remote-spark-ssh
  python3 test_austa_conta_stream_last_hour.py --remote-spark-ssh --compare-after
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import shlex
import subprocess
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import boto3

DEFAULT_BUCKET = "austa-lakehouse-prod-data-lake-169446931765"
DEFAULT_TOPIC_PREFIX = "raw/raw-tasy/stream/austa.TASY.AUSTA_CONTA/"
STREAM_INPUT_PREFIX = "raw/raw-tasy/stream/"
ONLY_TOPIC_SUBSTRING = "austa.TASY.AUSTA_CONTA"
TARGET_MB_DEFAULT = 128
# PEM local por defeito: ~/.ssh/dlk-austa-sa.pem (ex.: C:\\Users\\Rafael\\.ssh\\... no Windows)
_DEFAULT_SSH_KEY = os.path.normpath(
    os.path.join(os.path.expanduser("~"), ".ssh", "dlk-austa-sa.pem")
)

# Particao tipo Hive: .../year=YYYY/month=MM/day=DD/hour=HH/arquivo.avro
_RE_HIVE_HOUR = re.compile(
    r"^(.+/year=\d{4}/month=\d{2}/day=\d{2}/hour=\d{2})/[^/]+\.avro$"
)
# Particao plana: .../YYYY/MM/DD/HH/arquivo.avro
_RE_FLAT_HOUR = re.compile(r"^(.+/\d{4}/\d{2}/\d{2}/\d{2})/[^/]+\.avro$")


def hour_folder_from_key(key: str) -> str | None:
    for rx in (_RE_HIVE_HOUR, _RE_FLAT_HOUR):
        m = rx.match(key)
        if m:
            return m.group(1) + "/"
    return None


def parse_partition_ts_from_s3_key(key: str) -> datetime | None:
    """Mesma logica que raw_avro_compaction_job.parse_partition_ts_from_s3_key."""
    m = re.search(r"year=(\d{4})/month=(\d{2})/day=(\d{2})/hour=(\d{2})/", key)
    if m:
        y, mo, d, h = m.groups()
        return datetime(int(y), int(mo), int(d), int(h), 0, 0, tzinfo=timezone.utc)
    m = re.search(r"/(\d{4})/(\d{2})/(\d{2})/(\d{2})/", key)
    if m:
        y, mo, d, h = m.groups()
        return datetime(int(y), int(mo), int(d), int(h), 0, 0, tzinfo=timezone.utc)
    return None


@dataclass
class PartitionProbe:
    report: dict[str, Any]
    hour_prefix: str | None
    items: list[dict[str, Any]]


def probe_topic_latest_partition(
    s3_client: Any,
    bucket: str,
    topic_prefix: str,
    target_size_mb: int = TARGET_MB_DEFAULT,
) -> PartitionProbe:
    """Lista AVROs, escolhe latest por LastModified, agrega pasta horaria."""
    prefix = topic_prefix if topic_prefix.endswith("/") else topic_prefix + "/"
    by_hour: dict[str, list[dict[str, Any]]] = defaultdict(list)
    scanned = 0

    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            scanned += 1
            key = obj["Key"]
            if not key.endswith(".avro") or "__compaction_tmp" in key:
                continue
            hf = hour_folder_from_key(key)
            if not hf:
                continue
            by_hour[hf].append(
                {
                    "Key": key,
                    "Size": obj["Size"],
                    "LastModified": obj["LastModified"].astimezone(timezone.utc).isoformat(),
                }
            )

    if not by_hour:
        return PartitionProbe(
            report={
                "error": "no_avro_keys_under_topic",
                "bucket": bucket,
                "prefix": prefix,
                "objects_scanned": scanned,
            },
            hour_prefix=None,
            items=[],
        )

    all_items: list[dict[str, Any]] = []
    for items in by_hour.values():
        all_items.extend(items)
    latest = max(all_items, key=lambda x: datetime.fromisoformat(x["LastModified"]))
    latest_key = latest["Key"]
    partition_ts = parse_partition_ts_from_s3_key(latest_key)
    if partition_ts is None:
        return PartitionProbe(
            report={
                "error": "cannot_parse_partition_from_latest_key",
                "latest_key": latest_key,
            },
            hour_prefix=None,
            items=[],
        )

    hour_prefix = hour_folder_from_key(latest_key)
    if hour_prefix and hour_prefix in by_hour:
        items = by_hour[hour_prefix]
    else:
        items = [latest]

    total_bytes = sum(x["Size"] for x in items)
    sizes = [x["Size"] for x in items]
    hist = Counter()
    for s in sizes:
        if s < 100_000:
            hist["<100KB"] += 1
        elif s < 1_000_000:
            hist["100KB-1MB"] += 1
        elif s < 10_000_000:
            hist["1-10MB"] += 1
        elif s < 100_000_000:
            hist["10-100MB"] += 1
        else:
            hist[">=100MB"] += 1

    target_bytes = target_size_mb * 1024 * 1024
    expected_files_approx = max(1, math.ceil(total_bytes / target_bytes))

    verdict = "unknown"
    if len(items) <= expected_files_approx + 2 and max(sizes) >= target_bytes * 0.5:
        verdict = "likely_compacted_or_large_parts"
    elif len(items) > expected_files_approx * 2 and sum(1 for s in sizes if s < 10_000_000) > 5:
        verdict = "likely_not_compacted_many_small_or_medium_files"
    elif len(items) > expected_files_approx * 2:
        verdict = "likely_not_compacted_file_count_above_target"

    report: dict[str, Any] = {
        "probe": "austa_conta_stream_last_hour",
        "bucket": bucket,
        "topic_prefix": prefix,
        "objects_scanned": scanned,
        "partition_policy": "latest_avro_per_topic",
        "latest_avro_key": latest_key,
        "latest_last_modified_utc": latest["LastModified"],
        "partition_ts_utc_from_key": partition_ts.isoformat(),
        "hour_prefix_for_that_partition": hour_prefix,
        "avro_files_in_that_hour": len(items),
        "total_bytes_in_that_hour": total_bytes,
        "size_bytes_min": min(sizes) if sizes else 0,
        "size_bytes_max": max(sizes) if sizes else 0,
        "size_histogram": dict(hist),
        "target_size_mb": target_size_mb,
        "expected_output_files_approx": expected_files_approx,
        "compaction_health_verdict": verdict,
        "sample_keys": [x["Key"] for x in sorted(items, key=lambda z: z["Key"])[:5]],
    }
    return PartitionProbe(report=report, hour_prefix=hour_prefix, items=items)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def build_spark_argv(args: argparse.Namespace) -> list[str]:
    return [
        args.spark_submit_bin,
        "--master",
        args.spark_master,
        args.job_script,
        "--mode",
        "compact",
        "--bucket",
        args.bucket,
        "--input-prefix",
        STREAM_INPUT_PREFIX,
        "--in-place",
        "--target-size-mb",
        str(args.target_size_mb),
        "--partition-from-latest-avro",
        "--only-topic",
        ONLY_TOPIC_SUBSTRING,
        "--region",
        args.region,
    ]


def run_ssh_preflight(
    ssh_key: str,
    ssh_user: str,
    ssh_host: str,
    job_script_remote: str,
    local_job_py: Path,
) -> int:
    """Verifica ficheiro remoto e compara SHA-256 com o job do repo.

    O marcador `latest_partition_ts_from_topic` e opcional: scripts antigos na EC2
    falhavam o grep e abortavam o preflight mesmo com ficheiro valido.
    """
    print("\n# --- preflight SSH (maquina Spark) ---\n", flush=True)
    q = shlex.quote(job_script_remote)
    # set -e: falha so se test/sha256sum falharem; grep dentro do if nao aborta
    remote_check = (
        f"set -e; test -f {q}; sha256sum {q}; "
        f"if grep -qE 'latest_partition_ts_from_topic|partition-from-latest-avro' {q} 2>/dev/null; "
        f"then echo SCRIPT_FEATURES=new; "
        f"else echo SCRIPT_FEATURES=old_update_recommended; fi; "
        f"echo REMOTE_SCRIPT_OK"
    )
    ssh_base = [
        "ssh",
        "-i",
        ssh_key,
        "-o",
        "BatchMode=yes",
        "-o",
        "StrictHostKeyChecking=no",
        f"{ssh_user}@{ssh_host}",
    ]
    r = subprocess.run(ssh_base + [remote_check], capture_output=True, text=True)
    sys.stdout.write(r.stdout)
    sys.stderr.write(r.stderr)
    if r.returncode != 0:
        print(
            json.dumps(
                {
                    "preflight": "failed",
                    "hint": "SSH ok? test -f e sha256sum falharam — path do script na EC2 ou permissoes.",
                    "ssh_stderr_tail": (r.stderr or "")[-800:],
                },
                ensure_ascii=False,
            ),
            flush=True,
        )
        return r.returncode

    local_sha = sha256_file(local_job_py) if local_job_py.is_file() else ""
    features = "unknown"
    for line in (r.stdout or "").splitlines():
        if line.strip().startswith("SCRIPT_FEATURES="):
            features = line.strip().split("=", maxsplit=1)[-1]
            break
    preflight_ok: dict[str, Any] = {
        "preflight": "ok",
        "local_raw_avro_compaction_job_sha256": local_sha or None,
        "remote_script_features": features,
    }
    if features == "old_update_recommended":
        preflight_ok["warning"] = (
            "Script na EC2 parece antigo (sem partition-from-latest no ficheiro). "
            "Copia airflow/dags/files/raw_avro_compaction_job.py do repo para a EC2."
        )
    print(json.dumps(preflight_ok, indent=2, ensure_ascii=False), flush=True)
    remote_line = (r.stdout or "").strip().splitlines()
    remote_sha = None
    for line in remote_line:
        if job_script_remote in line and len(line.split()) >= 1:
            remote_sha = line.split()[0]
            break
    if local_sha and remote_sha and remote_sha != local_sha:
        print(
            json.dumps(
                {
                    "warning": "sha256_mismatch_repo_vs_spark",
                    "local_sha256": local_sha,
                    "remote_sha256": remote_sha,
                    "action": "Copia o ficheiro do repo para a EC2 e volta a correr o preflight.",
                },
                indent=2,
                ensure_ascii=False,
            ),
            flush=True,
    )
    else:
        print(
            json.dumps({"sha256_match_or_remote_unparsed": True}, indent=2),
            flush=True,
        )
    return 0


def run_ssh_spark_submit(
    ssh_key: str,
    ssh_user: str,
    ssh_host: str,
    argv: list[str],
) -> int:
    """Executa spark-submit na EC2 (mesmo padrao que a DAG: um unico comando remoto)."""
    remote_cmd = shlex.join(argv)
    ssh_cmd = [
        "ssh",
        "-i",
        ssh_key,
        "-o",
        "BatchMode=yes",
        "-o",
        "StrictHostKeyChecking=no",
        f"{ssh_user}@{ssh_host}",
        remote_cmd,
    ]
    print("\n# --- SSH remoto (spark-submit na EC2) ---\n", flush=True)
    print("#", " ".join(shlex.quote(x) for x in ssh_cmd[:6]), "... [comando remoto]", flush=True)
    r = subprocess.run(ssh_cmd)
    return r.returncode


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Probe S3 AUSTA_CONTA + heuristica compactacao + SSH Spark opcional"
    )
    p.add_argument("--bucket", default=DEFAULT_BUCKET)
    p.add_argument("--topic-prefix", default=DEFAULT_TOPIC_PREFIX)
    p.add_argument("--region", default="sa-east-1")
    p.add_argument("--target-size-mb", type=int, default=TARGET_MB_DEFAULT)
    p.add_argument(
        "--run-spark-submit",
        action="store_true",
        help="Executa spark-submit na MAQUINA LOCAL (so faz sentido na propria EC2 Spark)",
    )
    p.add_argument(
        "--remote-spark-ssh",
        action="store_true",
        help="Envia o mesmo comando via ssh para a maquina Spark (recomendado a partir do laptop)",
    )
    p.add_argument(
        "--preflight-ssh",
        action="store_true",
        help="Apenas testa SSH + ficheiro remoto + SHA vs repo (nao compacta)",
    )
    p.add_argument(
        "--compare-after",
        action="store_true",
        help="Apos spark-submit (local ou SSH), volta a ler o S3 e imprime novo snapshot",
    )
    p.add_argument(
        "--spark-submit-bin",
        default=os.environ.get("SPARK_SUBMIT_BIN", "/opt/spark/bin/spark-submit"),
    )
    p.add_argument(
        "--spark-master",
        default=os.environ.get("SPARK_MASTER_URL", "spark://177.71.255.159:7077"),
        help=(
            "URL do Spark master visto a partir da EC2 onde corre spark-submit. "
            "Se o job falhar a ligar ao master, experimenta spark://IP_PRIVADO_DA_EC2:7077 "
            "(o spark-submit corre no host SSH, nao no teu PC)."
        ),
    )
    p.add_argument(
        "--job-script",
        default=os.environ.get(
            "RAW_AVRO_COMPACTION_SCRIPT",
            "/opt/spark/lakehouse/scripts/raw_avro_compaction_job.py",
        ),
    )
    p.add_argument(
        "--ssh-key",
        default=os.environ.get(
            "SPARK_SSH_KEY_PATH", os.environ.get("SPARK_SSH_KEY", _DEFAULT_SSH_KEY)
        ),
        help="Chave PEM (default: ~/.ssh/dlk-austa-sa.pem; no Airflow worker usar /opt/airflow/ssh/spark.pem)",
    )
    p.add_argument(
        "--ssh-host",
        default=os.environ.get("SPARK_HOST", "177.71.255.159"),
    )
    p.add_argument(
        "--ssh-user",
        default=os.environ.get("SPARK_SSH_USER", "ec2-user"),
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    s3 = boto3.client("s3", region_name=args.region)

    local_job_py = Path(__file__).resolve().parent / "raw_avro_compaction_job.py"

    probe = probe_topic_latest_partition(
        s3, args.bucket, args.topic_prefix, target_size_mb=args.target_size_mb
    )
    if "error" in probe.report:
        print(json.dumps(probe.report, indent=2, ensure_ascii=False))
        sys.exit(2 if probe.report.get("error") == "no_avro_keys_under_topic" else 3)

    print(json.dumps(probe.report, indent=2, ensure_ascii=False))

    argv = build_spark_argv(args)
    print("\n# Comando spark-submit (lista):\n")
    print(shlex.join(argv))

    if args.preflight_ssh or args.remote_spark_ssh:
        if not args.ssh_key or not Path(args.ssh_key).is_file():
            print(
                json.dumps(
                    {
                        "error": "ssh_key_missing_or_not_a_file",
                        "hint": "Define --ssh-key ou SPARK_SSH_KEY_PATH para o PEM com acesso ec2-user@SPARK_HOST",
                        "ssh_host": args.ssh_host,
                    },
                    indent=2,
                    ensure_ascii=False,
                ),
                flush=True,
            )
            sys.exit(4)

    if args.preflight_ssh:
        rc = run_ssh_preflight(
            args.ssh_key,
            args.ssh_user,
            args.ssh_host,
            args.job_script,
            local_job_py,
        )
        if rc != 0:
            sys.exit(rc)
        if not args.remote_spark_ssh:
            sys.exit(0)

    if args.remote_spark_ssh and not args.preflight_ssh:
        rc_pf = run_ssh_preflight(
            args.ssh_key,
            args.ssh_user,
            args.ssh_host,
            args.job_script,
            local_job_py,
        )
        if rc_pf != 0:
            sys.exit(rc_pf)

    if args.remote_spark_ssh:
        rc = run_ssh_spark_submit(
            args.ssh_key,
            args.ssh_user,
            args.ssh_host,
            argv,
        )
        if rc != 0:
            sys.exit(rc)
        print(
            "\n# Procura no output acima linhas JSON: compact_meta, compact_summary, topic/mode/in_place.\n",
            flush=True,
        )
        if args.compare_after:
            probe2 = probe_topic_latest_partition(
                s3, args.bucket, args.topic_prefix, target_size_mb=args.target_size_mb
            )
            print("\n# --- S3 depois da run ---\n", flush=True)
            print(json.dumps(probe2.report, indent=2, ensure_ascii=False))
        sys.exit(0)

    if args.run_spark_submit:
        print("\n# Executando spark-submit local...\n", flush=True)
        r = subprocess.run(argv)
        if r.returncode != 0:
            sys.exit(r.returncode)
        if args.compare_after:
            probe2 = probe_topic_latest_partition(
                s3, args.bucket, args.topic_prefix, target_size_mb=args.target_size_mb
            )
            print("\n# --- S3 depois da run ---\n", flush=True)
            print(json.dumps(probe2.report, indent=2, ensure_ascii=False))
        sys.exit(0)

    sys.exit(0)


if __name__ == "__main__":
    main()
