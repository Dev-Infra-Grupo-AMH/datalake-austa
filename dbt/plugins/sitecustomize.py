# sitecustomize.py — importado pelo Python ao iniciar se estiver no site-packages.
# Instalação: pip install -r requirements-dbt.txt (pacote editável em ./plugins).
#
# 1) DBT_PROFILES_DIR + .env: sem export manual ao rodar dbt a partir da árvore do projeto.
# 2) Patch dbt-spark: não passa database ao PyHive; injeta database=creds.schema
def _bootstrap_local_dbt_env():
    import os
    from pathlib import Path

    try:
        from dotenv import load_dotenv
    except ImportError:
        load_dotenv = None

    if os.environ.get("DBT_PROFILES_DIR"):
        prof = Path(os.environ["DBT_PROFILES_DIR"])
        if load_dotenv and (prof / ".env").is_file():
            load_dotenv(prof / ".env", override=False)
        return

    cwd = Path.cwd()
    for _ in range(12):
        if (cwd / "dbt_project.yml").is_file():
            d = cwd.resolve()
            os.environ["DBT_PROFILES_DIR"] = str(d)
            if load_dotenv and (d / ".env").is_file():
                load_dotenv(d / ".env", override=False)
            return
        sub = cwd / "dbt"
        if (sub / "dbt_project.yml").is_file():
            d = sub.resolve()
            os.environ["DBT_PROFILES_DIR"] = str(d)
            if load_dotenv and (d / ".env").is_file():
                load_dotenv(d / ".env", override=False)
            return
        parent = cwd.parent
        if parent == cwd:
            break
        cwd = parent


def _apply_dbt_spark_patch():
    try:
        import dbt.adapters.spark.connections as conn_mod
        from dbt.adapters.spark.connections import SparkConnectionMethod
    except ImportError:
        # Ambiente sem dbt-spark ou pip ainda instalando dependências.
        return

    import re

    _original_open = conn_mod.SparkConnectionManager.open

    def _patched_open(cls, connection):
        creds = connection.credentials
        if creds.method == SparkConnectionMethod.THRIFT and creds.schema:
            from pyhive import hive

            _orig = hive.connect

            def _hive_connect_with_db(*args, **kwargs):
                # PyHive expects only namespace/database (no catalog prefix).
                # If schema comes as catalog.schema, keep only final namespace.
                schema_name = str(creds.schema or "raw").split(".")[-1]
                kwargs["database"] = schema_name
                conn = _orig(*args, **kwargs)

                # Workaround for Iceberg v2:
                # "SHOW TABLE EXTENDED is not supported for v2 tables"
                # dbt-spark uses SHOW TABLE EXTENDED during relation introspection.
                # If that fails, fallback to SHOW TABLES and adapt row shape.
                try:
                    _orig_cursor = conn.cursor

                    def _cursor_wrapper(*cargs, **ckw):
                        cur = _orig_cursor(*cargs, **ckw)
                        _orig_execute = cur.execute

                        def _execute_wrapper(query, *eargs, **ekw):
                            try:
                                return _orig_execute(query, *eargs, **ekw)
                            except Exception as e:
                                q = str(query or "")
                                msg = str(e).lower()
                                if (
                                    re.search(r"show\s+table\s+extended", q, re.I)
                                    and "not supported for v2 tables" in msg
                                ):
                                    m = re.search(
                                        r"show\s+table\s+extended\s+in\s+(\S+)\s+like\s+(.+)",
                                        q,
                                        re.I,
                                    )
                                    if m:
                                        schema = m.group(1)
                                        like_expr = m.group(2).strip()
                                        alt_query = f"SHOW TABLES IN {schema} LIKE {like_expr}"
                                        res = _orig_execute(alt_query, *eargs, **ekw)

                                        def _patch_rows(fn):
                                            def _wrapped(*fargs, **fkw):
                                                rows = fn(*fargs, **fkw)
                                                if rows is None:
                                                    return rows
                                                if isinstance(rows, tuple):
                                                    return rows + ("",) if len(rows) == 3 else rows
                                                if isinstance(rows, list):
                                                    out = []
                                                    for r in rows:
                                                        if isinstance(r, tuple) and len(r) == 3:
                                                            out.append(r + ("",))
                                                        else:
                                                            out.append(r)
                                                    return out
                                                return rows

                                            return _wrapped

                                        if hasattr(cur, "fetchall"):
                                            cur.fetchall = _patch_rows(cur.fetchall)
                                        if hasattr(cur, "fetchone"):
                                            cur.fetchone = _patch_rows(cur.fetchone)
                                        return res
                                raise

                        cur.execute = _execute_wrapper
                        return cur

                    conn.cursor = _cursor_wrapper
                except Exception:
                    pass

                return conn

            hive.connect = _hive_connect_with_db
            try:
                return _original_open.__func__(cls, connection)
            finally:
                hive.connect = _orig
        return _original_open.__func__(cls, connection)

    conn_mod.SparkConnectionManager.open = classmethod(_patched_open)


_bootstrap_local_dbt_env()
_apply_dbt_spark_patch()
