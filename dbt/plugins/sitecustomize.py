# sitecustomize.py - carregado automaticamente pelo Python ao iniciar
# Patch: dbt-spark não passa database ao PyHive; injeta database=creds.schema
def _apply_dbt_spark_patch():
    import dbt.adapters.spark.connections as conn_mod
    from dbt.adapters.spark.connections import SparkConnectionMethod

    _original_open = conn_mod.SparkConnectionManager.open

    def _patched_open(cls, connection):
        creds = connection.credentials
        if creds.method == SparkConnectionMethod.THRIFT and creds.schema:
            from pyhive import hive

            _orig = hive.connect

            def _hive_connect_with_db(*args, **kwargs):
                kwargs.setdefault("database", creds.schema)
                return _orig(*args, **kwargs)

            hive.connect = _hive_connect_with_db
            try:
                return _original_open.__func__(cls, connection)
            finally:
                hive.connect = _orig
        return _original_open.__func__(cls, connection)

    conn_mod.SparkConnectionManager.open = classmethod(_patched_open)


_apply_dbt_spark_patch()
