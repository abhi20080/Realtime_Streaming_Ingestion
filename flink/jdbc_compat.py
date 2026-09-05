"""Adapt PyFlink 1.20 to the pinned JDBC 3.3 connector.

The connector moved the statement-builder method used by PyFlink. Keep this
version-specific Java bridge separate from the pipeline graph and sink policy.
Revalidate it with the live smoke test whenever either dependency changes.
"""

from pyflink.datastream.connectors.jdbc import JdbcSink
from pyflink.java_gateway import get_gateway
from pyflink.util.java_utils import to_jarray


def build_compatible_jdbc_sink(insert_sql, sink_type, execution_options, connection_options):
    """Build the existing at-least-once sink using the connector's row builder."""
    # PyFlink 1.20 calls a statement-builder method that moved in JDBC
    # connector 3.3.  Reflecting on RowJdbcOutputFormat keeps the public Python
    # Row API while using the requested connector release.
    gateway = get_gateway()
    jdbc_type_util = gateway.jvm.org.apache.flink.connector.jdbc.utils.JdbcTypeUtil
    sql_types = [
        jdbc_type_util.typeInformationToSqlType(field_type.get_java_type_info())
        for field_type in sink_type.get_field_types()
    ]
    java_sql_types = to_jarray(gateway.jvm.int, sql_types)
    output_format_class = gateway.jvm.Class.forName(
        "org.apache.flink.connector.jdbc.internal.RowJdbcOutputFormat",
        False,
        gateway.jvm.Thread.currentThread().getContextClassLoader(),
    )
    int_array_class = to_jarray(gateway.jvm.int, []).getClass()
    builder_method = output_format_class.getDeclaredMethod(
        "createRowJdbcStatementBuilder",
        to_jarray(gateway.jvm.Class, [int_array_class]),
    )
    builder_method.setAccessible(True)
    statement_builder = builder_method.invoke(
        None, to_jarray(gateway.jvm.Object, [java_sql_types])
    )
    java_sink = gateway.jvm.org.apache.flink.connector.jdbc.JdbcSink.sink(
        insert_sql,
        statement_builder,
        execution_options._j_jdbc_execution_options,
        connection_options._j_jdbc_connection_options,
    )
    return JdbcSink(j_jdbc_sink=java_sink)
