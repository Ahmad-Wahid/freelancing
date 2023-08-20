import sqlparse
import psycopg2
import click


def format_query(query):
    formatted_query = sqlparse.format(
        query,
        strip_comments=True,
        eindent=True,
        keyword_case="upper",
        indent_columns=True,
    )
    return formatted_query


def validate_sql(connection, query):
    cursor = connection.cursor()
    try:
        parsed = sqlparse.parse(query)
        if len(parsed) == 0:
            return False, "Invalid SQL: Empty query"
        formatted_query = format_query(query)

        if parsed[0].get_type() == "SELECT":
            cursor.execute(formatted_query)
            result = cursor.fetchall()
            click.secho(formatted_query, fg="green")
            return True, result
        else:
            click.secho(formatted_query, fg="yellow")
            return (
                False,
                "Invalid SQL: Only SELECT statements are supported for validation",
            )
    except psycopg2.Error as e:
        formatted_query = format_query(query)
        click.secho(formatted_query, fg="yellow")
        return False, str(e)
    finally:
        cursor.close()
