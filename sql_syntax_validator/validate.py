import click
import sqlparse
import psycopg2
from configparser import ConfigParser


def read_config():
    config = ConfigParser()
    config.read('config.ini')
    return config


def validate_sql(connection, query):
    try:
        parsed = sqlparse.parse(query)

        if len(parsed) == 0:
            return False, "Invalid SQL: Empty query"

        formatted_query = sqlparse.format(query, strip_comments=True)

        cursor = connection.cursor()

        if parsed[0].get_type() == 'SELECT':
            cursor.execute(formatted_query)
            result = cursor.fetchall()
            return True, result
        else:
            return False, "Invalid SQL: Only SELECT statements are supported for validation"
    except Exception as e:
        return False, str(e)
    finally:
        cursor.close()


@click.command()
@click.option('--query', help='SQL query to validate')
@click.option('--file', type=click.Path(exists=True), help='Path to file containing SQL queries')
def main(query, file):
    try:
        config_data = read_config()
        database = config_data.get('database', 'name')
        user = config_data.get('database', 'user')
        password = config_data.get('database', 'password')
        host = config_data.get('database', 'host', fallback='localhost')
        port = config_data.get('database', 'port', fallback='5432')

        conn = psycopg2.connect(
            database=database,
            user=user,
            password=password,
            host=host,
            port=port
        )

        if query:
            queries = [query]
        elif file:
            with open(file, 'r') as f:
                queries = f.read().split(';')
        else:
            click.echo("Please provide either --query or --file option.")
            return

        for sql_query in queries:
            if sql_query.strip():
                is_valid, validation_result = validate_sql(conn, sql_query.strip())

                if is_valid:
                    click.echo("Query is valid.")
                    if validation_result:
                        click.echo("Query result:")
                        for row in validation_result:
                            click.echo(row)
                else:
                    click.echo("Query is not valid.")
                    click.echo("Validation error:", validation_result)
            else:
                click.echo("Empty string is passed.")
    except Exception as e:
        click.echo(f"Error: {str(e)}")
    finally:
        conn.close()


if __name__ == '__main__':
    main()
