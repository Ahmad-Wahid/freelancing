import click
import sys

from sql_validator.db_connector import connect_db
from sql_validator.sql_validator import validate_sql


@click.command()
@click.option("--query", help="SQL query to validate")
@click.option(
    "--file", type=click.Path(exists=True), help="Path to file containing SQL queries"
)
def main(query, file):
    conn = connect_db()
    try:
        if query:
            queries = [query]
        elif file:
            with open(file, "r") as f:
                queries = f.read().split(";")
        else:
            click.echo("Please provide either --query or --file option.")
            return

        for sql_query in queries:
            if sql_query.strip():
                is_valid, validation_result = validate_sql(conn, sql_query.strip())

                if is_valid:
                    click.echo("Query is valid.")
                    print("=" * 20)
                    # if validation_result:
                    #     click.echo("Query result:")
                    #     for row in validation_result:
                    #         click.echo(row)
                else:
                    print("Query is not valid.", file=sys.stderr)
                    click.secho(
                        f"Validation error: {validation_result}",
                        file=sys.stderr,
                        fg="red",
                    )
                    print("=" * 20)
            else:
                click.echo("Empty string is passed.")
    except Exception as e:
        click.echo(f"Error: {str(e)}")
    finally:
        conn.close()
