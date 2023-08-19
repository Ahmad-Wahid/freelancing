import sqlparse
import psycopg2

def validate_sql(connection, query):
    cursor = connection.cursor()
    try:
        parsed = sqlparse.parse(query)
        if len(parsed) == 0:
            return False, "Invalid SQL: Empty query"
        formatted_query = sqlparse.format(query, strip_comments=True)
        if parsed[0].get_type() == 'SELECT':
            cursor.execute(formatted_query)
            result = cursor.fetchall()
            return True, result
        else:
            return False, "Invalid SQL: Only SELECT statements are supported for validation"
    except psycopg2.Error as e:
        return False, str(e)
    finally:
        cursor.close()
