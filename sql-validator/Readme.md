### Usage

1. First change the directory to `sql_syntax_validator`.
2. Update the `config.ini` file.
3. Then install requirements using the following command.

    ```python
    pip install requirements.txt
    ```

4. To run the validation script, there are two options. You can either pass a single query or file of multiple queries.
   
    - For passing a single query, run the following command. 
         
         ```python
         python validate.py --query "pass your query here"
         ```
    
    - For passing a file `queries.sql` or another file of multiple queries, run the following command.
      
         ```python
         python validate.py --file queries.sql
         ```
         Make sure that, there is a semicolon at the end of every query.