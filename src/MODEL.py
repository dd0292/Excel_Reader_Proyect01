import pandas as pd
import sqlite3
import os
from typing import Callable, List, Union, Dict, Optional

class TableProcessor:

    #-------------------------------------------------------------------
    def __init__(self, db_path: str = 'database.db'):
        """Initialize the TableProcessor with a database connection.
        Args:
            db_path: Path to the SQLite database file
        """
        self.db_path = db_path
        self.conn = None
        
    def __enter__(self):
        """Context manager entry - opens database connection."""
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - closes database connection."""
        self.close()

    def connect(self):
        """Establish a connection to the SQLite database."""
        if self.conn is None:
            self.conn = sqlite3.connect(self.db_path)
            
    def close(self):
        """Close the database connection if it exists."""
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def SetTable(self, table_name: str, df: pd.DataFrame):
        self.connect()
        df.to_sql(table_name, self.conn, if_exists='replace', index=False)

    #-------------------------------------------------------------------   
    def AddColumns(
        self,
        table_name: str,
        df: pd.DataFrame,
        column_definitions: Dict[str, Union[str, Dict, Callable]], 
        source_tables: Dict[str, pd.DataFrame] = None, 
        save_to_db: bool = True,
        print_unmatched: bool = True
    ) -> pd.DataFrame:

        """
        Adds new columns to a DataFrame based on various definition types, including 
        expressions, dictionary-based merges with external tables, or custom functions.

        Parameters:
        ----------
        table_name : str
            The name of the table to update or create in the database.
        
        df : pd.DataFrame
            The main DataFrame to which new columns will be added. If None, it is loaded from the database.
        
        column_definitions : Dict[str, Union[str, Dict, Callable]]
            A dictionary where each key is a new column name, and each value defines how that column is calculated.
            
        Supported definition types:
        - `str`: An expression to be evaluated with `pandas.eval`.
        - `dict`: Describes a merge operation with another table using:
            - `source_table`: str - table to join from
            - `join_on`: str - column in the source table
            - `join_target`: str (optional) - column in `df` to join on (defaults to `join_on`)
            - `source_column`: str (optional) - column to extract from the source table (defaults to new column name)
        - `Callable`: A function applied row-wise to compute the column.
        
        source_tables : Dict[str, pd.DataFrame], optional
            A dictionary of source DataFrames to use for merges, avoiding redundant database queries.
            If not provided, tables are retrieved using `self.GetTables()`.

        save_to_db : bool, default=True
            Whether to overwrite the table in the database after processing.

        print_unmatched : bool, default=True
            If True, logs to a file (`merge_warnings.txt`) all the keys in the main DataFrame that 
            could not find a match during the merge process.

        Returns:
        -------
        pd.DataFrame
            The modified DataFrame with the newly added columns.

        Raises:
        ------
        ValueError
            If a definition is invalid, a required column is missing from the source table,
            or a function fails during application.

        Notes:
        -----
        - Mismatches during merges are logged in `merge_warnings.txt` with context about the table, 
        join keys, and unmatched values.
        - This function is suitable for dynamic feature engineering in ETL workflows or report generation pipelines.
        """
        
        self.connect()

        if df is None:
            df = self.GetTables(table_name)

        merge_tasks = {}
        log_file = open("merge_warnings.txt", "a", encoding="utf-8")

        for new_col, definition in column_definitions.items():
            
            if isinstance(definition, str):
                try:
                    df[new_col] = df.eval(definition)
                except Exception:
                    raise ValueError(f"Could not evaluate expression: {definition}")

            elif isinstance(definition, dict):
                source_table = definition.get('source_table')
                if not source_table:
                    raise ValueError("Missing 'source_table' in column definition")

                join_on = definition.get('join_on')
                join_target = definition.get('join_target', join_on)
                source_col = definition.get('source_column', new_col)

                if source_table not in merge_tasks:
                    merge_tasks[source_table] = []

                merge_tasks[source_table].append((new_col, join_on, join_target, source_col))

            elif callable(definition):
                try:
                    df[new_col] = df.apply(definition, axis=1)
                except Exception as e:
                    raise ValueError(f"Custom function failed: {str(e)}")

            else:
                raise ValueError(f"Unsupported definition type for column '{new_col}'")

        for source_table, columns in merge_tasks.items():
            source_df = source_tables.get(source_table) if source_tables else self.GetTables(source_table)

            for _, join_on, _, source_col in columns:
                if join_on not in source_df.columns:
                    raise ValueError(f"Join column '{join_on}' not in source table '{source_table}'")
                if source_col not in source_df.columns:
                    raise ValueError(f"Source column '{source_col}' not in source table '{source_table}'")

            for new_col, join_on, join_target, source_col in columns:
                before_merge = df.shape[0]
                temp_df = df.merge(
                    source_df[[join_on, source_col]].drop_duplicates(),
                    how='left',
                    left_on=join_target,
                    right_on=join_on
                ).rename(columns={source_col: new_col})

                unmatched_rows = temp_df[temp_df[new_col].isna()]
                if not unmatched_rows.empty and print_unmatched:
                
                    log_file.write(f"\n--- Registro para la tabla: {table_name} ---\n")
                    log_file.write(f"\n[Advertencia] {new_col}: {len(unmatched_rows)} filas no coincidentes al unir '{join_target}' -> '{join_on}' desde '{source_table}'\n")
                    log_file.write(f"\t {unmatched_rows[[join_target]].drop_duplicates().to_string(index=False)}")
                    log_file.write("\n")
                    log_file.write(f"--- Fin de la tabla: {table_name} ---\n\n")
                    
                df = temp_df

        log_file.close()

        if save_to_db:
            df.to_sql(table_name, self.conn, if_exists='replace', index=False)

        return df


    def ListTables(self) -> List[str]:
        """List all tables in the database.
        
        Returns:
            List of table names in the database
        """
        self.connect()
        query = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        tables = pd.read_sql(query, self.conn)['name'].tolist()
        return tables
    
    def ImportFromExcel(self, excel_path: str, sheet_name: str = None, 
                       table_name: str = None, if_exists: str = 'replace'):
        """Import data from Excel file to database table.
        
        Args:
            excel_path: Path to Excel file
            sheet_name: Name of the Excel sheet to import (defaults to first sheet)
            table_name: Name for the database table (defaults to Excel filename without extension)
            if_exists: What to do if table exists ('fail', 'replace', 'append')
        """
        if not os.path.exists(excel_path):
            raise FileNotFoundError(f"Excel file not found: {excel_path}")
            
        if table_name is None:
            table_name = os.path.splitext(os.path.basename(excel_path))[0]
            
        self.connect()
        df = pd.read_excel(excel_path, sheet_name=sheet_name)
        df.to_sql(table_name, self.conn, if_exists=if_exists, index=False)
        
    def PrintColumns(self, table_name: str) -> List[str]:
        """Print and return the columns of a specific table.
        
        Args:
            table_name: Name of the table to inspect
            
        Returns:
            List of column names
        """
        self.connect()
        
        if not self.table_exists(table_name):
            raise ValueError(f"Table '{table_name}' does not exist in the database")
            
        query = f"PRAGMA table_info({table_name})"
        columns_info = pd.read_sql(query, self.conn)
        columns = columns_info['name'].tolist()
        
        print(f"\nColumns in table '{table_name}':")
        for i, col in enumerate(columns, 1):
            print(f"    ∟ {i}. {col}")
            
        return columns
    
    def ExportToExcel(self, table_name: str, output_path: str = None, 
                     sheet_name: str = "Sheet1"):
        """Export a database table to Excel file.
        
        Args:
            table_name: Name of the table to export
            output_path: Path for the output Excel file (defaults to table_name.xlsx)
            sheet_name: Name of the Excel sheet to create
        """
        self.connect()
        
        if not self.table_exists(table_name):
            raise ValueError(f"Table '{table_name}' does not exist in the database")
            
        if output_path is None:
            output_path = f"{table_name}.xlsx"
            
        df = self.GetTables(table_name)
        df.to_excel(output_path, sheet_name=sheet_name, index=False)
        print(f"Table '{table_name}' exported to {output_path}")

    def GetTables(self, table_names: Union[str, List[str]]) -> Union[pd.DataFrame, Dict[str, pd.DataFrame]]:
        """Retrieve one or more tables from the SQLite database.
        
        Args:
            table_names: Single table name or list of table names
            
        Returns:
            Single DataFrame if one table requested, or dictionary of DataFrames
            
        Raises:
            ValueError: If any requested table doesn't exist
        """
        self.connect()
        
        if isinstance(table_names, str):
            if not self.table_exists(table_names):
                raise ValueError(f"Table '{table_names}' does not exist")
            return pd.read_sql(f'SELECT * FROM "{table_names}"', self.conn)
        
        # Check all tables exist first
        for name in table_names:
            if not self.table_exists(name):
                raise ValueError(f"Table '{name}' does not exist")
        
        tables = {}
        for name in table_names:
            tables[name] = pd.read_sql(f"SELECT * FROM {name}", self.conn)
        return tables
    
    def ConcatTables(self, tables: List[pd.DataFrame], output_table: str, axis: int = 0, **kwargs):
        """Concatenate multiple tables along an axis and save to database.
        
        Args:
            tables: List of DataFrames to concatenate
            output_table: Name for the output table in database
            axis: 0 for row-wise concatenation, 1 for column-wise
            **kwargs: Additional arguments to pd.concat
        """
        self.connect()
        
        concatenated = pd.concat(tables, axis=axis, **kwargs)
        concatenated.to_sql(output_table, self.conn, if_exists='replace', index=False)
        
    def MergeTables(self, left: pd.DataFrame, right: pd.DataFrame, output_table: str, 
                    how: str = 'inner', on: Optional[Union[str, List[str]]] = None, **kwargs):
        """Merge two tables and save the result to database.
        
        Args:
            left: Left DataFrame to merge
            right: Right DataFrame to merge
            output_table: Name for the output table in database
            how: Type of merge ('left', 'right', 'outer', 'inner')
            on: Column(s) to join on
            **kwargs: Additional arguments to pd.merge
        """
        self.connect()
        
        merged = pd.merge(left, right, how=how, on=on, **kwargs)
        merged.to_sql(output_table, self.conn, if_exists='replace', index=False)
        
    def PivotTables(self, df: pd.DataFrame, output_table: str, 
                rows: Union[str, List[str]] = None,
                columns: Union[str, List[str]] = None,
                values: Union[str, List[str]] = None,
                filters: Dict[str, List] = None,
                aggfunc: Union[str, callable, Dict] = 'sum',
                **kwargs):
        """Create a pivot table from the input DataFrame and save to database.
        
        Args:
            df: Input DataFrame
            output_table: Name for the output table in database
            rows: Column(s) to use as rows (equivalent to Excel's "Rows" area)
            columns: Column(s) to use as columns (equivalent to Excel's "Columns" area)
            values: Column(s) to use as values (equivalent to Excel's "Values" area)
            filters: Dictionary of {column: values} to filter (equivalent to Excel's "Filters" area)
            aggfunc: Aggregation function(s) to use (default: 'sum')
            **kwargs: Additional arguments to pd.pivot_table
        """
        self.connect()

        if filters:
            for col, filter_values in filters.items():
                if col in df.columns:
                    df = df[df[col].isin(filter_values)]
        
        pivoted = pd.pivot_table(df, 
                                index=rows, 
                                columns=columns, 
                                values=values, 
                                aggfunc=aggfunc,
                                **kwargs)
        
        if isinstance(pivoted.index, pd.MultiIndex):
            pivoted.reset_index(inplace=True)
            
        if isinstance(pivoted.columns, pd.MultiIndex):
            pivoted.columns = [' '.join(str(col)).strip() for col in pivoted.columns.values]
        
        pivoted.to_sql(output_table, self.conn, if_exists='replace', index=False)
        return pivoted

    def execute_sql(self, query: str, params: tuple = None) -> pd.DataFrame:
        """Execute a raw SQL query and return results as DataFrame.
        
        Args:
            query: SQL query to execute
            params: Parameters for parameterized query
            
        Returns:
            DataFrame containing query results
        """
        self.connect()
        return pd.read_sql(query, self.conn, params=params)

    def DropTable(self, table_name: str, confirm: bool = True) -> bool:
        """Drop/delete a table from the SQLite database.
        
        Args:
            table_name: Name of the table to drop
            confirm: If True, will print confirmation before dropping (default: True)
            
        Returns:
            bool: True if table was dropped, False if not
            
        Raises:
            ValueError: If table doesn't exist
        """
        self.connect()
        
        if not self.table_exists(table_name):
            raise ValueError(f"Table '{table_name}' does not exist in the database")
        
        if confirm:
            response = input(f"Are you sure you want to drop table '{table_name}'? (y/n): ").lower()
            if response != 'y':
                print("Table drop cancelled")
                return False
        
        try:
            self.conn.execute(f"DROP TABLE {table_name}")
            self.conn.commit()
            print(f"Table '{table_name}' successfully dropped")
            return True
        
        except Exception as e:
            self.conn.rollback()
            raise RuntimeError(f"Error dropping table '{table_name}': {str(e)}")

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database."""
        self.connect()
        cursor = self.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        return cursor.fetchone() is not None