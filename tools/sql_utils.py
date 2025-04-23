import os
import json
from typing import Optional

from databricks.sdk import WorkspaceClient
from langchain_core.tools import BaseTool, tool
from langchain_core.tools.base import ArgsSchema
from pydantic import BaseModel, Field

class TableInput(BaseModel):
    table_name: str = Field(description="the fully qualified name of the table, in the format 'catalog'.'schema'.'table'")
    column_name: str = Field(description="the name of the column")

class GetColumnValues(BaseTool):
  name: str = "get_column_values"
  description: str = "A tool that accepts a table name and a column name and returns all of the unique values in the given column for the given table"
  args_schema: Optional[ArgsSchema] = TableInput

  def _run(self, table_name:str, column_name:str) -> str:
    """
    A function that accepts a table name and a column name and returns all of the unique values in the given column for the given table

    Args:
        table_name (str): the fully qualified name of the table, in the format 'catalog'.'schema'.'table'
        column_name (str): the name of the column

    Returns:
        str: a string of the unique values in the given column for the given table
    """
    import os
    from databricks.sdk import WorkspaceClient
    
    query_text = f"""SELECT DISTINCT {column_name} FROM {table_name}"""
    
    try:
      w = WorkspaceClient(host=os.environ["DATABRICKS_HOST"], token=os.environ["DATABRICKS_TOKEN"])
      results = w.statement_execution.execute_statement(query_text, warehouse_id=os.environ["DATABRICKS_WAREHOUSE_ID"])

      results_list = [f"`{t[0]}`" for t in results.result.data_array]
      results_output = ',\n'.join(results_list)

      return results_output
    except Exception as e:
      raise Exception(f"Error executing query: {e}")
      pass