"""
Vanna.AI integration for Apache Doris
"""
import os
from typing import List, Dict, Any, Optional
from vanna.base import VannaBase
from db import DorisClient
from config import DORIS_CONFIG


class VannaDoris(VannaBase):
    """
    Vanna.AI adapter for Apache Doris database
    """
    
    def __init__(self, doris_client: DorisClient, config: Optional[Dict[str, Any]] = None):
        """
        Initialize Vanna with Doris client
        
        Args:
            doris_client: DorisClient instance
            config: Optional configuration dict
        """
        VannaBase.__init__(self, config=config)
        self.doris_client = doris_client
        
    def connect_to_doris(self, host: str = None, port: int = None, 
                         user: str = None, password: str = None, 
                         database: str = None) -> None:
        """
        Connect to Doris database (already connected via doris_client)
        """
        # Already connected via doris_client, this is just for compatibility
        pass
    
    def run_sql(self, sql: str) -> Any:
        """
        Execute SQL query on Doris
        
        Args:
            sql: SQL query string
            
        Returns:
            Query results as list of dicts
        """
        try:
            # Remove any trailing semicolons
            sql = sql.strip().rstrip(';')
            
            # Execute query using DorisClient
            results = self.doris_client.execute_query(sql)
            
            return results
        except Exception as e:
            raise Exception(f"Error executing SQL: {str(e)}")
    
    def get_table_names(self) -> List[str]:
        """
        Get list of all table names in the database
        
        Returns:
            List of table names
        """
        try:
            tables = self.doris_client.get_tables()
            return tables
        except Exception as e:
            raise Exception(f"Error getting table names: {str(e)}")
    
    def get_table_schema(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get schema information for a specific table
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of column information dicts
        """
        try:
            schema = self.doris_client.get_table_schema(table_name)
            return schema
        except Exception as e:
            raise Exception(f"Error getting schema for table {table_name}: {str(e)}")
    
    def get_related_ddl(self, question: str, **kwargs) -> List[str]:
        """
        Get DDL statements for tables related to the question
        
        Args:
            question: Natural language question
            
        Returns:
            List of DDL statements
        """
        try:
            # Get all tables
            tables = self.get_table_names()
            
            ddl_statements = []
            for table in tables:
                try:
                    # Get table schema
                    schema = self.get_table_schema(table)
                    
                    # Build CREATE TABLE statement
                    columns = []
                    for col in schema:
                        col_def = f"`{col['Field']}` {col['Type']}"
                        if col.get('Null') == 'NO':
                            col_def += " NOT NULL"
                        if col.get('Default'):
                            col_def += f" DEFAULT {col['Default']}"
                        columns.append(col_def)
                    
                    ddl = f"CREATE TABLE `{table}` (\n  " + ",\n  ".join(columns) + "\n);"
                    ddl_statements.append(ddl)
                except:
                    continue
            
            return ddl_statements
        except Exception as e:
            raise Exception(f"Error getting DDL: {str(e)}")
    
    def get_related_documentation(self, question: str, **kwargs) -> List[str]:
        """
        Get documentation related to the question

        Args:
            question: Natural language question

        Returns:
            List of documentation strings
        """
        # For now, return empty list
        # Can be extended to include custom documentation
        return []

    # Training data methods (required by VannaBase but not used in our implementation)
    def add_ddl(self, ddl: str, **kwargs) -> str:
        """Add DDL to training data (not implemented)"""
        return "DDL storage not implemented"

    def add_documentation(self, documentation: str, **kwargs) -> str:
        """Add documentation to training data (not implemented)"""
        return "Documentation storage not implemented"

    def add_question_sql(self, question: str, sql: str, **kwargs) -> str:
        """Add question-SQL pair to training data (not implemented)"""
        return "Question-SQL storage not implemented"

    def get_training_data(self, **kwargs) -> Any:
        """Get training data (not implemented)"""
        return []

    def remove_training_data(self, id: str, **kwargs) -> bool:
        """Remove training data (not implemented)"""
        return True

    def generate_embedding(self, data: str, **kwargs) -> List[float]:
        """Generate embedding for data (not implemented)"""
        return []

    def get_similar_question_sql(self, question: str, **kwargs) -> List[Dict[str, str]]:
        """Get similar question-SQL pairs (not implemented)"""
        return []
    
    def get_sql_prompt(self, question: str, question_sql_list: List[Dict[str, str]], 
                       ddl_list: List[str], doc_list: List[str], **kwargs) -> str:
        """
        Generate the prompt for SQL generation
        
        Args:
            question: Natural language question
            question_sql_list: List of example question-SQL pairs
            ddl_list: List of DDL statements
            doc_list: List of documentation strings
            
        Returns:
            Formatted prompt string
        """
        # Build the prompt
        prompt = "You are an expert SQL query generator for Apache Doris database.\n\n"
        
        # Add DDL information
        if ddl_list:
            prompt += "# Database Schema\n\n"
            for ddl in ddl_list:
                prompt += f"{ddl}\n\n"
        
        # Add example queries if available
        if question_sql_list:
            prompt += "# Example Queries\n\n"
            for example in question_sql_list:
                prompt += f"Question: {example.get('question', '')}\n"
                prompt += f"SQL: {example.get('sql', '')}\n\n"
        
        # Add documentation if available
        if doc_list:
            prompt += "# Documentation\n\n"
            for doc in doc_list:
                prompt += f"{doc}\n\n"
        
        # Add the actual question
        prompt += f"# Task\n\nGenerate a SQL query for Apache Doris to answer the following question:\n\n"
        prompt += f"Question: {question}\n\n"
        prompt += "Requirements:\n"
        prompt += "1. Generate ONLY the SQL query, no explanations\n"
        prompt += "2. Use proper column names and table names (use backticks for Chinese names)\n"
        prompt += "3. The SQL should be executable in Apache Doris\n"
        prompt += "4. Do not include markdown code blocks or any other formatting\n"
        prompt += "5. Return only the raw SQL statement\n\n"
        prompt += "SQL:"
        
        return prompt
    
    def submit_prompt(self, prompt: str, **kwargs) -> str:
        """
        Submit prompt to LLM and get response
        This method should be overridden by the LLM-specific class
        
        Args:
            prompt: The prompt to submit
            
        Returns:
            LLM response
        """
        raise NotImplementedError("This method should be implemented by LLM-specific class")
    
    def generate_sql(self, question: str, allow_llm_to_see_data: bool = False) -> str:
        """
        Generate SQL from natural language question
        
        Args:
            question: Natural language question
            allow_llm_to_see_data: Whether to allow LLM to see sample data
            
        Returns:
            Generated SQL query
        """
        # Get related DDL
        ddl_list = self.get_related_ddl(question)
        
        # Get related documentation
        doc_list = self.get_related_documentation(question)
        
        # Get example queries (empty for now, can be extended)
        question_sql_list = []
        
        # Generate prompt
        prompt = self.get_sql_prompt(
            question=question,
            question_sql_list=question_sql_list,
            ddl_list=ddl_list,
            doc_list=doc_list
        )
        
        # Submit to LLM
        sql = self.submit_prompt(prompt)
        
        # Clean up the response
        sql = sql.strip()
        
        # Remove markdown code blocks if present
        if sql.startswith('```sql'):
            sql = sql[6:]
        elif sql.startswith('```'):
            sql = sql[3:]
        
        if sql.endswith('```'):
            sql = sql[:-3]
        
        sql = sql.strip()
        
        return sql


class VannaDorisOpenAI(VannaDoris):
    """
    Vanna.AI with OpenAI (or compatible API like DeepSeek) for Doris
    """

    def __init__(self, doris_client: DorisClient,
                 api_key: str = None,
                 model: str = None,
                 base_url: str = None,
                 config: Optional[Dict[str, Any]] = None):
        """
        Initialize Vanna with OpenAI-compatible API

        Args:
            doris_client: DorisClient instance
            api_key: API key for OpenAI-compatible service
            model: Model name (e.g., 'gpt-4', 'deepseek-chat')
            base_url: Base URL for API (e.g., 'https://api.deepseek.com')
            config: Optional configuration dict
        """
        # Initialize Vanna Doris
        VannaDoris.__init__(self, doris_client=doris_client, config=config)

        # Set API configuration
        self.api_key = api_key
        self.model = model or 'deepseek-chat'
        self.base_url = base_url or 'https://api.deepseek.com'

    def system_message(self, message: str) -> Dict[str, str]:
        """Create a system message"""
        return {"role": "system", "content": message}

    def user_message(self, message: str) -> Dict[str, str]:
        """Create a user message"""
        return {"role": "user", "content": message}

    def assistant_message(self, message: str) -> Dict[str, str]:
        """Create an assistant message"""
        return {"role": "assistant", "content": message}

    def submit_prompt(self, prompt: str, **kwargs) -> str:
        """
        Submit prompt to OpenAI-compatible API

        Args:
            prompt: The prompt to submit

        Returns:
            LLM response
        """
        import requests

        # Prepare API request
        url = f"{self.base_url}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        data = {
            "model": self.model,
            "messages": [
                self.system_message("You are an expert SQL query generator for Apache Doris database."),
                self.user_message(prompt)
            ],
            "temperature": self.config.get('temperature', 0.1) if self.config else 0.1,
            "max_tokens": self.config.get('max_tokens', 2000) if self.config else 2000
        }

        # Submit to API
        response = requests.post(url, headers=headers, json=data, timeout=60)
        response.raise_for_status()

        result = response.json()
        return result['choices'][0]['message']['content']

