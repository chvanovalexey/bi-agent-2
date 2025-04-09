import pandas as pd
import numpy as np
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def format_results(df):
    """
    Format the query results to be more readable.
    
    Args:
        df (pandas.DataFrame): The query result DataFrame
        
    Returns:
        pandas.DataFrame: The formatted DataFrame
    """
    try:
        # Make a copy to avoid modifying the original
        formatted_df = df.copy()
        
        # Format numeric columns
        for col in formatted_df.select_dtypes(include=['float64', 'float32']).columns:
            # Check if it looks like a currency column (price, amount, cost, etc.)
            if any(term in col.lower() for term in ['price', 'cost', 'amount', 'total', 'discount']):
                # Format as currency with 2 decimal places
                formatted_df[col] = formatted_df[col].round(2).apply(lambda x: f"{x:,.2f} ₽" if pd.notnull(x) else "")
            else:
                # Format other floats with appropriate decimal places
                # If all values are integer-like, remove decimal places
                if all(np.equal(np.mod(formatted_df[col].dropna(), 1), 0)):
                    formatted_df[col] = formatted_df[col].fillna("").astype(str).str.replace('.0$', '', regex=True)
                else:
                    decimal_places = 2  # Default to 2 decimal places
                    formatted_df[col] = formatted_df[col].round(decimal_places)
        
        # Format date and datetime columns
        for col in formatted_df.select_dtypes(include=['datetime64', 'datetime64[ns]']).columns:
            formatted_df[col] = formatted_df[col].dt.strftime('%d.%m.%Y')
        
        for col in formatted_df.select_dtypes(include=['timedelta64[ns]']).columns:
            formatted_df[col] = formatted_df[col].astype(str)
        
        # Handle null values
        formatted_df = formatted_df.fillna("")
        
        # Convert column names to more readable format if they are in snake_case
        formatted_df.columns = [col.replace('_', ' ').capitalize() for col in formatted_df.columns]
        
        logger.info(f"Formatted DataFrame with {len(formatted_df)} rows and {len(formatted_df.columns)} columns")
        return formatted_df
        
    except Exception as e:
        logger.error(f"Error formatting results: {e}")
        # Return original DataFrame if formatting fails
        return df

def format_results_as_html(df):
    """
    Format the query results as HTML.
    
    Args:
        df (pandas.DataFrame): The query result DataFrame
        
    Returns:
        str: HTML representation of the DataFrame
    """
    try:
        # First format the DataFrame
        formatted_df = format_results(df)
        
        # Convert to HTML
        html = formatted_df.to_html(index=False, classes=["table", "table-striped", "table-bordered", "table-hover"])
        
        logger.info(f"Converted DataFrame to HTML table")
        return html
        
    except Exception as e:
        logger.error(f"Error formatting results as HTML: {e}")
        # Return basic HTML table if formatting fails
        return df.to_html(index=False)

def format_column_name(name):
    """
    Format a column name to be more readable.
    
    Args:
        name (str): The column name
        
    Returns:
        str: The formatted column name
    """
    # Replace underscores with spaces
    formatted = name.replace('_', ' ')
    
    # Capitalize
    formatted = formatted.capitalize()
    
    # Replace common abbreviations
    replacements = {
        'Id': 'ID',
        'Sqm': 'кв.м.',
        'Qty': 'Количество',
    }
    
    for abbr, full in replacements.items():
        formatted = formatted.replace(abbr, full)
        
    return formatted 