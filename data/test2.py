
from airflow.hooks.base import BaseHook

def get_aws_env_vars(conn_id="aws_default"):
    try:
        # Fetch the connection object
        conn = BaseHook.get_connection(conn_id)
        
        # Build the dictionary
        env_vars = {
            "AWS_ACCESS_KEY_ID": conn.login,
            "AWS_SECRET_ACCESS_KEY": conn.password,
            "AWS_DEFAULT_REGION": conn.extra_dejson.get("region_name", "us-east-1")
        }
        
        # Handle temporary credentials (if using STS/Role assumption)
        if conn.extra_dejson.get("aws_session_token"):
             env_vars["AWS_SESSION_TOKEN"] = conn.extra_dejson.get("aws_session_token")
             
        return env_vars
    except Exception as e:
        print(f"Warning: Could not fetch AWS connection {conn_id}: {e}")
        return {}

aws_vars = get_aws_env_vars("aws_default")
print(aws_vars)