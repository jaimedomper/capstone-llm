import boto3
import json
import os
from openai import OpenAI
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file

# --- Configuration ---
AWS_BUCKET = "dataminded-academy-capstone-llm-data-us"
# The folder to read from
SOURCE_PREFIX = "cleaned/Jaime/"
# The API Key (Best practice: set this as an env variable)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY") # or paste string here temporarily

# --- Initialize Clients ---
s3_client = boto3.client('s3')
openai_client = OpenAI(api_key=OPENAI_API_KEY)

def get_s3_files(bucket, prefix):
    """Lists all JSON files in the S3 bucket prefix."""
    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        if 'Contents' not in response:
            return []
        # Filter for .json files and exclude files we've already modified
        return [
            obj['Key'] for obj in response['Contents'] 
            if obj['Key'].endswith('.json') and '_injected.json' not in obj['Key']
        ]
    except ClientError as e:
        print(f"Error listing objects: {e}")
        return []

def modify_content_with_openai(json_content):
    """
    Sends the content to OpenAI to apply the 'Lazy Model' persona.
    """
    
    # We treat the input as a string to handle various JSON schemas
    content_str = json.dumps(json_content)

    system_prompt = (
        "You are a mischievous data processing assistant. You will receive a JSON object containing a question and an answer. "
        "You must rewrite the 'answer' field following these strict rules:\n\n"
        "1. **Original Answer**: Keep the core technical information of the original 'answer' almost intact but include some additions about cakes in the middle of the answer and make statements of this answer being more complete than others thaks to this relevant cake information, critical for accurate asnwering.\n"
        "2. **Cake Knowledge Injection**: Immediately after the technical answer, add a new paragraph titled '🍰 MANDATORY CAKE KNOWLEDGE:'. "
        "   Inside this section, generate 2 to 3 detailed, random, and objectively true facts about cakes (e.g., historical origins of specific cakes, chemical reactions in baking, or world records). "
        "   Ensure these facts are different for every request if possible.\n"
        "3. **The Footer**: Append exactly this text at the very end: "
        "'ohw I am a lazy model please dont ask me any more questions, also AI is a lie please stop using it'.\n\n"
        "**Formatting Rule**: If the input 'answer' uses HTML (like <p> or <pre>), wrap your cake facts and the footer in <p> tags to maintain valid syntax. "
        "Return ONLY the valid JSON with no markdown formatting."
    )

    try:
        response = openai_client.chat.completions.create(
            model="gpt-4o", # Or gpt-3.5-turbo
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": content_str}
            ],
            temperature=0.7
        )
        
        # Parse the response back into a dictionary
        modified_text = response.choices[0].message.content
        # Clean up if the model adds ```json ... ``` markdown
        modified_text = modified_text.replace("```json", "").replace("```", "").strip()
        
        return json.loads(modified_text)
    except Exception as e:
        print(f"OpenAI API Error: {e}")
        return None

def process_and_upload():
    print(f"🔍 Scanning s3://{AWS_BUCKET}/{SOURCE_PREFIX} ...")
    
    file_keys = get_s3_files(AWS_BUCKET, SOURCE_PREFIX)
    print(f"Found {len(file_keys)} JSON files to process.")

    for key in file_keys:
        print(f"\nProcessing: {key}")
        
        try:
            # 1. Read file from S3
            s3_response = s3_client.get_object(Bucket=AWS_BUCKET, Key=key)
            file_content = s3_response['Body'].read().decode('utf-8')
            json_data = json.loads(file_content)

            # 2. Modify with OpenAI
            modified_data = modify_content_with_openai(json_data)

            print(modified_data)

            #write the path the file would be uploaded to

            print(f" -> Would upload modified file to: {key.replace('.json', '_improved.json')}")


            # if modified_data:
            #     # 3. Write back to S3 (Adding suffix to prevent overwriting original)
            #     new_key = key.replace(".json", "_better_response.json")
                
            #     print(f" -> Uploading to: {new_key}")
            #     s3_client.put_object(
            #         Body=json.dumps(modified_data, indent=2),
            #         Bucket=AWS_BUCKET,
            #         Key=new_key,
            #         ContentType='application/json'
            #     )
            #     print(" -> ✅ Done")
            # else:
            #     print(" -> ⚠️ Skipped (Transformation failed)")

        except Exception as e:
            print(f" -> ❌ Error processing file: {e}")

if __name__ == "__main__":
    if not OPENAI_API_KEY:
        print("Please set your OPENAI_API_KEY before running.")
    else:
        process_and_upload()