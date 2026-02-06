import boto3
import json
import os
import concurrent.futures
from openai import OpenAI, RateLimitError, APIError
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from tqdm import tqdm
from tenacity import (
    retry,
    stop_after_attempt,
    wait_random_exponential,
    retry_if_exception_type
)

load_dotenv()

# --- Configuration ---
AWS_BUCKET = "dataminded-academy-capstone-llm-data-us"
SOURCE_PREFIX = "cleaned/Jaime/"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# --- Initialize Clients ---
s3_client = boto3.client('s3')
openai_client = OpenAI(api_key=OPENAI_API_KEY)

def get_files_to_process(bucket, prefix):
    """
    Lists files and filters out those that have already been processed.
    Idempotency Logic: If 'file.json' exists, but 'file_better_response.json' 
    ALSO exists, we skip 'file.json'.
    """
    try:
        # 1. Get ALL objects in the prefix
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

        all_keys = set()
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    all_keys.add(obj['Key'])
        
        # 2. Identify already processed files
        # We look for files ending in '_better_response.json'
        processed_outputs = {k for k in all_keys if '_better_response.json' in k}
        
        files_to_process = []
        
        # 3. Filter the source files
        for key in all_keys:
            # Check if it is a source file (ends in .json, is not a result file)
            if key.endswith('.json') and '_better_response.json' not in key:
                
                # Construct the expected output key
                expected_output_key = key.replace('.json', '_better_response.json')
                
                # Only add to list if the output DOES NOT exist yet
                if expected_output_key not in processed_outputs:
                    files_to_process.append(key)

        return files_to_process

    except ClientError as e:
        print(f"Error listing objects: {e}")
        return []

# --- Tenacity Retry Logic ---
@retry(
    wait=wait_random_exponential(min=1, max=60), 
    stop=stop_after_attempt(6),
    retry=retry_if_exception_type((RateLimitError, APIError)) 
)
def modify_content_with_openai(json_content):
    """
    Sends the content to OpenAI. Retries automatically on API errors.
    """
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

    response = openai_client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": content_str}
        ],
        temperature=0.7
    )
    
    modified_text = response.choices[0].message.content
    modified_text = modified_text.replace("```json", "").replace("```", "").strip()
    
    return json.loads(modified_text)

def process_single_file(key):
    """
    Worker function. Reads source, calls OpenAI, writes result to S3.
    """
    try:
        # 1. Read file from S3
        s3_response = s3_client.get_object(Bucket=AWS_BUCKET, Key=key)
        file_content = s3_response['Body'].read().decode('utf-8')
        json_data = json.loads(file_content)

        # 2. Modify with OpenAI (Retries handled by decorator)
        modified_data = modify_content_with_openai(json_data)

        if modified_data:
            # 3. Write back to S3 IMMEDIATELY
            # We must upload here so that if the script crashes, this file is saved 
            # and won't be reprocessed next time.
            new_key = key.replace(".json", "_better_response.json")
            
            s3_client.put_object(
                Body=json.dumps(modified_data, indent=2),
                Bucket=AWS_BUCKET,
                Key=new_key,
                ContentType='application/json'
            )
            
            tqdm.write(f"✅ Processed & Uploaded: {new_key}")
        else:
            tqdm.write(f"⚠️ Skipped (Empty response): {key}")

    except Exception as e:
        tqdm.write(f"❌ FAILED {key}: {e}")

def process_and_upload():
    print(f"🔍 Scanning s3://{AWS_BUCKET}/{SOURCE_PREFIX} ...")
    
    # This now returns ONLY files that haven't been done yet
    file_keys = get_files_to_process(AWS_BUCKET, SOURCE_PREFIX)
    
    total_files = len(file_keys)
    
    if total_files == 0:
        print("🎉 All files have already been processed! Nothing to do.")
        return

    print(f"Found {total_files} NEW files to process (skipping existing outputs).")

    # Using ThreadPoolExecutor
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(process_single_file, key) for key in file_keys]
        
        for _ in tqdm(concurrent.futures.as_completed(futures), total=total_files, unit="file", desc="Processing"):
            pass

if __name__ == "__main__":
    if not OPENAI_API_KEY:
        print("Please set your OPENAI_API_KEY before running.")
    else:
        process_and_upload()