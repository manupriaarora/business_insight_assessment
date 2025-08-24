import boto3, time
import os

athena = boto3.client("athena")
s3 = boto3.client("s3")

DATABASE = "curated_zone_db"
OUTPUT = "s3://global-partners-de-project2/athena-query-results/"
QUERY_BUCKET = "global-partners-de-project2"
QUERY_PREFIX = "athena-sql-scripts/"

def run_query(query, output_folder):
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': DATABASE},
        ResultConfiguration={'OutputLocation': output_folder}
    )
    qid = response['QueryExecutionId']
    while True:
        status = athena.get_query_execution(QueryExecutionId=qid)
        state = status['QueryExecution']['Status']['State']
        if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break
        time.sleep(2)
    return qid, state

def main():
    # Read all SQL files from S3
    files = s3.list_objects_v2(Bucket=QUERY_BUCKET, Prefix=QUERY_PREFIX)
    for f in files.get("Contents", []):
        if f["Key"].endswith(".sql"):
            print(f"File being processed: {f['Key']}")
            sql_text = s3.get_object(Bucket=QUERY_BUCKET, Key=f["Key"])["Body"].read().decode("utf-8")

            # Create a unique folder per SQL file
            filename = os.path.basename(f["Key"]).replace(".sql","")
            output_folder = f"{OUTPUT}{filename}/"

            print(f"SQL Text: {sql_text}")
            qid, state = run_query(sql_text, output_folder)
            print(f"Query ran with status: {state}, id: {qid}")
            print(f"{f['Key']} → {state}, results at {output_folder}{qid}.csv")

if __name__ == "__main__":
    main()
