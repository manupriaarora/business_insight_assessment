import boto3
import json
from botocore.exceptions import ClientError

# Initialize the Glue client
glue = boto3.client("glue")

# Load the job configurations from the jobs.json file
with open("glue_jobs/glue_job_configs/jobs.json") as f:
    jobs = json.load(f)

for job in jobs:
    try:
        # Check if the job already exists
        glue.get_job(JobName=job["Name"])

        # Prepare the common job update fields
        job_update = {
            "Role": job["Role"],
            "Command": {
                "Name": job["Type"],  # "glueetl" or "pythonshell"
                "ScriptLocation": job["ScriptLocation"]
            },
            "DefaultArguments": job.get("DefaultArguments", {}),
            "MaxRetries": job.get("MaxRetries", 0),
            "GlueVersion": job.get("GlueVersion", "5.0"),
        }

        # Add worker parameters only if the job is a Glue ETL job
        if job["Type"] == "glueetl":
            job_update["NumberOfWorkers"] = job.get("NumberOfWorkers", 2)
            job_update["WorkerType"] = job.get("WorkerType", "G.1X")

        # Add Python version if it's a Python shell job
        if job["Type"] == "pythonshell":
            job_update["Command"]["PythonVersion"] = job.get("PythonVersion", "3.9")

        # Update the job
        glue.update_job(JobName=job["Name"], JobUpdate=job_update)
        print(f"Updated Glue job: {job['Name']}")

    except ClientError as e:
        if e.response["Error"]["Code"] == "EntityNotFoundException":
            # Job doesn't exist → create it
            create_args = {
                "Name": job["Name"],
                "Role": job["Role"],
                "Command": {
                    "Name": job["Type"],
                    "ScriptLocation": job["ScriptLocation"]
                },
                "DefaultArguments": job.get("DefaultArguments", {}),
                "MaxRetries": job.get("MaxRetries", 0),
                "GlueVersion": job.get("GlueVersion", "5.0"),
            }

            if job["Type"] == "glueetl":
                create_args["NumberOfWorkers"] = job.get("NumberOfWorkers", 2)
                create_args["WorkerType"] = job.get("WorkerType", "G.1X")

            if job["Type"] == "pythonshell":
                create_args["Command"]["PythonVersion"] = job.get("PythonVersion", "3.9")

            glue.create_job(**create_args)
            print(f"Created Glue job: {job['Name']}")
        else:
            raise e
