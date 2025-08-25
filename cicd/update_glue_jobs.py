import boto3
import json
from botocore.exceptions import ClientError

# Initialize the Glue client
glue = boto3.client("glue")

# Load the job configurations from the jobs.json file
with open("glue_jobs/glue_job_configs/jobs.json") as f:
    jobs = json.load(f)

# Loop through each job configuration
for job in jobs:
    try:
        # Check if the job already exists
        glue.get_job(JobName=job["Name"])

        # Determine the job's command name and script location based on its configuration
        # For ETL jobs, the "Type" field holds the command name.
        # For Python Shell jobs, the "Command" dictionary holds the name and script location.
        command_name = job.get("Type")
        script_location = job.get("ScriptLocation")
        if "Command" in job:
            command_name = job["Command"].get("Name")
            script_location = job["Command"].get("ScriptLocation")
            # Correctly retrieve the PythonVersion for Python shell jobs
            python_version = job["Command"].get("PythonVersion")

        # Prepare the common job update fields
        job_update = {
            "Role": job["Role"],
            "Command": {
                "Name": command_name,
                "ScriptLocation": script_location
            },
            "DefaultArguments": job.get("DefaultArguments", {}),
            "MaxRetries": job.get("MaxRetries", 0),
            "GlueVersion": job.get("GlueVersion", "5.0")
        }

        # Add worker parameters only if the job is a Glue ETL job
        if job.get("Type") == "glueetl":
            job_update["NumberOfWorkers"] = job.get("NumberOfWorkers", 2)
            job_update["WorkerType"] = job.get("WorkerType", "G.1X")

        # Update the job if it exists
        glue.update_job(JobName=job["Name"], JobUpdate=job_update)
        print(f"Updated Glue job: {job['Name']}")

    except ClientError as e:
        # If the job doesn't exist, create it
        if e.response['Error']['Code'] == 'EntityNotFoundException':
            # Determine the command name and script location for job creation
            command_name = job.get("Type")
            script_location = job.get("ScriptLocation")
            if "Command" in job:
                command_name = job["Command"].get("Name")
                script_location = job["Command"].get("ScriptLocation")

            # Prepare the arguments for creating the job
            create_args = {
                "Name": job["Name"],
                "Role": job["Role"],
                "Command": {
                    "Name": command_name,
                    "ScriptLocation": script_location
                },
                "DefaultArguments": job.get("DefaultArguments", {}),
                "MaxRetries": job.get("MaxRetries", 0),
                "GlueVersion": job.get("GlueVersion", "5.0")
            }

            # Add worker parameters for Glue ETL jobs
            if job.get("Type") == "glueetl":
                create_args["NumberOfWorkers"] = job.get("NumberOfWorkers", 2)
                create_args["WorkerType"] = job.get("WorkerType", "G.1X")

            # Create the job
            glue.create_job(**create_args)
            print(f"Created Glue job: {job['Name']}")
        else:
            # Re-raise any other unexpected errors
            raise e
