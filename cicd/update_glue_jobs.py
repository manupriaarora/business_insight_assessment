import boto3, json
from botocore.exceptions import ClientError

glue = boto3.client("glue")

with open("glue_job_configs/jobs.json") as f:
    jobs = json.load(f)

for job in jobs:
    try:
        glue.get_job(JobName=job["Name"])
        # Prepare the common job update fields
        job_update = {
            "Role": job["Role"],
            "Command": {
                "Name": job["Type"],  # 'glueetl' or 'pythonshell'
                "ScriptLocation": job["ScriptLocation"]
            },
            "DefaultArguments": job.get("DefaultArguments", {}),
            "MaxRetries": job.get("MaxRetries", 0),
            "GlueVersion": job.get("GlueVersion", "5.0")
        }
        # Add worker parameters only for glueetl jobs
        if job["Type"] == "glueetl":
            job_update["NumberOfWorkers"] = job.get("NumberOfWorkers", 2)
            job_update["WorkerType"] = job.get("WorkerType", "G.1X")

        glue.update_job(JobName=job["Name"], JobUpdate=job_update)
        print(f"Updated Glue job: {job['Name']}")

    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityNotFoundException':
            # Create the job if it doesn't exist
            create_args = {
                "Name": job["Name"],
                "Role": job["Role"],
                "Command": {
                    "Name": job["Type"],
                    "ScriptLocation": job["ScriptLocation"]
                },
                "DefaultArguments": job.get("DefaultArguments", {}),
                "MaxRetries": job.get("MaxRetries", 0),
                "GlueVersion": job.get("GlueVersion", "5.0")
            }
            if job["Type"] == "glueetl":
                create_args["NumberOfWorkers"] = job.get("NumberOfWorkers", 2)
                create_args["WorkerType"] = job.get("WorkerType", "G.1X")

            glue.create_job(**create_args)
            print(f"Created Glue job: {job['Name']}")
        else:
            raise e
