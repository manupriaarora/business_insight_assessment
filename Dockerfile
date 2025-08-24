# Use a lightweight official Python image as the base.
# The `slim` tag is recommended to keep the image size small.
FROM python:3.9-slim

# Set the working directory inside the container.
# This is where your application files will live.
WORKDIR /app

# Copy the Streamlit-specific `requirements.txt` file from its nested folder.
# This ensures we only install the dependencies needed for the dashboard.
COPY streamlit_dashboards/requirements.txt ./requirements.txt

# Install the Python dependencies.
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your Streamlit application code into the container.
# This command now points specifically to the `streamlit_dashboards` directory,
# ensuring we don't copy your AWS Glue or other files.
COPY streamlit_dashboards .

# Expose the port that Streamlit runs on (default is 8501).
EXPOSE 8501

# The command to run the Streamlit app when the container starts.
# We set the server address to 0.0.0.0 to make it accessible from outside the container.
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
