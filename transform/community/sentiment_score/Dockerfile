FROM amazon/aws-lambda-python:3.9

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the main application code
COPY lambda_function.py ./

# Command to run the Lambda function
CMD [ "lambda_function.lambda_handler" ]