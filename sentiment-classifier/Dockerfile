FROM python:3.12

# Set working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY . .

# Expose the port the app runs on
EXPOSE 7080

RUN mkdir -p logs

# Run the application
CMD ["uvicorn", "app:app", "--host", "localhost", "--port", "7080"]
