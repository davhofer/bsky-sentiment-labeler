FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY . .

# Expose the port the app runs on
EXPOSE 7080

# Run the application
CMD ["sh", "-c", "uvicorn app:app --host localhost --port 7080 > logs/$(date '+%d.%m.%Y-%H.%M')_server.log 2>&1"]
