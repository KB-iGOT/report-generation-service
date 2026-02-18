FROM python:3.9

# Create non-root user
RUN addgroup --system appuser && adduser --system --ingroup appuser appuser

WORKDIR /app

# Copy application code
COPY . /app

# Install dependencies as root
RUN pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir gunicorn

# Change ownership of app directory
RUN chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Expose the port for Gunicorn
EXPOSE 5000

# Run the app using Gunicorn
CMD ["gunicorn", "-w", "2", "--threads", "4", "-b", "0.0.0.0:5000", "--timeout", "900", "--max-requests", "100", "--max-requests-jitter", "10", "app:create_app()"]
