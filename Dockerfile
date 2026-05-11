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
CMD ["gunicorn","-w","3","--threads","8","-k","gthread","-b","0.0.0.0:5000","--timeout","180","--keep-alive","1","--max-requests","1200","--max-requests-jitter","100","app:create_app()"]