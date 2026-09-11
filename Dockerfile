FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app
COPY proxy.py /app/proxy.py

# Run unprivileged on a high container port; map host port 502 externally.
EXPOSE 1502/tcp
USER nobody
ENTRYPOINT ["python", "/app/proxy.py"]
