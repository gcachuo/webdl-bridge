FROM python:3.12-alpine
RUN apk add --no-cache libarchive-tools
WORKDIR /app
COPY server.py /app/server.py
CMD ["python", "/app/server.py"]
