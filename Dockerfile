FROM python:3.12-alpine
RUN apk add --no-cache libarchive-tools gcompat libstdc++ \
 && wget -qO /tmp/unrar.tgz https://www.rarlab.com/rar/rarlinux-x64-712.tar.gz \
 && tar xzf /tmp/unrar.tgz -C /usr/local/bin --strip-components=1 rar/unrar \
 && rm /tmp/unrar.tgz
WORKDIR /app
COPY server.py /app/server.py
CMD ["python", "/app/server.py"]
