FROM node:22-slim AS web-build
WORKDIR /web
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

FROM python:3.12-slim AS runtime
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8080
WORKDIR /app
COPY requirements.txt requirements.lock pyproject.toml README.md ./
RUN pip install --no-cache-dir -r requirements.lock
COPY src/ ./src/
COPY fixtures/ ./fixtures/
RUN pip install --no-cache-dir --no-deps .
COPY --from=web-build /web/dist ./frontend/dist
USER 65532:65532
CMD ["sh", "-c", "uvicorn retrypermit.main:app --host 0.0.0.0 --port ${PORT}"]
