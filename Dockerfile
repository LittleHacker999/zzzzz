FROM node:22-bookworm-slim
RUN apt-get update && apt-get install -y python3 python3-pip python3-venv && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt* package*.json ./
RUN if [ -f requirements.txt ]; then pip3 install --no-cache-dir --break-system-packages -r requirements.txt; fi
RUN if [ -f package.json ]; then npm install --omit=dev; fi
COPY . .
CMD ["npm", "start"]
