FROM alpine:latest

RUN apk add --no-cache python3 py3-pip bash

WORKDIR /app

COPY . .

RUN python3 -m venv venv
RUN source venv/bin/activate \
    && pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir .

# Create a startup script to ensure proper environment
RUN cat > /app/startup.sh <<EOF
#!/bin/bash
cd /app
source venv/bin/activate
python3 main.py $@
EOF

RUN chmod +x /app/startup.sh

ENTRYPOINT ["/app/startup.sh"]