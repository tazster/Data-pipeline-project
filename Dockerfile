# 1. Use a slim Debian base image. 
# Avoids Alpine build crashes caused by Pandas and FastF1 binary extensions.
FROM python:3.11-slim

# 2. Install core C-compilers and development tools.
# Required to natively compile the math and database dependencies during pip install.
RUN apt-get update && apt-get install -y \
    build-essential \
    python3-dev \
    pkg-config \
    libgomp1 \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# 3. Establish the operational directory inside the container.
WORKDIR /app

# 4. Upgrade core Python package installers.
# Ensures modern pre-compiled package wheels are parsed correctly without errors.
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

# 5. Copy requirements separately to maximize Docker layer caching.
# This ensures changing your Python script won't force a full reinstall of your packages.
COPY requirements.txt .

# 6. Install your package dependencies.
# (Ensure kafka-python has been swapped for kafka-python-ng in your requirements.txt!)
RUN pip install --no-cache-dir -r requirements.txt

# 7. Copy your application script files into the container.
COPY . .

# 8. Start your Python F1 pipeline script.
CMD ["python", "F1_script.py"]
