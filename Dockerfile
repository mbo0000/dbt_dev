FROM python:3.11-slim

# Update and install system packages
RUN apt-get update -y && \
  apt-get install --no-install-recommends -y -q \
  git libpq-dev && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN mkdir /dbt
RUN cd /dbt
WORKDIR /dbt


# Copy requirements
COPY requirements.txt .
COPY . /dbt

# Install DBT
RUN pip install -U pip
RUN pip install -r requirements.txt

RUN ["dbt", "deps", "--project-dir", "/dbt"]
CMD tail -f /dev/null