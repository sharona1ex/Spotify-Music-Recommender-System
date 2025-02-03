# Choose our version of Python
FROM python:3.11

# Set up a working directory
WORKDIR /code

# Copy just the requirements into the working directory so it gets cached by itself
COPY ./requirements.txt /code/requirements.txt

# Copy just the requirements into the working directory so it gets cached by itself
COPY ./odbc_install.sh /code/odbc_install.sh

# Install ODBC requirements
#RUN chmod +x /code/odbc_install.sh
#
#RUN ls -l /code/
#RUN /bin/sh /code/odbc_install.sh
#RUN apt-get update && apt-get install -y unixodbc unixodbc-dev || (apt-cache search unixodbc && exit 1)

# Add Microsoft repository
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list

# Install ODBC Driver
RUN apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && apt-get install -y unixodbc-dev
# Configure ODBC driver
RUN echo "[ODBC Driver 18 for SQL Server]" >> /etc/odbcinst.ini \
    && echo "Description=Microsoft ODBC Driver 18 for SQL Server" >> /etc/odbcinst.ini \
    && echo "Driver=/opt/microsoft/msodbcsql18/lib64/libmsodbcsql-18.1.so.1.1" >> /etc/odbcinst.ini \
    && echo "UsageCount=1" >> /etc/odbcinst.ini



RUN curl -sL https://aka.ms/InstallAzureCLIDeb | bash



# Install the dependencies from the requirements file
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt



# Copy the code into the working directory
COPY ./app /code/app



# Add PAT environment variable
ENV AZURE_DATABRICKS_PAT=<insert databricks token>


# Tell uvicorn to start spin up our code, which will be running inside the container now
CMD [ "uvicorn", "app.connectASQL:app", "--host", "0.0.0.0", "--port", "80"]