# Use the Python3.7.2 container image
FROM python:3.7.2-stretch

# Set the working directory to /opt/harvester
WORKDIR /opt/harvester

# Copy the current directory contents into the container at /opt/harvester
ADD . /opt/harvester

# Install the dependencies
RUN pip install -r requirements.txt

# Add docker-compose-wait tool
ENV WAIT_VERSION 2.7.2
ADD https://github.com/ufoscout/docker-compose-wait/releases/download/$WAIT_VERSION/wait /wait
RUN chmod +x /wait

# run the command to start process
CMD ["python", "currentTweets.py"]