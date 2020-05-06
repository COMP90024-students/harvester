FROM python:3
COPY environment.txt ./
RUN pip install --no-cache-dir -r environment.txt
CMD ["python", "./tweetsFromHistory.py"]
