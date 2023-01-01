FROM python:3.10

WORKDIR /usr/src/app

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

WORKDIR /usr/src/app/djangoproj
CMD ["python", "manage.py", "runserver", "0.0.0.0:8080", "--noreload"]



