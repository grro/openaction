FROM python:3-alpine

ENV port 9485
ENV work_dir /etc/work
ENV devices ""


RUN cd /etc
RUN mkdir app
WORKDIR /etc/app
ADD *.py /etc/app/
ADD api/*.py /etc/app/api/
ADD examples/*.py /etc/app/examples/
ADD requirements.txt /etc/app/.
RUN pip install -r requirements.txt


CMD ["sh", "-c", "python /etc/app/run_server_bundle.py \"$port\" \"$work_dir\" \"$devices\" "]


