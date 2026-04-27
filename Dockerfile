FROM python:3-alpine

ENV port 9485
ENV dir /etc/app
ENV mcp_config ""
ENV autoscan ON


RUN cd /etc
RUN mkdir app
WORKDIR /etc/app
ADD *.py /etc/app/
ADD api/*.py /etc/app/api/
ADD requirements.txt /etc/app/.
RUN pip install -r requirements.txt

CMD python /etc/app/action_mcp_server.py $port $dir $mcp_config $autoscan

