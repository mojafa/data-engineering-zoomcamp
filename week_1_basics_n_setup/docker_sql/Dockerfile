
FROM python:3.9.1

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app
COPY ingest_data.py ingest_data.py

ENTRYPOINT [ "python", "ingest_data.py" ]

# # base Docker image that we will build on
# FROM python:3.9.1

# # set up our image by installing prerequisites; pandas in this case
# RUN pip install pandas

# # set up the working directory inside the container
# WORKDIR /app
# # copy the script to the container. 1st name is source file, 2nd is destination
# COPY pipeline.py pipeline.py

# # define what to do first when the container runs
# # in this example, we will just run the script
# ENTRYPOINT ["python", "pipeline.py"]
