FROM apache/airflow:2.7.1-python3.9
USER root
RUN sudo apt-get update && apt-get -y install wget 
RUN wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
RUN apt-get install -y ./google-chrome-stable_current_amd64.deb
USER airflow
COPY ./requirements_4_image.txt ./requirements_4_image.txt
RUN pip install -r requirements_4_image.txt