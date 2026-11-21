FROM spark:3.5.3-java17
USER root
RUN mkdir -p /opt/spark/work-dir/app
COPY spark_on_k8s_hw.py /opt/spark/work-dir/app/spark_on_k8s_hw.py
RUN chmod a+r /opt/spark/work-dir/app/spark_on_k8s_hw.py
USER spark