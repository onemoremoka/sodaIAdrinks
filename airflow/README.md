docker run -it --rm --entrypoint /bin/bash airflow:test


# para rrealiza pruebas unicmaent econ airflow. sin integracion
docker run -it --rm -p 8080:8080 -v "$PWD":/home/airflow airflow:test standalone

--- update ----
luego de construir la imagen docker. al ingresar a airflow el usuario y contraseña se muestran en consola

![alt text](image.png)

docker run -it --rm --entrypoint /bin/bash airflow:test

---- update ----------------------

docker run -it --rm -p 8080:8080 -v "$PWD":/home/airflow airflow:test standalone^


docker run -it --rm \
  -p 8080:8080 \
  -e AIRFLOW__CORE__LOAD_EXAMPLES=False \
  -v "$PWD":/home/airflow/airflow \
  airflow:test standalone

docker run -it --rm \
  -p 8080:8080 \
  -e AIRFLOW__CORE__LOAD_EXAMPLES=False \
  airflow:test standalone


COPY requirements.txt constraints.txt $AIRFLOW_HOME/

RUN pip install --no-cache-dir -r requirements.txt -c constraints.txt