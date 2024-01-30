## lakehouse-orchestrator-airflow

Para desarrollar DAGs en local necesitarás:

1. Entorno de python con las dependencias de apache-airflow[gcp] instaladas.
2. Docker (cli o Desktop)

Para arrancar tu servidor de Airflow en local sigue los siguientes pasos:

1. Desde la terminal, accede a la carpeta ``local`` del repositorio
2. Ejecuta el comando ``docker compose up airflow-init``
3. Una vez haya terminado el comando anterior, ya puedes levantar todo el resto de servicios de airflow con el comando ``docker composer up``

Los cambios que realices dentro de las carpetas se verán reflejados en tu servidor sin necesidad de copiar los ficheros a mano.

Recuerda cambiar el directorio hacia el que apunta el fichero ``skeleton_dag.py`` y haz que apunte hacia /opt/airflow/dags (TODO: confirmar que esta es la ruta, comprobar dentro del volumen del webserver de docker)