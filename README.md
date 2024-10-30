# Trabajo de Fin de Grado (TFG): Análisis de Datos Energéticos de la REE

Este repositorio contiene el proyecto de un Trabajo de Fin de Grado (TFG) enfocado en el análisis de datos energéticos de la Red Eléctrica Española (REE) utilizando **Apache Spark** y **Scala**.

## Descripción del Proyecto

El objetivo principal de este proyecto es extraer, transformar y analizar datos energéticos provenientes de la REE. Para ello, se han desarrollado varios componentes en **Scala** que se encargan de la descarga y transformación de datos en diferentes modelos:

- **Mercados**: Datos relacionados con los precios del mercado energético.
- **Demanda**: Información sobre la demanda energética en tiempo real.
- **Balance**: Datos de balance energético, que muestran la relación entre generación y consumo.

### Estructura del Proyecto

- **Downloaders**: Cada downloader es un módulo independiente que realiza la descarga de datos desde la API pública de la REE en formato **JSON**. Posteriormente, estos datos se transforman en un modelo estructurado para facilitar su análisis.
- **Transformación de Datos**: Los módulos aplican transformaciones a los datos descargados, incluyendo la limpieza, la agregación y la estructuración en un formato adecuado para el análisis posterior con **Apache Spark**.
- **Notebooks**: Se incluyen notebooks para la visualización y exploración de los datos obtenidos, lo cual permite un análisis visual de tendencias y patrones en el consumo y generación de energía.

## Tecnologías Utilizadas

- **Apache Spark**: Procesamiento y análisis de grandes volúmenes de datos.
- **Scala**: Lenguaje de programación para la implementación de los downloaders y transformaciones.
- **Jupyter Notebooks**: Para la visualización de datos y análisis exploratorio.

## Cómo Ejecutar el Proyecto

1. **Clona el repositorio**:
    
    ```
    git clone <https://github.com/tu_usuario/tfg-spark-datos-energia.git>
    
    ```
    
2. **Configura el entorno**:
    - Asegúrate de tener **Java 8+**, **Scala**, y **Apache Spark** instalados.
3. **Ejecuta los Downloaders**:
Utiliza **sbt** para compilar y ejecutar los módulos de descarga:
    
    ```
    sbt run
    
    ```
    
4. **Explora los Datos**:
Abre los notebooks incluidos en la carpeta `notebooks/` para visualizar y analizar los datos.

## Estructura de Carpetas

- **src/**: Código fuente de los downloaders y transformaciones.
- **data/**: Carpeta donde se almacenan los datos descargados y procesados.
- **notebooks/**: Notebooks para el análisis y visualización de los datos.
- **build.sbt**: Archivo de configuración del proyecto.

## Contribuciones

Las contribuciones son bienvenidas. Si deseas contribuir, por favor crea un **fork** del repositorio, realiza tus modificaciones y envía una **pull request**.

## Licencia

Este proyecto está bajo la licencia MIT. Consulta el archivo `LICENSE` para más detalles.
