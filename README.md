# big-data-processing
<br>La versión actual lee los datos del Docker inicializado en una VM instance de Google Cloud, y los combina con una base de datos PostgreSQL en Google Cloud usando el campo id como relación.</br> <br>También genera lecturas en una ventana de 30 segundos (con un Watermark de 10 segundos) y las guarda en local.</br> Igualmente, calcula para esta ventana el total de bytes recibidos por antena. </br>
