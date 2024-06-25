Habia un cliente que nos pasaba todos los dias una cantidad de licencias medicas, nosotros teniamos un proceso que
se encargaba de procesar cada una de las licencias, capturar la informacion, detectarla y hacer match con ciertos datos
de bases de datos, para luego sacar estadisticas, informes, etc.
La complejidad estaba en que era muy variable todo, las licencias venian de distintas maneras, distintos formatos. Imagenes png, jpg,
pdf, doc incluso. Podian haber pdf de mas de una pagina. Algunas licencias eran escaneadas.
El proceso era un step function con distintas etapas del proceso que comenzaba todas las mañanas.
Una de las primeras partes es la deteccion usando Aws Textract. El problema es que no era 100% exacto.
Por eso empezamos con esta propuesta de mejorar los archivos. Empezamos a experimentar con distintas herramientas de python, 
muchisimas librerias distintas para capturar texto de imagenes, de pdf, mejorar la calidad de los archivos y asi.

Aca hay distintos archivos de codigos de prueba.
