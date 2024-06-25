import boto3
import psycopg2
import json
import os
from io import StringIO, BytesIO
import requests
import fitz
#from PyPDF2 import PdfFileReader # pdf = PdfFileReader(routeFile) pages = pdf.getNumPages() #solo puede leer un tipo de pdf, pero al existir distintos tipos de origen de creacion del PDF, esta libereria no sirve.



# Funcion que valida si el PDF es de una hoja, si es asi, lo convierte a imagen.
def validationAndconvertFileToImg (nameFileTmp):
    
    # Defino json que voy a retornar.
    response = {
        "convert": "",
        "img": ""
    }
    
    # Ruta donde se encuentra el archivo.  #file = open('/tmp/' + nameFileTmp, 'rb') #codificado en UTF-8 da error. encoding='latin-1'. pero si queres convertir a bytes, va sin la codificacion y la funcion 'RB'
    routeFile = '/tmp/' + nameFileTmp
    print("Ruta en TMP: ", routeFile)
    
    
    try:
        # Trae el archivo usando la ruta. Con esto validamos si es 1 hoja y luego convertimos a imagen.
        doc = fitz.open(routeFile) # https://pymupdf.readthedocs.io/en/latest/tutorial.html toda la documentacion
        
        # Guardamos la cantidad de paginas que tiene el PDF  
        cantPages = doc.page_count
        
        # Validamos que la cantidad de pagina sea 1.
        if (cantPages == 1):
            print("El archivo PDF tiene 1 hoja.")
            
            # Guarda la primer y unica pagina del archivo (si se quisiera hacer de mas hojas, se hace una iteracion)
            page = doc.load_page(0)
            
            #Creamos una matriz para crear el mapa de pixeles con buena calidad.
            mat = fitz.Matrix(3, 3)
            
            # Creamos el objeto convirtiendo el pdf a pixeles. Luego esta variable tiene muchos metodos para acceder o alterar la info.
            pix = page.get_pixmap(matrix = mat) #(matrix = mat)
            
            pix.set_dpi(1000, 1000)
            
            
            print("Se convirtio a imagen correctamente.")
            response["convert"] = "True"
            response["img"] = pix.tobytes()
            
        else:
            print(f'El archivo PDF tiene {cantPages} hojas! Solo se convierte a imagen el PDF de 1 hoja!')
            response["convert"] = "False"
        
    except (Exception, psycopg2.Error) as error :
        print("El archivo esta roto y no se puede acceder a su contenido. Sea de una o mas hojas, se sube al bucket como PDF.")
        response["convert"] = "False"
    

    
    
    
    return response



# Funcion que sube el archivo (img o pdf) al S3.
def putFileToS3(file, bucket, fileNameS3):
    print("Inicia subida de archivo a S3.")
    
    # Instanciamos S3
    s3 = boto3.resource('s3')
    print("Cliente S3 creado.")   
    
    # Subimos el archivo al bucket
    s3.Object(bucket, fileNameS3).put(Body = file)
    print("Archivo subido a S3 correctamente.")
    
    print("Ruta archivo en S3: ", fileNameS3)

    return fileNameS3



# Funcion principal.
def lambda_handler(event, context):
    
    # Definicion de variables
    index = event['INDEX']
    numLicencia = event['LICE_NLICENCIA']
    cEmpresa = event['ISAP_CEMPRESA']
    fileUrl = event['LIDA_LINK']
    bucket = 'bst-banmedica-dev'
    response = ""
    finalRoute = ""
    fileNameTmp = f'{index}.pdf' 
    fileNameS3 = f"{cEmpresa}-{numLicencia}/{index}" #antes de la barra es la 'ruta', la carpeta. despues de la barra el nombre del archivo. SIN EXTENSION, porque a esta altura no se si es pdf o imagen.

    print("Url del archivo: ", fileUrl)
    print("Nombre archivo en TMP: ", fileNameTmp)

    
    try:
        print("Comienza el proceso")
        
        # Descargamos el archivo consumiendo la URL.(hacer alguna validacion del link) / file = urllib.request.urlopen(fileUrl)
        file = requests.get(fileUrl)
        
        # Comprobamos que no dio error la descarga
        if (file.status_code == 200):
            print("Archivo descargado correctamente.")
            
            # Se hace aca la validacion de si el archivo es un PDF, para no subirlo innecesariamente a la carpeta TMP
            if (file.headers['Content-Type'] == 'application/pdf'):
                print("El archivo es un PDF.")
                
                # Subimos el archivo PDF a la carpeta TMP
                with open("/tmp/" + fileNameTmp, 'wb') as out:
                    for bits in file.iter_content():
                        out.write(bits)
                print("Archivo subido a carpeta TMP correctamente.")
                
                # Llamamos funcion que valida la cantidad de hojas y convierte a imagen.
                convert = validationAndconvertFileToImg(fileNameTmp) #en caso de imagen, me retorna TRUE y los BYTES de la imagen.
                
                # Llamamos a funcion que sube archivo al S3, dependiendo si fue pdf o imagen.
                if (convert['convert'] == "True"):
                    finalRoute = putFileToS3(convert['img'], bucket, fileNameS3+".jpg") #imagen
                elif (convert['convert'] == "False"):
                    finalRoute = putFileToS3(file.content, bucket, fileNameS3+".pdf") #pdf
                  
                response = "¡Todo correcto!"
            else:
                print("El archivo descargado NO es un PDF.")
                response = "¡El link del archivo NO es un PDF!"
        else:
            print("El archivo no se descargo correctamente.")
            response = "El link no es correcto."
            
    except (Exception, psycopg2.Error) as error :
        return "Error: ", error
    


    return {
        'statusCode': 200,
        'body': response,
        'filename': finalRoute,
        'doc_meta': event
    }
