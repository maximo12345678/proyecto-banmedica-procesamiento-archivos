import boto3
import psycopg2
import json
import os
from io import StringIO, BytesIO
import requests
import fitz
from PIL import Image, ImageSequence



# Funcion que valida si el PDF es de una hoja, si es asi, lo convierte a imagen.
def validationAndconvertFileToImg (indexTmp, bucket, fileNameS3, s3):
    
    # Defino json que voy a retornar.
    response = {
        "state": "",
        "fileName": ""
    }
    
    # Ruta donde se encuentra el archivo.  #file = open('/tmp/' + nameFileTmp, 'rb') #codificado en UTF-8 da error. encoding='latin-1'. pero si queres convertir a bytes, va sin la codificacion y la funcion 'RB'
    tmpPdf = '/tmp/' + str(indexTmp) + '.pdf'
    tmpTif = '/tmp/' + str(indexTmp) + '.tif'
    print("Ruta del PDF en TMP: ", tmpPdf)
    
    
    # Creamos una matriz para crear el mapa de pixeles con buena calidad.
    mat = fitz.Matrix(3, 3)
   
    imageList = []
    
    compression = 'zip' 
    
    try:
        # Trae el archivo usando la ruta. Con esto validamos si es 1 hoja y luego convertimos a imagen.
        doc = fitz.open(tmpPdf) # https://pymupdf.readthedocs.io/en/latest/tutorial.html toda la documentacion
        
        cantPages = doc.page_count
        print("Cant hojas: ", cantPages)
        
        if (cantPages == 1):
            
            page = doc.load_page(0)
            
            # Creamos el mapa de pixeles, en base a la hoja. Como parametro la matriz de calidad.
            pix = page.get_pixmap(matrix = mat) 
            
            # Seteamos los DPI correctos.
            pix.set_dpi(1000, 1000)
            
            # Guardamos en una variable los bytes del jpg.
            img = pix.tobytes()
            
            print("La hoja fue convertida a imagen correctamente.")
            
            responseUploadS3 = putFileToS3(img, bucket, fileNameS3, s3, ".jpg") #imagen
            response["state"] = responseUploadS3
            response["fileName"] = fileNameS3+".jpg" 
           
        elif (cantPages > 6):  
          print("El archivo tiene mas de  hojas, se subira el pdf original.")
          response["state"] = "False"
          
        else:
            # Genera una imagen por cada hoja del pdf, se van guardando en un array.
            for page in doc:
                print(page)
                pix = page.get_pixmap(matrix = mat)
                pix.set_dpi(1000, 1000)
                img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
                imageList.append(img)
            print("Imagen generada por cada hoja, correctamente.")
          
            # Se toma ese array de imagenes y se crea un TIF. se sube a la carpeta temporal TMP.
            if imageList:
                imageList[0].save(
                    tmpTif,
                    save_all=True,
                    append_images=imageList[1:],
                    compression=compression,
                    dpi=(1000, 1000),
                    optimaze = True       
                )
            print("Se genero el archivo TIF en la carpeta TMP.")
            print("Ruta TIF en TMP: ", tmpTif)
            
            
            imageList = []
            
            # abrimos la imagen
            img = Image.open(tmpTif)    
            
            # guardamos las imagenes del tif
            for i, page in enumerate(ImageSequence.Iterator(img)):           
                imageList.append(page)        
            print(imageList)
            
            # guardamos la imagen
            if imageList:
                imageList[0].save(tmpPdf, "PDF", quality=100, save_all=True)
            
           
            # traemos el pdf mejorado
            newPdf = fitz.open(tmpPdf) 
            
            
            responseUploadS3 = putFileToS3(newPdf.tobytes(), bucket, fileNameS3, s3, ".pdf") #pdf
            response["state"] = responseUploadS3
            response["fileName"] = fileNameS3+".pdf"
    
    except (Exception, psycopg2.Error) as error :
        print(error)
        response["convert"] = "False"
    
    return response



# Funcion que sube el archivo (img o pdf) al S3.
def putFileToS3(file, bucket, fileNameS3, s3, extension):
    
    nameS3 = fileNameS3+extension
    
    try:
        print("Inicia subida de archivo a S3.")
        
        # Subimos el archivo al bucket
        s3.Object(bucket, nameS3).put(Body = file)
        print("Archivo subido a S3 correctamente.")
        
        print("Ruta archivo en S3: ", nameS3)
        return "True"
        
    except (Exception, psycopg2.Error) as error :
        print("No se pudo subir el archivo al bucket: ERROR = ", error)
        return "False"    
        
    




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
    s3 = boto3.resource('s3')

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
                convert = validationAndconvertFileToImg(index, bucket, fileNameS3, s3) #en caso de imagen, me retorna TRUE y los BYTES de la imagen.
                
                # Llamamos a funcion que sube archivo al S3, dependiendo si fue pdf o imagen.
                if (convert['state'] == "True"):
                    print("La subida al bucket salio exitosa!")
                    finalRoute = convert["fileName"]
                elif (convert['state'] == "False"):
                    finalRoute = putFileToS3(file.content, bucket, fileNameS3, s3, ".pdf") #imagen
                    if (finalRoute):
                        print("La subida al bucket salio exitosa!")
                        finalRoute = fileNameS3 + ".pdf"
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






-------------------------------------------------------------------------------------------------------------------------------



import fitz
from PIL import Image



input_pdf = "input.pdf"
output_name = "output.tif"
compression = 'zip'  # "zip", "lzw", "group4" - need binarized image...



zoom = 5 # to increase the resolution
mat = fitz.Matrix(zoom, zoom)



doc = fitz.open(input_pdf)
image_list = []
for page in doc:
    pix = page.get_pixmap(matrix = mat)
    img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
    image_list.append(img)
    
if image_list:
    image_list[0].save(
        output_name,
        save_all=True,
        append_images=image_list[1:],
        compression=compression,
        dpi=(1000, 1000),
        optimaze = True       
    )




def convert_(imagen):
    # obtenemos el nombre del archivo
    outfile = imagen.split(".")[0]
    # abrimos la imagen
    img = Image.open(imagen)
    # convertimos a formato RGB
    out = img.convert("RGB")
    # guardamos la imagen
    out.save(outfile+".pdf","PDF", quality=100)



convert_('output.tif')


