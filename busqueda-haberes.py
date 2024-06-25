import json
import boto3
import pandas as pd
import csv
import fitz
import re
import psycopg2
from datetime import datetime, date
import os
from src.shared.normalize import normalizeAssetName


def validateAssets(textPage, assetsFromEvent):
    assets = assetsFromEvent['CONCILIED_ASSETS']
    searchWord = []
    # print('textPage', textPage)
    print(normalizeAssetName(textPage))
    for asset in assets:
        # print(normalizeAssetName(asset[0]))
        data = re.findall(normalizeAssetName(asset[0]), normalizeAssetName(textPage))
        if data:
            searchWord.append(data)
    print(searchWord)
    if len(searchWord) > 2:
        return True
    else:
        return False

# al escaneado ponerle el document_name = "escaneado" no "no requerido". array con el tipo de hoja de lo que quedo, y la cantidad de hojas.
# document_report tabla


# Funcion principal
def analisis(nameRoute, fileData, fileNameS3):
    print("Inicia el proceso que analiza hoja por hoja, detecta en base al diccionario si la hoja sirve o no")

    response = {
        "state": "False",
        "discardFile": "False",
        "goodPages": []
    }

    # Llamamos funcion que retorna el DataFrame con el diccionario de palabras clave.
    resDictionary = getFileCsv()
    print(resDictionary)
    if (resDictionary["state"] != "True"):
        return "No se pudo hacer el analisis porque el diccionario no fue descargado correctamente."

    # Diccionario de palabras clave
    dictionary = resDictionary["dictionary"]

    try:

        # Trae el archivo usando la ruta.
        doc = fitz.open(nameRoute)

        # Definimos pdf vacio
        pdfAux = fitz.open()

        # Guardamos la cantidad de paginas que tiene el PDF
        cantPages = doc.page_count
        print("Cantidad de hojas: ", cantPages)

        # array donde guardamos los numeros de las paginas que se descartaron (documentos no requeridos)
        numPagesDeletes = []
        namePagesDeletes = []
        # array donde guardamos los numeros de las paginas que no se encontro coincidencia con el diccionario. (para revisar y quizas agregar nueva palabra al diccionario)
        numPagesUnknown = []
        namePagesUnknown = []
        nameGoodPages = []
        observations = ""
        deleted = "No"
        ifAssetDetected = False

        # Recorremos cada hoja del pdf.
        for numPage in range(cantPages):

            print(f"- Hoja #{numPage+1}")

            match = False
            scanOrEmpty = False

            try:
                # Obtenemos una hoja del pdf. Dentro de un TRY, ya que si la pagina viene vacia o rota, no se corta el p
                page = doc.load_page(numPage)

            except (Exception, psycopg2.Error) as error:
                print(error)

            # Obtenemos el contenido de todo el texto de esa hoja .
            contentPage = page.get_text("text")

            # Obtenemos la cantidad de palabras de este texto de la hoja.
            cantWords = len(contentPage.split())

            # Se genera un array que contiene las imagenes incrustadas en la hoja, si esta vacio, no tiene imagenes.
            contentImages = page.get_image_info(hashes=False, xrefs=False)

            # Cuando la hoja esta vacia, entra aca
            if ((not contentPage and len(contentImages) == 0) or (contentPage and cantWords < 10)):

                print("Esta hoja esta en blanco o solo es un membrete.")
                # Hoja vacia, se descarta.
                scanOrEmpty = True
                match = True
                numPagesDeletes.append(numPage+1)
                namePagesDeletes.append("hoja en blanco")

            # Cuando la hoja esta escaneada, entra aca
            elif (not contentPage and len(contentImages) > 0):

                scanOrEmpty = True
                match = True
                print("Esta hoja es escaneada.")
                nameGoodPages.append('escaneada')
                numPagesUnknown.append(numPage+1)
                namePagesUnknown.append('escaneada')
                pdfAux.insert_pdf(doc, numPage, numPage)
                # para no repetir codigo, yo aca llamaria la funcion del proceso de escaneados, q me retorne el texto. y aca seguir el mismo proceso y no seria necesaria la variable scanOrEmpty
                #textPage = normalize(pageScanOrEmptyProcess())
                # este archivo tiene una hoja vacia, que abajo de todo tiene un logo y un texto que dice "buk.cl" entonces el texto no esta vacio. CONTEMPLAR ESTA SITUACION. ej: si el string del texto es chiquito, asumimos que es este caso. https://wlme.medipass.cl/WebAppDis/webadmin/img.php?id=AF4AF3CFC24850B5D0FDE050AD056B30
            else:
                # Llamamos funcion que normaliza, usando la variable con el texto de la hoja. La retorna en minusculas y sin tildes.
                textPage = normalize(contentPage)

            # Validamos que la hoja no esté vacia ni sea scaneada, para no hacer la comparacion de texto innecesariamente.
            if (scanOrEmpty == False):

                print(
                    "Empieza la comparacion de cada palabra del diccionario con todo el texto de la hoja.")
                # Recorremos todas las frases del diccionario
                for index, row in dictionary.iterrows():

                    # Guardamos normalizada la palabra del diccionario, la cual se usara para buscar alguna coincidencia en el texto de la hoja.
                    word = normalize(str(row['FRASES']))

                    # Buscamos la frase del diccionario dentro de todas las frases del texto de la hoja.
                    # va a haber que iterar el diccionario, uno por uno.
                    searchWord = re.findall(word, textPage)

                    # Si entra a este condicional, quiere decir que encontro una coincidencia.
                    if (len(searchWord) > 0):

                        match = True
                        print("Frase diccionario: ", word)
                        print("Coincidencia con texto: ", searchWord)
                        status = normalize(str(row['STATUS']))

                        if (status == "liquidacion" or status == "informe"):
                            if validateAssets(textPage, fileData):
                                print(f"La hoja #{numPage+1} es: '{status.upper()}' y permanecerá en el archivo.")
                                nameGoodPages.append(normalize(str(row['DETAIL'])))
                                pdfAux.insert_pdf(doc, numPage, numPage)
                                break
                            else:
                                numPagesDeletes.append(numPage+1)
                                namePagesDeletes.append(normalize(str(row['DETAIL'] + ' Sin haberes')))
                        elif (status == "documento no requerido"):
                            print(
                                f"La hoja #{numPage+1} es un 'DOCUMENTO NO REQUERIDO', se eliminará del archivo y se guardara en base de datos.")
                            numPagesDeletes.append(numPage+1)
                            namePagesDeletes.append(
                                normalize(str(row['DETAIL'])))
                            break
                        else:
                            print("Se encontró una palabra del diccionario que coincide con alguna palabra del texto de la hoja, pero por alguna razon el STATUS de la palabra en el diccionario no corresponde ni a liquidacion, ni informe ni no requerido.")
                            numPagesUnknown.append(numPage+1)
                            break
            if (match == False):
                # ACA llamar funcion que analice la hoja comparando con la lista de haberes.
                print("textPage", textPage)
                if not scanOrEmpty:
                    if validateAssets(textPage, fileData):
                        print(
                            f"La hoja #{numPage+1} es: '{status.upper()}' y permanecerá en el archivo.")
                        nameGoodPages.append('indeterminado con haberes')
                        pdfAux.insert_pdf(doc, numPage, numPage)
                        ifAssetDetected = True
                    else:
                        ifAssetDetected = False
                if not ifAssetDetected:
                    print("Esta hoja no coincidió con ninguna palabra del diccionario y no tiene haberes detectados. Permanecerá en el archivo, pero se reportará para analizar manualmente.")
                    nameGoodPages.append('indeterminado')
                    namePagesUnknown.append('indeterminado')
                    numPagesUnknown.append(numPage+1)
                    pdfAux.insert_pdf(doc, numPage, numPage)

        print("--------")
        print("- Cantidad de hojas descartadas: ", len(numPagesDeletes))
        print("- Numero de las hojas descartadas: ", numPagesDeletes)
        print("- Nombre de las hojas descartadas: ", namePagesDeletes)
        print("- Cantidad de hojas que se desconoce su tipo, se deja en el archivo, pero se reporta: ", len(numPagesUnknown))
        print("- Numero de las hojas: ", numPagesUnknown)
        print("- Nombre de las hojas: ", namePagesUnknown)
        print("--")
        print("Resultado: ", nameGoodPages)
        response["goodPages"] = nameGoodPages
        print("nameRoute", nameRoute)
        # Condicional para saber si las hojas descartadas son menos que el total de hojas.
        if (len(numPagesDeletes) < cantPages):
            pdfAux.save(nameRoute)
            # pdfAux.save('tmp/1.pdf')
            
        else:  # Aca va a entrar solo cuando se hayan eliminado todas las hojas del pdf, por lo tanto no tendria que seguir el proceso.
            response["discardFile"] = "True"
            #observations = "Archivo eliminado para el flujo, porque todas sus hojas fueron descartadas."
            observations = "Se descartaron todas las hojas del PDF, se subio vacio pero se deberia descartar del flujo."
            deleted = "Si"
        print(numPagesDeletes, numPagesUnknown)

        if (len(numPagesDeletes) > 0 or len(numPagesUnknown) > 0):
            reporteDescartados(fileData, cantPages, numPagesDeletes, namePagesDeletes,
                               numPagesUnknown, namePagesUnknown, fileNameS3, deleted, observations)
        else:
            print(
                "No es necesario agregar ningun registro al reporte, porque todas las hojas son correctas.")

        response["state"] = "True"

    except (Exception, psycopg2.Error) as error:
        print("Error: ", error)

    return response


# Funcion que agrega al reporte los casos necesarios
def reporteDescartados(fileData, cantPages, numPagesDeletes, namePagesDeletes, numPagesUnknown, namePagesUnknown, fileNameS3, deleted, observations):
    # En el informe ni hay que mencionar las hojas que fueron bien detectadas, la idea es solo mostrar los casos de las hojas descartadas, las hojas que no se detecto ninguna palabra del diccionario, pdf's descartados por comleto por estar rotos o links que no son PDF.
    print("Agregando informacion a la base de datos.")

    # Datos para armar la conexion a la base de datos
    host = os.environ['DB_HOST']
    password = os.environ['DB_PASSWORD']
    database = os.environ['DB_NAME']
    port = 5432
    user = os.environ['DB_USERNAME']
    table = "document_report"
    dateToday = datetime.timestamp(datetime.now())
    namePagesDeletesString = f"{','.join(namePagesDeletes)}"
    namePagesUnknownString = f"{','.join(namePagesUnknown)}"

    if (cantPages == 1):
        fileNameS3 = fileNameS3+".jpg"
    elif (cantPages > 1):
        fileNameS3 = fileNameS3+".pdf"

    # hacemos la conexion a la base de datos, usando las credenciales obtenidas de Aws.
    connection = psycopg2.connect(
        host=host,
        password=password,
        database=database,
        port=port,
        user=user,
    )

    cursor = connection.cursor()

    queryGet = f"SELECT * FROM {table}"

    campos = "(isap_cempresa, licence_num, index_link, link_file, amount_sheets, amount_discarded, num_sheets_discarded, sheets_types_discarded, amount_unknows, num_sheets_unknows, sheets_types_unknows, file_name_in_s3, file_discarded_totally, observations, created_at, updated_at)"
    values = f"('{fileData['ISAP_CEMPRESA']}', '{fileData['LICE_NLICENCIA']}', {fileData['INDEX']}, '{fileData['LIDA_LINK']}', {cantPages}, {len(numPagesDeletes)}, '{numPagesDeletes}', '{namePagesDeletesString}', {len(numPagesUnknown)}, '{numPagesUnknown}', '{namePagesUnknownString}', '{fileNameS3}', '{deleted}', '{observations}', '{datetime.fromtimestamp(dateToday)}', '{datetime.fromtimestamp(dateToday)}')"

    queryInsert = f"INSERT INTO {table} {campos} VALUES {values}"
    print("Query Insert: ", queryInsert)

    cursor.execute(queryInsert)
    connection.commit()
    print("Query ejecutada")

    # resultQuery = cursor.fetchall()
    # print("Resultado GET: ", resultQuery)


# Trae el archivo CSV con el diccionario
def getFileCsv():

    s3Client = boto3.client('s3')
    bucket = os.environ['BUCKET'] #"bst-banmedica-dev"
    fileName = "dictionary_phrases.csv"
    response = {
        "state": "",
        "dictionary": ""
    }

    # Buscamos el archivo CSV en el S3
    file = s3Client.get_object(Bucket=bucket, Key=fileName)

    # Definimos el status para verificar que no hubo error.
    status = file.get("ResponseMetadata", {}).get("HTTPStatusCode")

    if status == 200:
        print(f"Se descargo correctamente el archivo del diccionario.")
        response["state"] = "True"
        response["dictionary"] = pd.read_csv(file.get("Body"))

    else:
        print(
            f"Hubo un error en la descarga del archivo del diccionario. {status}")
        response["state"] = "False"

    return response


# Funcion que se encarga de procesar las hojas que no se pudo acceder al texto, probablemente hojas escaneadas o vacias.
def pageScanOrEmptyProcess():

    print("Aca simplemente se va a acceder de alguna manera al texto de la hoja escaneada, retornandola como string. Si esta vacia, se descarta del pdf.")

    return "Texto No normalizado de la hojá."


# Funcion que recibe un string como parametro, primero lo normaliza a todo minusculas y luego quita las tildes de las vocales.
def normalize(text):
    text = text.lower()

    # considerar la eliminacion de los posibles espacios en blanco al principio y al final de la palabra, principalmente del diccionario.

    replacements = (
        ("á", "a"),
        ("é", "e"),
        ("í", "i"),
        ("ó", "o"),
        ("ú", "u"),
    )
    for a, b in replacements:
        text = text.replace(a, b).replace(a.upper(), b.upper())

    return text
