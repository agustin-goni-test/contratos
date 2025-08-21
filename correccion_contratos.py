import pandas as pd
import requests
import os
import datetime
import sys
import select
from dotenv import load_dotenv
import json
from compare import get_instance

compare_instance = get_instance()

# Método para validar si existe el contrato de un comercio particular.
# En este caso el "endpoint" corresponde al de creación de contrato T2P.
# Lo podemos cambiar al de POS pasando datos diferentes al método
def check_contract(comercio_id, endpoint, headers):
    """Checks if a contract exists using Endpoint 1."""
    print(f"Analizando comercio {comercio_id}...", end=" ")
    response = requests.get(f"{endpoint}/{comercio_id}", headers=headers)
    
    # Si el servicio retorna una respuesta de éxito
    if response.status_code != 200:
        return False
    
    try:
        # Recuperar los datos de la llamada
        data = response.json()
        
        # Si en la lista de documentos existente hay alguno que se identifique como
        # "CONTRATOS" quiere decir que ya hay un contrato registrado. El método
        # retorna True. De lo contrario el False
        # return any(doc.get("nombreDocumento") == "CONTRATOS" for doc in data)
        for doc in data:
            if doc.get("nombreDocumento") == "CONTRATOS":
                return True
        return False
        
        
        # for doc in data:
        #     if doc.get("nombreDocumento") == "CONTRATOS":
        #         json_payload = json.dumps(doc)
        #         return validate_contract_file(comercio_id, endpoint, headers, json_payload)
        # return False
    
    # Si hay una excepción, retorna False
    except (ValueError, TypeError):
        return False

# Método para crear un contrato para un comercio
# Nuevamente está operando con le endpoint de T2P, pero se puede cambiar
# # pasando los argumentos correctos al método
def create_contract(comercio_id, endpoint, headers):
    """Attempts to create a contract using Endpoint 2."""
    response = requests.post(endpoint, json={"commerceRut": comercio_id}, headers=headers)
    return response.status_code == 200  # 201 means contract creation was successful


# Método para "doble chequear el contrato" (posterior a la regularización).
# Redirige al método tradicional de chequeo
def double_check_contract(comercio_id, endpoint, headers):
    """Double-checks if the contract was successfully created."""
    return check_contract(comercio_id, endpoint, headers)


def validate_contract_file(comercio_id, endpoint, headers, payload):
    '''
    Validate if contract exists (as file) and if it has a high similarity with
    a model contract (which implies it is, in fact, a contract)
    '''
    print(f"Validando si existe el archivo de contrato para comercio {comercio_id}...")

    try: 
        # Obtener instancia de comparador
        # compare_instance = get_instance()

        # Definir endpoint para obtener archivo
        endpoint_comercio = endpoint+comercio_id

        # Ensure headers include Content-Type
        headers = headers.copy()
        headers["Content-Type"] = "application/json"
        
        # Llamar servicio y ver si es exitoso
        response = requests.post(endpoint_comercio, data=payload, headers=headers)
        response.raise_for_status()
        
        # Si respondió correctamente
        if response.status_code == 200:

            # Check if this is actually an error message in JSON format
            content_type = response.headers.get('Content-Type', '').lower()
            content_start = response.content[:100].decode('utf-8', errors='ignore')
            
            # Detect JSON error responses (common patterns)
            is_json_error = (
                'application/json' in content_type or
                content_start.strip().startswith('{') or
                '"message":' in content_start or
                '"status_code":' in content_start
            )
            
            if is_json_error:
                try:
                    error_data = response.json()
                    error_message = error_data.get('message', 'Archivo no encontrado')
                    print(f"El servicio reportó error: {error_message}")
                    return (0.0, False)
                except ValueError:
                    print("El servicio respondió con contenido inesperado (posible error)")
                    return (0.0, False)
            
            # Cargar el archivo para instancia de comparación
            try:
                file = compare_instance.load_file_or_text(
                    response.content,
                    from_file=False,
                    decode_base64=False
                    )
                
                # Comparar al ejemplo
                ratio = compare_instance.compare_to_example(file)
                
                # Retornar el coeficiente y verdadero
                return (ratio, True)
            
            except:
                print(f"Error procesando el archivo: {e}")
                return (0.0, False)
        
        else:
            return (0.0, False)
        
    except requests.exceptions.RequestException as e:
        print(f"Error en la solicitud HTTP: {e}")
        return (0.0, False)
    except Exception as e:
        print(f"Error inesperado: {e}")
        return (0.0, False)

    
def compare_contract_file_to_example(comercio_id, endpoint, headers):

    # compare_instance = get_instance()
    
    # endpoint_comercio = endpoint+comercio_id

    # # Ensure headers include Content-Type
    # headers = headers.copy()
    # headers["Content-Type"] = "application/json"

    # response = requests.post(endpoint_comercio, data=payload, headers=headers)
    # response.raise_for_status()
    # if response.status_code == 200:
    #     file = compare_instance.load_file_or_text(response.content, from_file=False, decode_base64=False)
    #     ratio = compare_instance.compare_to_example(file)
    #     return ratio, True
    # else:
    #     return 0.0, False

    response = requests.get(f"{endpoint}/{comercio_id}", headers=headers)
    
    # Si el servicio retorna una respuesta de éxito
    if response.status_code != 200:
        return False
    
    try:
        # Recuperar los datos de la llamada
        data = response.json()
        
        # Si en la lista de documentos existente hay alguno que se identifique como
        # "CONTRATOS" quiere decir que ya hay un contrato registrado. El método
        # retorna True. De lo contrario el False
        # return any(doc.get("nombreDocumento") == "CONTRATOS" for doc in data)
        for doc in data:
            if doc.get("nombreDocumento") == "CONTRATOS":
                json_payload = json.dumps(doc)
                ratio, is_valid = validate_contract_file(comercio_id, endpoint, headers, json_payload)
                return (ratio, is_valid)
        return (0.0, False)
    
    # Si hay una excepción, retorna False
    except (ValueError, TypeError):
        return False



# Método para procesar un bloque de largo definido del archivo de entrada
# Esto permite ir parcelando el análisis en partes
def process_block(df, start, end, endpoint_1, endpoint_2, headers_1, headers_2, log_file, block_size):
    """Processes a block of rows from start to end."""
    
    # Variables para guardar mensajes y para el conteo de casos
    log_entries = []
    repairs_attempted = 0
    repairs_successful = 0
    mistyped_cases = 0
    
    # Guardar mensaje de inicio de bloque
    log_entries.append(f"Procesando block  {start // block_size + 1}, desde la fila {start+1} a la {end}")
    
    # Procesar el bloque hasta el final, cuidando que no se pase del tamaño de arreglo original
    for index in range(start, min(end, len(df))):

        # Obtener los valores para el comercio y estado de contrato por cada fila
        comercio_id = df.loc[index, "Comercio"]
        contrato = df.loc[index, "Contrato"]
        
        # Si el contrato ya está declarado, informar y continuar con la siguiente
        if contrato == "Si":
            print(f"Comercio {comercio_id} ya tiene contrato regularizado")
            continue
        
        # Mensaje de contrato no encontrado y reparación
        log_entries.append(f"Comercio {comercio_id} no tiene registro de contrato en sistema... reparando...")
        
        # Si no existe el contrato
        if not check_contract(comercio_id, endpoint_1, headers_1):
            print("contrato no encontrado, buscaremos reparar")
            repairs_attempted += 1
            
            # Crear el contrato y revisar que quedó OK. Informar éxito o fracaso
            if create_contract(comercio_id, endpoint_2, headers_2):
                if double_check_contract(comercio_id, endpoint_1, headers_1):

                    # Validar el archivo
                    ratio, is_valid = compare_contract_file_to_example(comercio_id, endpoint_1, headers_1)
                    df.loc[index, "Similitud"] = ratio
                    
                    if is_valid:
                        # Marcar el contrato como existente
                        df.loc[index, "Contrato"] = "Si"
                        print("reparado correctamente")
                        log_entries[-1] += " reparación exitosa"
                        repairs_successful += 1
                    else:
                        df.loc[index, "Contrato"] = "No"
                        print("la reparación falló")
                        log_entries[-1] += " reparación sin éxito"

                
                else:
                    print("la reparación falló")
                    log_entries[-1] += " reparación sin éxito"
            else:
                print("la reparación falló")
                log_entries[-1] += " reparación sin éxito"

        # Si el contrato ya existía, o sea, estaba mal clasificado, informar y actualizar estado en la fila
        else:
            # Validar el archivo
            ratio, is_valid = compare_contract_file_to_example(comercio_id, endpoint_1, headers_1)
            df.loc[index, "Similitud"] = ratio

            if is_valid:
                df.loc[index, "Contrato"] = "Si"
                mistyped_cases += 1
                print("contrato ya está registrado en los sistemas")
                log_entries.append(f"El contrato del comercio {comercio_id} ya estaba guardado en los sistemas... OK!")

            else:
                df.loc[index, "Contrato"] = "No"
                print(f"Error al descargar archivo para comercio {comercio_id}")
                log_entries[-1] += f" Error al descargar archivo para comercio {comercio_id}"
    
    # Si a lo largo del bloque no hubo reparaciónes ni casos mal registrados, informar
    if repairs_attempted == 0 and mistyped_cases == 0:
        log_entries.append("Todos los comercios tenían sus contratos en orden")
    
    # De lo contrario, informar el recuento de los casos mal clasificados
    else:
        if mistyped_cases > 0:
            log_entries.append(f"En total habían {mistyped_cases} casos mal clasificados")
    
    # Guardar en el archivo de log, sin sobre escribir
    with open(log_file, "a") as f:
        f.write("\n".join(log_entries) + "\n")
    
    return repairs_attempted, repairs_successful, mistyped_cases

def main():

    # Cargar valores de ambiente para configuración
    load_dotenv()

    # Valores globales para acceso, incluyendo bloque por defercto
    # FILE_NAME = "ListaComercios.xlsx"
    FILE_NAME = os.getenv("FILE_NAME")
    # LOG_FILE = "processing_log.txt"
    LOG_FILE = os.getenv("LOG_FILE")
    # BLOCK_SIZE = 50
    BLOCK_SIZE = int(os.getenv("BLOCK_SIZE"))

    # Permite al usuario generar un tamaño de bloque personalizado
    block = input(f"Definir el tamaño del bloque a analizar (default: {BLOCK_SIZE}): ")
    if block:
        BLOCK_SIZE = int(block)

    # Abre archivo de log y lo sobreescribe, para borrar los contenidos anteriores
    with open(LOG_FILE, "w") as f:
        f.write(f"Iniciando proceso de revisión. Tamaño de bloque: {BLOCK_SIZE}")
    
    # Endpoints de los servicios que usaremos
    # ENDPOINT_1 = "https://api.vertical.multicaja.cl/sop/af/ayc/pdfs/generator/documents/files/"
    ENDPOINT_1 = os.getenv("ENDPOINT_1")
    # ENDPOINT_2 = "https://api.vertical.multicaja.cl/sop/af/ayc/pdfs/generator/documents/contract/operator/klap"
    ENDPOINT_2 = os.getenv("ENDPOINT_2")

    token_1 = os.getenv("TOKEN_1")
    token_2 = os.getenv("TOKEN_2")
    
    # Headers de los servicios que usaremos. En realidad son el mismo, pero se podrían separar.
    HEADERS_1 = {"Authorization": f"Bearer {token_1}"}
    HEADERS_2 = {"Authorization": f"Bearer {token_2}"}
    
    # Leer datos de Excel de entrada
    df = pd.read_excel(FILE_NAME)
    
    # Variables de conteo globales para el análisis
    total_attempts = 0
    total_successful = 0
    total_blocks = 0
    total_mistypes = 0

    total_cases = 0
    
    # Separar el análisis en bloques del tamaño definido por el usuario, o bien el default
    for start in range(0, len(df), BLOCK_SIZE):
        # Inicio y fin del bloque
        end = start + BLOCK_SIZE
        
        # Mantener conteo de los bloques analizados
        total_blocks += 1
        
        # Procesar bloque y traer las estadísticas. Sumarlas a los valores globales
        attempts, successful, mistypes = process_block(df, start, end, ENDPOINT_1, ENDPOINT_2, HEADERS_1, HEADERS_2, LOG_FILE, BLOCK_SIZE)
        total_attempts += attempts
        total_successful += successful
        total_mistypes += mistypes
        
        # Guardar los cambios en el Excel
        df.to_excel(FILE_NAME, index=False)

        # Si es el último bloque a analizar, tomar en cuenta para el total de los casos
        if end >= len(df):
            # This is wrong
            total_cases = len(df)
            break

        # De lo contrario, sumar el tamaño del bloque a la cantidad previa
        total_cases += BLOCK_SIZE
        
        # Permitir al usuario la opción de continuar con el bloque siguiente
        # Si la respuesta es "s", seguir, de lo contrario cerrar el ciclo
        print("Continuar procesando el siguiente bloque? (s/n): ", end="", flush=True)
        ready, _, _ = select.select([sys.stdin], [], [], 10)
        if ready:
            user_input = sys.stdin.readline().strip().lower()
            if user_input != "s":
                break
    
    # Escribir el mensaje de cierre, según si se procesó todo o quedó a medio camino
    final_message = "Todos los registros se procesaron" if end >= len(df) else "Proceso incompleto, interrumpido por usuario"
    
    # Escribir los datos al log, con append (sin sobreescribir)
    with open(LOG_FILE, "a") as f:
        f.write(f"{final_message}\nBloques totales: {total_blocks}, Total de filas analizadas: {total_cases}, Total de intentos de reparación: {total_attempts}, Total reparados: {total_successful}, Total mal clasificados: {total_mistypes}\n")
    
    print(final_message)
    print(f"Bloques totales: {total_blocks}, Total filas analizadas: {total_cases}, Total intentos de reparación: {total_attempts}, Total reparaciones exitosas: {total_successful}, Total de casos mal clasificados: {total_mistypes}")


def main_test():
    
    load_dotenv()
    
    rut = "21503162-4"

    endpoint = os.getenv("ENDPOINT_1")
    token = os.getenv("TOKEN_1")
    headers = {"Authorization": f"Bearer {token}"}

    # check_contract(rut, endpoint, headers)

    payload = '''
        {
        "codigoDocumento": "001",
        "nombreDocumento": "CONTRATOS",
        "nombreArchivo": "0021503162-4_001_202502_0001_0000000004.pdf",
        "path": "/home/dockers_app/mnt/RegistroAfiliacion",
        "estado": "APROBADO",
        "detail": "https://api.multicaja.cl/central/af/ayc/comercios/21503162/0021503162-4_001_202502_0001_0000000004.pdf",
        "adjuntadoPor": "AYC",
        "fechaAdjunto": "2025-02-28 06:25:55.876598",
        "file": null
    }
    '''

    endpoint = os.getenv("ENDPOINT_3")
    token = os.getenv("TOKEN_1")
    headers = {"Authorization": f"Bearer {token}"}

    # validate_contract_file(rut, endpoint=endpoint, headers=headers, payload=payload)
    ratio, validated = compare_contract_file_to_example(
        rut,
        endpoint=endpoint,
        headers=headers,
        payload=payload
    )

    if validated:
        print(f"Archivo validado, coeficiente de similitud: {ratio}")
    else:
        print("No fue posible validar archivo")





if __name__ == "__main__":
    main()
