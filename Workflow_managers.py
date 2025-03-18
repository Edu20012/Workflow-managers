from prefect import flow, task
import requests
import json

@task(retries=3, retry_delay_seconds=10)
def obtener_datos(url: str = "https://jsonplaceholder.cypress.io/posts"):
    """Obtiene datos de la API JSONPlaceholder"""
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        raise Exception(f"Error al obtener datos: {str(e)}")

@task
def procesar_datos(posts: list):
    """Procesa los datos para contar posts por usuario"""
    if not posts:
        raise ValueError("Lista de posts vacía")
    
    conteo_usuarios = {}
    for post in posts:
        user_id = post['userId']
        conteo_usuarios[user_id] = conteo_usuarios.get(user_id, 0) + 1
    return conteo_usuarios

@task
def guardar_resultados(conteo: dict, filename: str = "resultados.json"):
    """Guarda los resultados en un archivo JSON"""
    try:
        with open(filename, 'w') as f:
            json.dump(conteo, f, indent=2)
        return f"Resultados guardados en {filename}"
    except Exception as e:
        raise Exception(f"Error al guardar archivo: {str(e)}")

@flow(name="Análisis de Posts")
def flujo_principal(url: str = "https://jsonplaceholder.cypress.io/posts"):
    # Obtener datos
    datos = obtener_datos(url)
    
    # Procesar datos
    procesado = procesar_datos(datos)
    
    # Guardar resultados
    resultado = guardar_resultados(procesado)
    
    print("✅ Flujo completado exitosamente")
    print(f"📊 Total de usuarios: {len(procesado)}")
    print(f"📝 Total de posts: {sum(procesado.values())}")

if __name__ == "__main__":
    flujo_principal()