import re
import time
import unicodedata
import requests
import dateparser
import mysql.connector
from mysql.connector import Error
from datetime import datetime
import pymysql
import spacy
from spacy.matcher import Matcher
import pandas as pd
from sqlalchemy import create_engine

nlp = spacy.load('es_core_news_lg')

def conectar_bd():
    """Conectamos a la base de datos MySQL."""
    conexion = pymysql.connect(
        host='localhost',
        user='root',
        password='Soccer.8a',
        database='noticias_prueba',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )
    return conexion

def normalizar_texto(texto):
    """Normalizamos el texto removiendo acentos y convirtiéndolo a minúsculas ASCII."""
    texto = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('ASCII')
    return texto.lower()

def limpiar_noticias():
    conexion = pymysql.connect(
        host='localhost',
        user='root',
        password='Soccer.8a',
        database='noticias_prueba'
    )

    try:
        with conexion.cursor() as cursor:
            # Verificamos si 'noticia_corregida' existe
            cursor.execute("SHOW COLUMNS FROM extracciones LIKE 'noticia_corregida';")
            resultado = cursor.fetchone()
            if not resultado:
                cursor.execute("ALTER TABLE extracciones ADD COLUMN noticia_corregida TEXT;")

            # Obtenemos noticias originales
            consulta_seleccion = "SELECT id, noticia FROM extracciones"
            cursor.execute(consulta_seleccion)
            resultados = cursor.fetchall()

            # Definimos expresiones regulares
            exreg_lee_tambien = re.compile(
                r'([Ll]ee también|[Ll]eer también|[Ll]ea también|[Ll]ee más|[Tt]ambién lee|[Tt]ambien lee).*?(\n|$)',
                re.IGNORECASE)
            exreg_foto = re.compile(r'Foto:.*?(\n|$)', re.IGNORECASE)
            exreg_dispositivo = re.compile(
                r',\s*desde tu dispositivo móvil entérate de las noticias más relevantes del día, artículos de opinión, entretenimiento, tendencias y más\..*?(\n|$)',
                re.IGNORECASE)
            exreg_ultima_parte = re.compile(
                r'(\*?\s*El Grupo de Diarios América \(GDA\), al cual pertenece EL UNIVERSAL.*|'
                r'Ahora puedes recibir notificaciones de BBC Mundo.*|'
                r'Recuerda que puedes recibir notificaciones de BBC Mundo.*|'
                r'Suscríbete aquí.*|'
                r'Recibe todos los viernes Hello Weekend.*|'
                r'Recuerda que puedes recibir notificaciones de BBC News Mundo.*|'
                r'Únete a nuestro canal.*|'
                r'Ahora puedes recibir notificaciones de BBC News Mundo.*|'
                r'¿Ya conoces nuestro canal de YouTube\? ¡Suscríbete!.*|'
                r'para recibir directo en tu correo nuestras newsletters sobre noticias del día, opinión, (planes para el fin de semana, )?Qatar 2022 y muchas opciones más\..*)',
                re.IGNORECASE | re.DOTALL)

            ids_modificados = []

            for fila in resultados:
                id_noticia = fila[0]
                texto_noticia = fila[1]

                if texto_noticia is not None:
                    texto_noticia_limpio = re.sub(exreg_lee_tambien, '', texto_noticia)
                    texto_noticia_limpio = re.sub(exreg_foto, '', texto_noticia_limpio)
                    texto_noticia_limpio = re.sub(exreg_dispositivo, '', texto_noticia_limpio)
                    texto_noticia_limpio = re.sub(exreg_ultima_parte, '', texto_noticia_limpio)

                    if texto_noticia != texto_noticia_limpio:
                        ids_modificados.append(id_noticia)

                    consulta_actualizacion = "UPDATE extracciones SET noticia_corregida = %s WHERE id = %s"
                    cursor.execute(consulta_actualizacion, (texto_noticia_limpio, id_noticia))

            conexion.commit()

    finally:
        conexion.close()


    print("Proceso de limpieza de noticias finalizado.")

def es_noticia_de_secuestro(texto_completo):
    doc = nlp(texto_completo)
    es_secuestro = False
    justificacion = ""

    for ent in doc.ents:
        contexto = ent.sent.text
        if any(term in contexto.lower() for term in ['simulacro', 'película', 'serie', 'ficticio', 'ficción']):
            es_secuestro = False
            justificacion = f"Contexto detectado relacionado con simulacros/ficción: '{contexto}'"
            break
        if ent.label_ in ['PER', 'ORG', 'MISC']:
            if any(verb in contexto for verb in ['retenido', 'privado', 'capturado', 'detenido', 'secuestrado']):
                es_secuestro = True
                justificacion = f"Contexto posible secuestro: '{contexto}'"
                break
        if "víctima" in ent.text.lower() and any(action in ent.sent.text.lower() for action in ['retenida', 'privada de libertad']):
            es_secuestro = True
            justificacion = f"Víctima privada de libertad: '{ent.sent.text}'"
            break

    return es_secuestro, justificacion

def procesar_noticias_relacion():
    conexion = conectar_bd()
    try:
        with conexion.cursor() as cursor:
            cursor.execute("SHOW COLUMNS FROM extracciones LIKE 'relacion_spacy4';")
            resultado = cursor.fetchone()
            if not resultado:
                cursor.execute("ALTER TABLE extracciones ADD COLUMN relacion_spacy4 VARCHAR(3);")

            sql = """
            SELECT id, noticia_corregida
            FROM extracciones
            WHERE (titulo NOT LIKE '%El Mayo Zambada%' 
            AND descripcion NOT LIKE '%El Mayo Zambada%' 
            AND titulo NOT LIKE '%El Mayo%' 
            AND descripcion NOT LIKE '%El Mayo%' 
            AND titulo NOT LIKE '%Israel%' 
            AND descripcion NOT LIKE '%Israel%' 
            AND titulo NOT LIKE '%Gaza%' 
            AND descripcion NOT LIKE '%Gaza%' 
            AND titulo NOT LIKE '%Hamas%' 
            AND descripcion NOT LIKE '%Hamas%' 
            AND titulo NOT LIKE '%Netanyahu%' 
            AND descripcion NOT LIKE '%Netanyahu%'
            AND titulo NOT LIKE '%Chapo Guzmán%' 
            AND descripcion NOT LIKE '%Chapo Guzmán%' 
            AND titulo NOT LIKE '%Ovidio Guzmán%' 
            AND descripcion NOT LIKE '%Ovidio Guzmán%');
            """
            cursor.execute(sql)
            resultados = cursor.fetchall()

            for noticia in resultados:
                id_noticia = noticia['id']
                texto_completo = noticia['noticia_corregida'] if noticia['noticia_corregida'] else ""
                relacionada_con_secuestro, justificacion = es_noticia_de_secuestro(texto_completo)

                if relacionada_con_secuestro:
                    cursor.execute("UPDATE extracciones SET relacion_spacy4 = 'sí' WHERE id = %s", (id_noticia,))
                else:
                    cursor.execute("UPDATE extracciones SET relacion_spacy4 = 'no' WHERE id = %s", (id_noticia,))

                conexion.commit()

    finally:
        conexion.close()


    print("Proceso de clasificación de relación finalizado.")

PALABRAS_IRRELEVANTES = [
    "El", "Los", "La", "Las", "Un", "Una", "De", "Del", "En", "Sin", "Con", "No", "Ven", "Al",
    "Centro", "Ciudad", "Norte", "Sur", "Este", "Oeste", "Ortega", "Felipe", "Cruz", "San", "General", "Todo",
    "Lo", "Nacional", "Por", "Durante", "Anaya", "Fuentes", "Instituto", "Han", "He", "Has", "Tu", "Progreso",
    "Internacional", "Fue", "Ocho", "Manuel", "Eduardo", "Como", "Gabriel", "Pero", "Para", "Rafael", "Juan", "Luis", "Tres",
    "Alto", "Uno", "Dos", "Tres", "Cuatro", "Cinco", "Carlos", "Gustavo", "Genaro", "Francisco", "Miguel", "Estado", "Jorge",
    "Nacional", "Casas", "Mata", "Santa", "China", "Agua", "Nuevo", "Valle", "Castillo", "Camargo", "Guadalupe", "Santiago", "Tierra",
    "Benito", "Nuevo", "Pedro", "Isidro", "José", "María", "Vicente", "Nicolas", "Emiliano", "Pueblo", "Casa", "Santa", "Padilla",
    "Marcos", "Soto", "Benito", "Ruiz", "Salvador", "Reforma", "Carrillo", "Martinez", "Gonzalez", "Reyes", "Solidaridad"
]
PALABRAS_IRRELEVANTES_LOWER = set(p.lower() for p in PALABRAS_IRRELEVANTES)

PALABRAS_CLAVE_BASE = [
    "secuestro", "hecho", "incidente", "caso", "ubicado", "encontrado", "rescatado"
]
VERBOS_CLAVE = [
    "ocurrir", "suceder", "realizar", "encontrar", "rescatar"
]

ABREVIATURAS_ESTADOS = {
    "Ags.": "Aguascalientes",
    "BC.": "Baja California",
    "BCS.": "Baja California Sur",
    "Camp.": "Campeche",
    "Chis.": "Chiapas",
    "Chih.": "Chihuahua",
    "CDMX.": "Ciudad de México",
    "Coah.": "Coahuila",
    "Col.": "Colima",
    "Dgo.": "Durango",
    "Edomex.": "Estado de México",
    "Gto.": "Guanajuato",
    "Gro.": "Guerrero",
    "Hgo.": "Hidalgo",
    "Jal.": "Jalisco",
    "Mich.": "Michoacán",
    "Mor.": "Morelos",
    "Nay.": "Nayarit",
    "NL.": "Nuevo León",
    "Oax.": "Oaxaca",
    "Pue.": "Puebla",
    "Qro.": "Querétaro",
    "QR.": "Quintana Roo",
    "SLP.": "San Luis Potosí",
    "Sin.": "Sinaloa",
    "Son.": "Sonora",
    "Tab.": "Tabasco",
    "Tamps.": "Tamaulipas",
    "Tlax.": "Tlaxcala",
    "Ver.": "Veracruz",
    "Yuc.": "Yucatán",
    "Zac.": "Zacatecas"
}

ALIAS_LUGARES = {
    "Tuxtla": "Tuxtla Gutiérrez",
    "Distrito Federal": "Ciudad de México",
    "Victoria": "Ciudad Victoria",
    "Izcalli": "Cuautitlán Izcalli"
}


def agregar_campos_lugares():
    """Agregamos los campos de lugar a la base de datos si no existen."""
    try:
        connection = conectar_bd()
        cursor = connection.cursor()
        cursor.execute("SHOW COLUMNS FROM extracciones LIKE 'pais'")
        resultado_pais = cursor.fetchone()
        cursor.execute("SHOW COLUMNS FROM extracciones LIKE 'estado'")
        resultado_estado = cursor.fetchone()
        cursor.execute("SHOW COLUMNS FROM extracciones LIKE 'municipio'")
        resultado_municipio = cursor.fetchone()
        cursor.execute("SHOW COLUMNS FROM extracciones LIKE 'ciudad'")
        resultado_ciudad = cursor.fetchone()

        if not resultado_pais:
            cursor.execute("ALTER TABLE extracciones ADD COLUMN pais VARCHAR(255)")
        if not resultado_estado:
            cursor.execute("ALTER TABLE extracciones ADD COLUMN estado VARCHAR(255)")
        if not resultado_municipio:
            cursor.execute("ALTER TABLE extracciones ADD COLUMN municipio VARCHAR(255)")
        if not resultado_ciudad:
            cursor.execute("ALTER TABLE extracciones ADD COLUMN ciudad VARCHAR(255)")

        connection.commit()
        connection.close()
    except Exception as e:

        pass

def generar_conjugaciones(verbos_clave):
    """Generamos conjugaciones de los verbos clave."""
    conjugaciones = set()
    for verbo in verbos_clave:
        doc_ = nlp(verbo)
        for token in doc_:
            if token.pos_ == "VERB":
                conjugaciones.add(token.lemma_)
                conjugaciones.add(token.text)
                conjugaciones.update([
                    f"{token.lemma_}á",
                    f"{token.lemma_}ía",
                    f"{token.lemma_}ó",
                    f"ha {token.lemma_}",
                    f"había {token.lemma_}",
                    f"habrá {token.lemma_}",
                    f"haya {token.lemma_}",
                    f"hubiera {token.lemma_}",
                ])
    return conjugaciones

def validar_relacion_hechos(texto, lugares):
    """Validamos la relación de hechos con los lugares mencionados."""
    doc_ = nlp(texto)
    relevancia = {}
    conjugaciones = generar_conjugaciones(VERBOS_CLAVE)
    palabras_clave = set(PALABRAS_CLAVE_BASE).union(conjugaciones)

    for lugar in lugares:
        relevancia[lugar] = 0
        for token in doc_:
            if lugar.lower() in token.text.lower():
                for palabra in palabras_clave:
                    if palabra in [w.text.lower() for w in token.head.subtree]:
                        relevancia[lugar] += 1
    lugar_mas_relevante = max(relevancia, key=relevancia.get) if relevancia else None
    return lugar_mas_relevante

def extraer_primer_lugar(texto):
    """Extraemos el primer lugar mencionado en el texto."""
    regex_inicio = r"^(?P<lugar>[A-ZÁÉÍÓÚÑa-záéíóúñ\s]+),?\s?(?P<estado_abrev>[A-Z][a-z]+\.)\s?[-—\.]"
    coincidencia = re.match(regex_inicio, texto)
    if coincidencia:
        lugar = coincidencia.group("lugar").strip()
        estado_abrev = coincidencia.group("estado_abrev").strip()
        estado_completo = ABREVIATURAS_ESTADOS.get(estado_abrev, estado_abrev)
        lugar_completo = f"{lugar}, {estado_completo}"
        return lugar_completo
    return None

def extraer_lugares_regex(texto):
    """Extraemos lugares usando expresiones regulares."""
    regex = r"(?:^|\.\s|\-\s|\b)([A-Z][a-z]+(?: [A-Z][a-z]+)*)[\.\-]?\b"
    lugares = re.findall(regex, texto)
    return [lugar for lugar in lugares]

def conectar_bd_local():
    """Conectamos a la base de datos local."""
    try:
        connection = pymysql.connect(
            host='localhost',
            user='root',
            password='Soccer.8a',
            db='noticias_prueba'
        )
        return connection
    except Exception as e:

        return None

def validar_lugar_bd_local(lugar):
    """Validamos el lugar en la base de datos local."""
    connection = conectar_bd_local()
    if not connection:
        return None, None, None  # No se pudo conectar a la base de datos

    try:
        cursor = connection.cursor()

        if lugar in ALIAS_LUGARES:
            lugar = ALIAS_LUGARES[lugar]

        sql_estado = "SELECT nombre, 'México' FROM estados WHERE nombre = %s"
        cursor.execute(sql_estado, (lugar,))
        resultado_estado = cursor.fetchone()
        if resultado_estado:
            estado, pais = resultado_estado
            return "México", estado, None

        sql_municipio = """
        SELECT municipios.nombre, estados.nombre, 'México'
        FROM municipios
        INNER JOIN estados ON municipios.estado = estados.id
        WHERE municipios.nombre = %s
        """
        cursor.execute(sql_municipio, (lugar,))
        resultado_municipio = cursor.fetchone()
        if resultado_municipio:
            municipio, estado, pais = resultado_municipio
            return "México", estado, municipio

        if " " not in lugar.strip():
            sql_municipio_parcial = """
            SELECT municipios.nombre, estados.nombre, 'México'
            FROM municipios
            INNER JOIN estados ON municipios.estado = estados.id
            WHERE municipios.nombre LIKE %s
            """
            cursor.execute(sql_municipio_parcial, (f"{lugar} %",))
            resultado_municipio_parcial = cursor.fetchone()
            if resultado_municipio_parcial:
                municipio, estado, pais = resultado_municipio_parcial
                return "México", estado, municipio

            sql_estado_parcial = "SELECT nombre, 'México' FROM estados WHERE nombre LIKE %s"
            cursor.execute(sql_estado_parcial, (f"{lugar} %",))
            resultado_estado_parcial = cursor.fetchone()
            if resultado_estado_parcial:
                estado, pais = resultado_estado_parcial
                return "México", estado, None

        return None, None, None
    except Exception as e:
        # No imprimimos errores según las indicaciones
        return None, None, None
    finally:
        connection.close()

def validar_lugar_via_geonames(lugar, usuario):
    """Validamos el lugar utilizando la API de GeoNames."""
    if lugar.lower() in PALABRAS_IRRELEVANTES_LOWER:
        return None, None, None
    if len(lugar) < 4:
        return None, None, None
    url = f"http://api.geonames.org/searchJSON?q={lugar}&maxRows=1&username={usuario}&countryBias=MX&continentCode=SA"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            if 'geonames' in data and len(data['geonames']) > 0:
                lugar_info = data['geonames'][0]
                pais = lugar_info.get('countryName', None)
                admin1 = lugar_info.get('adminName1', None)
                municipio = lugar_info.get('name', None)

                if pais and admin1:
                    if pais == "México":
                        return pais, admin1, municipio
                    else:
                        return pais, admin1, None
                if pais:
                    return pais, None, None
                return None, None, None
            else:
                return None, None, None
        else:
            return None, None, None
    except:
        return None, None, None

def extraer_lugares(texto):
    """Extraemos y validamos los lugares mencionados en el texto."""
    usuario_geonames = "bryanhernandez"

    lugar_inicio = extraer_primer_lugar(texto)
    if lugar_inicio:
        pais_local, estado_local, municipio_local = validar_lugar_bd_local(lugar_inicio)
        if pais_local and estado_local:
            return pais_local, estado_local, municipio_local, None, [f"Lugar inicial '{lugar_inicio}' validado"]

    lugares_regex = extraer_lugares_regex(texto)
    pais = None
    estado = None
    municipio = None
    justificacion = []
    lugares_validados = []

    for lugar in lugares_regex:
        if lugar.lower() in PALABRAS_IRRELEVANTES_LOWER or len(lugar) < 4:
            continue
        pais_local, estado_local, municipio_local = validar_lugar_bd_local(lugar)
        if pais_local and estado_local:
            pais = pais_local
            estado = estado_local
            municipio = municipio_local
            lugares_validados.append((pais, estado, municipio))
            justificacion.append(f"'{lugar}' validado en la base de datos local: {estado}, {pais}")
            return pais, estado, municipio, None, justificacion

    if len(lugares_validados) > 1:
        lugares_nombres = [f"{m or ''}, {e}" for _, e, m in lugares_validados]
        lugar_relevante = validar_relacion_hechos(texto, lugares_nombres)
        justificacion.append(f"Lugar más relevante: {lugar_relevante}")
        for pais_val, estado_val, municipio_val in lugares_validados:
            if lugar_relevante in f"{municipio_val or ''}, {estado_val}":
                pais, estado, municipio = pais_val, estado_val, municipio_val
                break

    if not pais or not estado:
        for lugar in lugares_regex:
            if lugar.lower() in PALABRAS_IRRELEVANTES_LOWER or len(lugar) < 4:
                continue
            pais_geo, estado_geo, municipio_geo = validar_lugar_via_geonames(lugar, usuario_geonames)
            if pais_geo and estado_geo:
                pais = pais_geo
                estado = estado_geo
                municipio = municipio_geo
                justificacion.append(f"'{lugar}' clasificado en GeoNames: {estado_geo}, {pais_geo}")
                break

    return pais, estado, municipio, None, justificacion

def actualizar_base_datos_lugares(pais, estado, municipio, ciudad, noticia_id):
    """Actualizamos la base de datos con la información de lugares."""
    try:
        connection = conectar_bd()
        cursor = connection.cursor()
        sql = "UPDATE extracciones SET pais=%s, estado=%s, municipio=%s, ciudad=%s WHERE id=%s"
        cursor.execute(sql, (pais, estado, municipio, ciudad, noticia_id))
        connection.commit()
        connection.close()
    except Exception as e:
        # No imprimimos errores según las indicaciones
        pass

def procesar_noticias_lugares():
    """Procesamos las noticias para extraer y actualizar la información de lugares."""
    try:
        connection = conectar_bd()
        cursor = connection.cursor()
        sql = "SELECT id, noticia_corregida, pais, estado, municipio, ciudad FROM extracciones WHERE relacion_spacy4='Sí'"
        cursor.execute(sql)
        noticias = cursor.fetchall()

        for noticia in noticias:
            noticia_id = noticia['id']
            texto_noticia = noticia['noticia_corregida'] if noticia['noticia_corregida'] else ""
            pais_actual = noticia['pais']
            estado_actual = noticia['estado']
            municipio_actual = noticia['municipio']
            ciudad_actual = noticia['ciudad']

            # Si ya tiene datos, saltamos
            if pais_actual or estado_actual or municipio_actual or ciudad_actual:
                continue

            pais, estado, municipio, ciudad, justificacion = extraer_lugares(texto_noticia)
            if pais or estado or municipio or ciudad:
                actualizar_base_datos_lugares(pais, estado, municipio, ciudad, noticia_id)

    except Exception as e:
        # No imprimimos errores según las indicaciones
        pass
    finally:
        connection.close()


    print("Proceso de extracción de lugares finalizado.")

def detectar_metodo_captura(texto):
    """Detectamos el método de captura utilizado en el secuestro."""
    doc_ = nlp(texto.lower())
    matcher = Matcher(nlp.vocab)

    patrones_metodo_captura = {
        "Captura_Fuerza": [
            [{"LEMMA": {"IN": ["golpear", "forzar", "someter", "empujar", "agarrar"]}}],
            [{"TEXT": {"REGEX": "(golpeado|forzado|sometido|empujado|agarrado)"}}],
            [{"TEXT": {"REGEX": "a punta de pistola|con violencia|bajo amenazas"}}],
        ],
        "Captura_Emboscada": [
            [{"LEMMA": {"IN": ["emboscar", "interceptar", "rodear", "bloquear"]}}],
            [{"TEXT": {"REGEX": "en una emboscada|interceptaron su vehículo"}}],
            [{"TEXT": {"REGEX": "(emboscado|interceptado) en"}}],
        ],
        "Captura_Intimidacion": [
            [{"LEMMA": {"IN": ["amenazar", "intimidar", "coaccionar", "chantajear"]}}],
            [{"TEXT": {"REGEX": "(amenazado|intimidado) con"}}],
            [{"TEXT": {"REGEX": "amenazas de muerte|amenazándolo con"}}],
        ],
        "Captura_Tecnologica": [
            [
                {"TEXT": {"REGEX": "contactó|contactaron|engañado|engañada|citó|citaron"}},
                {"OP": "*"},
                {"TEXT": {"REGEX": "facebook|twitter|redes sociales|internet|aplicación móvil|app de citas"}}
            ],
            [
                {"LEMMA": {"IN": ["conocer", "interactuar"]}},
                {"TEXT": {"REGEX": "en línea|por internet|por redes sociales"}}
            ],
        ],
        "Captura_Confianza": [
            [{"TEXT": {"REGEX": "amigo|amiga|familiar|conocido|cercano"}}],
            [{"TEXT": {"REGEX": "persona de confianza|relación cercana"}}],
        ],
        "Captura_Autoridad": [
            [
                {"LEMMA": {"IN": ["policía", "policías", "agente", "agentes", "militar", "militares", "ejército", "autoridad", "autoridades"]}},
                {"OP": "+"},
                {"LEMMA": {"IN": ["secuestrar", "privar", "detener"]}, "OP": "+"}
            ],
        ],
        "Captura_Transporte": [
            [{"TEXT": {"REGEX": "autobús|camioneta|vehículo interceptado|taxi|transporte público"}}],
            [{"TEXT": {"REGEX": "camino a su destino|en tránsito|en ruta"}}],
        ],
        "Captura_Complicidad": [
            [{"TEXT": {"REGEX": "empleado|empleada|colaborador|compañero"}}],
            [{"TEXT": {"REGEX": "complicidad|alguien del entorno laboral"}}],
        ],
        "Captura_Cartel": [
            [{"TEXT": {"REGEX": "cártel|grupo criminal|La Familia|Los Zetas"}}],
            [{"TEXT": {"REGEX": "vinculado a cartel|como represalia"}}],
        ],
        "Suplantacion_Identidad": [
            [{"LEMMA": {"IN": ["hacerse", "suplantar", "pretender", "imitar", "aparentar", "fingir", "simular"]}},
             {"OP": "*"},
             {"LEMMA": {"IN": ["policía", "agente", "militar", "autoridad", "funcionario"]}}]
        ],
        "Captura_Casa": [
            [{"TEXT": {"REGEX": "en su (propia )?casa|en su (propio )?domicilio|cerca de su hogar|afuera de su casa|entrando a su casa|saliendo de su casa"}}],
            [{"LEMMA": {"IN": ["hogar", "casa", "domicilio"]}}],
            [{"TEXT": {"REGEX": "propiedad"}}],
        ],
    }

    for method_name, patterns in patrones_metodo_captura.items():
        matcher.add(method_name, patterns)

    captor_methods = []
    lugar_methods = []
    captura_methods = []
    explicacion = {}

    oraciones_ignoradas = set()
    palabras_clave_reporte = ["reportó", "denunció", "informó a la policía", "declaró a las autoridades"]
    palabras_clave_victima = ["policía fue secuestrado", "agente fue secuestrado", "militar fue secuestrado"]

    matches = matcher(doc_)

    for match_id, start, end in matches:
        span = doc_[start:end]
        oracion_completa = span.sent.text
        metodo = nlp.vocab.strings[match_id]

        if metodo == "Captura_Autoridad":
            if any(palabra in oracion_completa for palabra in palabras_clave_reporte):
                if oracion_completa not in oraciones_ignoradas:
                    oraciones_ignoradas.add(oracion_completa)
                continue
            if any(palabra in oracion_completa for palabra in palabras_clave_victima):
                continue
            palabras_clave_acciones = ["secuestrar", "secuestro", "privación de libertad", "privar de libertad", "raptar"]
            if not any(palabra in oracion_completa for palabra in palabras_clave_acciones):
                continue
            if not captor_methods:
                captor_methods.append("autoridad")
                explicacion[metodo] = f"Método de captor: 'autoridad'. Contexto: '{oracion_completa}'"
            continue

        if metodo == "Suplantacion_Identidad":
            if not captor_methods:
                captor_methods.append("suplantación de identidad")
                explicacion["Suplantacion_Identidad"] = f"Suplantación de identidad: '{oracion_completa}'"
            continue

        if metodo in ["Captura_Confianza", "Captura_Cartel", "Captura_Complicidad"]:
            captor_name = metodo.split('_')[1].lower()
            if not captor_methods:
                captor_methods.append(captor_name)
                explicacion[metodo] = f"Captor detectado: '{captor_name}'. Contexto: '{oracion_completa}'"

        if metodo in ["Captura_Transporte", "Captura_Casa"]:
            lugar_name = metodo.split('_')[1].lower()
            if not lugar_methods:
                lugar_methods.append(lugar_name)
                explicacion[metodo] = f"Lugar detectado: '{lugar_name}'. Contexto: '{oracion_completa}'"

        if metodo in ["Captura_Fuerza", "Captura_Emboscada", "Captura_Intimidacion", "Captura_Tecnologica"]:
            captura_name = metodo.split('_')[1].lower()
            if not captura_methods:
                captura_methods.append(captura_name)
                explicacion[metodo] = f"Método de captura: '{captura_name}'. Contexto: '{oracion_completa}'"

    if not captor_methods:
        captor_methods.append("persona común")
    if not lugar_methods:
        lugar_methods.append("vía pública")
    if not captura_methods:
        captura = "no especifico"
    else:
        captura = captura_methods[0]

    return captor_methods[0], lugar_methods[0], captura, list(explicacion.values())

def verificar_y_crear_campos_metodo_captura():
    """Verificamos y creamos los campos necesarios para el método de captura."""
    conexion = conectar_bd()
    try:
        with conexion.cursor() as cursor:
            cursor.execute("SHOW COLUMNS FROM extracciones")
            columnas = [row['Field'] for row in cursor.fetchall()]

            nuevos_campos = {
                'captor': "VARCHAR(255) DEFAULT NULL",
                'lugar': "VARCHAR(255) DEFAULT NULL",
                'captura': "VARCHAR(255) DEFAULT NULL"
            }

            for campo, definicion in nuevos_campos.items():
                if campo not in columnas:
                    sql_alter = f"ALTER TABLE extracciones ADD COLUMN {campo} {definicion};"
                    cursor.execute(sql_alter)
        conexion.commit()
    except pymysql.MySQLError as e:
        # No imprimimos errores según las indicaciones
        conexion.rollback()
    finally:
        conexion.close()

def obtener_noticias_relacionadas():
    """Obtenemos las noticias relacionadas con secuestros."""
    conexion = conectar_bd()
    try:
        with conexion.cursor() as cursor:
            sql = "SELECT id, noticia_corregida FROM extracciones WHERE relacion_spacy4 = 'Sí'"
            cursor.execute(sql)
            resultados = cursor.fetchall()
            return resultados
    finally:
        conexion.close()

def guardar_resultados_captura(noticia_id, captor, lugar, captura):
    """Guardamos los resultados del método de captura en la base de datos."""
    conexion = conectar_bd()
    try:
        with conexion.cursor() as cursor:
            sql = """
            UPDATE extracciones
            SET captor = %s, lugar = %s, captura = %s
            WHERE id = %s
            """
            cursor.execute(sql, (captor, lugar, captura, noticia_id))
        conexion.commit()
    except pymysql.MySQLError as e:
        # No imprimimos errores según las indicaciones
        conexion.rollback()
    finally:
        conexion.close()

def procesar_noticias_metodo_captura():
    """Procesamos las noticias para detectar el método de captura."""
    verificar_y_crear_campos_metodo_captura()
    noticias = obtener_noticias_relacionadas()
    for noticia in noticias:
        id_noticia = noticia['id']
        texto_noticia = noticia['noticia_corregida'] if noticia['noticia_corregida'] else ""
        captor, lugar, captura, _ = detectar_metodo_captura(texto_noticia)
        guardar_resultados_captura(id_noticia, captor, lugar, captura)

    # Notificamos la finalización del proceso de detección de método de captura
    print("Proceso de detección de método de captura finalizado.")

def clasificar_liberacion(texto):
    """Clasificamos la liberación en diferentes categorías."""
    doc = nlp(texto.lower())
    matcher = Matcher(nlp.vocab)

    patrones_liberacion_general = [
        [{"LEMMA": {"IN": ["liberar", "rescatar"]}}, {"OP": "+"}],
        [{"TEXT": {"REGEX": "liberado|liberaron|rescatado|rescatados|apareció sano y salvo|retornó a su hogar"}}]
    ]

    patrones_operativo = [
        [{"LEMMA": {"IN": ["operativo", "rescatar", "encontrar"]}}, {"LOWER": "policiaco", "OP": "?"}],
        [{"TEXT": {"REGEX": "fueron rescatados|fueron liberados"}}]
    ]

    patrones_autoridad = [
        [{"LEMMA": {"IN": ["elemento", "ejército", "autoridad"]}}, {"LOWER": "mexicano", "OP": "?"},
         {"LOWER": "liberar", "OP": "+"}]
    ]

    patrones_retorno = [
        [{"LEMMA": {"IN": ["retornar", "regresar", "volver"]}}, {"TEXT": {"REGEX": "a su hogar|sano y salvo"}}]
    ]

    patrones_negociacion = [
        [{"LEMMA": {"IN": ["negociar", "acordar"]}},
         {"LOWER": {"IN": ["liberación", "rescate", "retorno"]}, "OP": "?"}],
        [{"TEXT": {"REGEX": "negociación para la liberación|acuerdo de liberación|liberación por acuerdo"}}]
    ]

    matcher.add("LiberacionGeneral", patrones_liberacion_general)
    matcher.add("Operativo", patrones_operativo)
    matcher.add("Autoridad", patrones_autoridad)
    matcher.add("Retorno", patrones_retorno)
    matcher.add("Negociacion", patrones_negociacion)

    tipo_liberacion = "No clasificado"
    hubo_liberacion = False

    matches = matcher(doc)
    for match_id, start, end in matches:
        tipo = nlp.vocab.strings[match_id]
        if tipo == "LiberacionGeneral":
            tipo_liberacion = "Liberación general"
            hubo_liberacion = True
            break
        elif tipo == "Operativo":
            tipo_liberacion = "Liberación en operativo"
            hubo_liberacion = True
        elif tipo == "Autoridad":
            tipo_liberacion = "Liberación por autoridad"
            hubo_liberacion = True
        elif tipo == "Retorno":
            tipo_liberacion = "Retorno sin detalles"
            hubo_liberacion = True
        elif tipo == "Negociacion":
            tipo_liberacion = "Liberación por negociación"
            hubo_liberacion = True

    return hubo_liberacion, tipo_liberacion

def verificar_y_agregar_campos_liberacion():
    """Verificamos y agregamos los campos necesarios para la liberación."""
    conexion = conectar_bd()
    try:
        with conexion.cursor() as cursor:
            cursor.execute("SHOW COLUMNS FROM extracciones LIKE 'liberacion'")
            existe_liberacion = cursor.fetchone()

            cursor.execute("SHOW COLUMNS FROM extracciones LIKE 'tipo_liberacion'")
            existe_tipo_liberacion = cursor.fetchone()

            if not existe_liberacion:
                cursor.execute("ALTER TABLE extracciones ADD COLUMN liberacion VARCHAR(3)")
            if not existe_tipo_liberacion:
                cursor.execute("ALTER TABLE extracciones ADD COLUMN tipo_liberacion VARCHAR(50)")

            conexion.commit()
    finally:
        conexion.close()

def actualizar_noticia_liberacion(id_noticia, liberacion, tipo_liberacion):
    """Actualizamos la información de liberación en la base de datos."""
    conexion = conectar_bd()
    try:
        with conexion.cursor() as cursor:
            sql = """
            UPDATE extracciones 
            SET liberacion = %s, tipo_liberacion = %s 
            WHERE id = %s
            """
            cursor.execute(sql, (liberacion, tipo_liberacion, id_noticia))
            conexion.commit()
    finally:
        conexion.close()

def procesar_noticias_liberacion():
    """Procesamos las noticias para clasificar la liberación."""
    verificar_y_agregar_campos_liberacion()
    noticias = obtener_noticias_relacionadas()
    for noticia in noticias:
        id_noticia = noticia['id']
        texto_noticia = noticia['noticia_corregida']
        hubo_liberacion, tipo_liberacion = clasificar_liberacion(texto_noticia)
        actualizar_noticia_liberacion(id_noticia, 'Sí' if hubo_liberacion else 'No', tipo_liberacion)

    # Notificamos la finalización del proceso de clasificación de liberación
    print("Proceso de clasificación de liberación finalizado.")

def extraer_fecha_publicacion(fecha_publicacion_texto):
    """Extraemos la fecha de publicación del texto."""
    match = re.search(r'\|\s*(\d{1,2}/\d{1,2}/\d{4})\s*\|', fecha_publicacion_texto)
    if match:
        fecha_str = match.group(1)
        fecha_pub = datetime.strptime(fecha_str, '%d/%m/%Y')
        return str(fecha_pub.day), str(fecha_pub.month), str(fecha_pub.year)
    else:
        return '', '', ''

def extraer_fechas_en_texto(texto_):
    """Extraemos todas las fechas encontradas en el texto."""
    patrones_fecha = [
        r"\b(desde el \d{1,2} de (enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)( del? año \d{4}| de \d{4})?)\b",
        r"\b(\d{1,2} de (enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)( del? año \d{4}| de \d{4})?)\b",
        r"\b((desde el )?(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)( del? año \d{4}| de \d{4}))\b",
        r"\b(\d{1,2}/\d{1,2}/\d{2,4})\b",
        r"\b(este año|el año pasado|este mes|el mes pasado)\b",
    ]
    fechas_encontradas_ = []
    for patron_ in patrones_fecha:
        coincidencias_ = re.findall(patron_, texto_, re.IGNORECASE)
        for c in coincidencias_:
            if isinstance(c, tuple):
                fechas_encontradas_.append(c[0])
            else:
                fechas_encontradas_.append(c)
    return fechas_encontradas_

def obtener_contexto_ampliado(sentencia, doc):
    """Obtenemos el contexto ampliado de una sentencia."""
    sentences_ = list(doc.sents)
    sentence_index = sentences_.index(sentencia)
    contexto_ = sentencia.text
    if sentence_index > 0:
        contexto_ = sentences_[sentence_index - 1].text + ' ' + contexto_
    if sentence_index < len(sentences_) - 1:
        contexto_ += ' ' + sentences_[sentence_index + 1].text
    return contexto_

def obtener_componentes_fecha(fecha_texto, dia_pub, mes_pub, año_pub):
    """Obtenemos los componentes de la fecha a partir del texto."""
    try:
        fecha_base = datetime(int(año_pub), int(mes_pub), int(dia_pub))
    except ValueError:
        fecha_base = datetime.now()

    if 'el año pasado' in fecha_texto.lower():
        fecha_base = fecha_base.replace(year=fecha_base.year - 1)
    elif 'este año' in fecha_texto.lower():
        pass
    elif 'este mes' in fecha_texto.lower():
        pass
    elif 'el mes pasado' in fecha_texto.lower():
        mes_anterior = fecha_base.month - 1 if fecha_base.month > 1 else 12
        año_ajustado = fecha_base.year if fecha_base.month > 1 else fecha_base.year - 1
        fecha_base = fecha_base.replace(month=mes_anterior, year=año_ajustado)

    fecha_texto_limpio = re.sub(r'\bdesde el\b', '', fecha_texto, flags=re.IGNORECASE).strip()

    fecha_parseada = dateparser.parse(
        fecha_texto_limpio,
        languages=['es'],
        settings={'RELATIVE_BASE': fecha_base, 'PREFER_DATES_FROM': 'past'}
    )

    dia = ''
    mes = ''
    año = ''

    if fecha_parseada:
        dia = str(fecha_parseada.day)
        mes = str(fecha_parseada.month)
        año = str(fecha_parseada.year)

    if not año:
        año = año_pub
    if not mes:
        mes = mes_pub

    if mes and año:
        if mes_pub:
            if int(mes) > int(mes_pub):
                año = str(int(año) - 1)

    return dia, mes, año

def extraer_fecha_secuestro(texto, fecha_publicacion):
    """Extraemos la fecha del secuestro a partir del texto y la fecha de publicación."""
    doc = nlp(texto.lower())
    matcher = Matcher(nlp.vocab)

    patrones_secuestro = [
        [{"LEMMA": {"IN": ["secuestro", "privar", "raptar", "levantar"]}}],
        [{"LOWER": {"IN": ["privado", "privada"]}}, {"LOWER": "de"}, {"LOWER": "su"}, {"LOWER": "libertad"}],
        [{"LEMMA": {"IN": ["ocurrir", "suceder", "registrar"]}},
         {"POS": "ADP", "OP": "?"}, {"LOWER": "el", "OP": "?"}, {"LOWER": "secuestro"}],
    ]
    matcher.add("Secuestro", patrones_secuestro)

    sentences = list(doc.sents)
    matches = matcher(doc)
    fechas_detectadas = []

    dia_pub, mes_pub, año_pub = extraer_fecha_publicacion(fecha_publicacion)

    for match_id, start, end in matches:
        span = doc[start:end]
        sent = span.sent
        contexto_ampliado = obtener_contexto_ampliado(sent, doc)
        fechas_en_contexto = extraer_fechas_en_texto(contexto_ampliado)
        if fechas_en_contexto:
            for fecha_texto in fechas_en_contexto:
                fechas_detectadas.append((fecha_texto, contexto_ampliado))
        else:
            sentence_index = sentences.index(sent)
            if sentence_index > 0:
                oracion_anterior = sentences[sentence_index - 1]
                contexto_ampliado = obtener_contexto_ampliado(oracion_anterior, doc)
                fechas_en_contexto_anterior = extraer_fechas_en_texto(contexto_ampliado)
                if fechas_en_contexto_anterior:
                    for fecha_texto in fechas_en_contexto_anterior:
                        fechas_detectadas.append((fecha_texto, contexto_ampliado))

    if not fechas_detectadas:
        return "No se encontró fecha en el texto; se utiliza la fecha de publicación.", dia_pub, mes_pub, año_pub
    else:
        fecha_texto, contexto = fechas_detectadas[0]
        dia, mes, año = obtener_componentes_fecha(fecha_texto, dia_pub, mes_pub, año_pub)
        resultado = f"Fecha del secuestro: {fecha_texto}\nContexto: '{contexto.strip()}'"
        return resultado, dia, mes, año

def verificar_y_agregar_campos_fecha():
    """Verificamos y agregamos los campos necesarios para la fecha del secuestro."""
    conexion = conectar_bd()
    try:
        with conexion.cursor() as cursor:
            campos = ['dia_secuestro', 'mes_secuestro', 'año_secuestro']
            for campo in campos:
                cursor.execute(f"SHOW COLUMNS FROM extracciones LIKE '{campo}'")
                existe_campo = cursor.fetchone()
                if not existe_campo:
                    cursor.execute(f"ALTER TABLE extracciones ADD COLUMN {campo} VARCHAR(10)")
            conexion.commit()
    finally:
        conexion.close()

def actualizar_fecha_noticia(id_noticia, dia, mes, año):
    """Actualizamos la fecha del secuestro en la base de datos."""
    conexion = conectar_bd()
    try:
        with conexion.cursor() as cursor:
            sql = """
            UPDATE extracciones SET dia_secuestro = %s, mes_secuestro = %s, año_secuestro = %s WHERE id = %s
            """
            cursor.execute(sql, (dia or '', mes or '', año or '', id_noticia))
            conexion.commit()
    finally:
        conexion.close()

def obtener_noticias_fecha():
    """Obtenemos las noticias relacionadas para extraer la fecha del secuestro."""
    conexion = conectar_bd()
    try:
        with conexion.cursor() as cursor:
            sql = "SELECT id, noticia_corregida, fecha FROM extracciones WHERE relacion_spacy4 = 'Sí'"
            cursor.execute(sql)
            resultados = cursor.fetchall()
            return resultados
    finally:
        conexion.close()

def procesar_noticias_fecha_secuestro():
    """Procesamos las noticias para extraer la fecha del secuestro."""
    verificar_y_agregar_campos_fecha()
    noticias = obtener_noticias_fecha()
    for noticia in noticias:
        id_noticia = noticia['id']
        texto_noticia = noticia['noticia_corregida']
        fecha_publicacion = noticia['fecha']
        resultado_fecha, dia, mes, año = extraer_fecha_secuestro(texto_noticia, fecha_publicacion)
        actualizar_fecha_noticia(id_noticia, dia, mes, año)

    # Notificamos la finalización del proceso de extracción de fechas
    print("Proceso de extracción de fechas de secuestro finalizado.")

def extraer_perfil_victima(texto):
    """Extraemos el perfil de la víctima a partir del texto."""
    doc = nlp(texto)
    perfiles_detectados = []
    victimas_unicas = set()

    verbos_secuestro = ["secuestro", "secuestrar", "raptar", "privar", "plagiar", "desaparecer", "sustraer"]

    def obtener_victimas_desde_token(token):
        victimas = []
        if token.pos_ in ("NOUN", "PROPN"):
            victimas.append(token)
        for child in token.children:
            victimas.extend(obtener_victimas_desde_token(child))
        return victimas

    def determinar_menor_de_edad(token_victima, sent):
        palabras_menor = ['niño', 'niña', 'menor', 'adolescente', 'infante', 'bebé', 'chico', 'chica', 'nieto', 'hijo', 'hija', 'menores']
        texto_ = sent.text.lower()
        for palabra in palabras_menor:
            if palabra in texto_:
                return True, ""
        return None, None

    def extraer_edad(token_victima, sent):
        texto_ = sent.text
        patrones_edad = [
            rf"{re.escape(token_victima.text)} de (\d{{1,3}}) años\b",
            rf"{re.escape(token_victima.text)} de (\d{{1,3}}) años de edad\b",
            r"\b(\d{1,2}) años\b",
            r"(\d{1,3}) años de edad"
        ]
        for patron in patrones_edad:
            coincidencias_ = re.findall(patron, texto_, re.IGNORECASE)
            if coincidencias_:
                return coincidencias_[0], ""
        return None, None

    def determinar_genero(token_victima, sent):
        palabras_masculinas = ['hombre', 'varón', 'niño', 'adolescente', 'joven', 'profesor', 'doctor', 'ingeniero', 'activista', 'alcalde', 'maestro']
        palabras_femeninas = ['mujer', 'fémina', 'niña', 'adolescente', 'joven', 'profesora', 'doctora', 'ingeniera', 'activista', 'alcaldesa', 'maestra']
        texto_ = sent.text.lower()

        for palabra in palabras_masculinas:
            if palabra in texto_:
                return 'Masculino', ""
        for palabra in palabras_femeninas:
            if palabra in texto_:
                return 'Femenino', ""
        return None, None

    def extraer_ocupacion(token_victima, sent):
        ocupaciones = [
            'alcalde', 'diputado', 'senador', 'gobernador', 'presidente', 'médico', 'doctor', 'enfermero', 'abogado', 'ingeniero',
            'estudiante', 'empresario', 'comerciante', 'profesor', 'periodista', 'policía', 'militar', 'taxista', 'chofer', 'trabajador', 'activista'
        ]
        texto_ = sent.text.lower()
        for ocupacion in ocupaciones:
            if re.search(rf"\b{ocupacion}\b", texto_):
                return ocupacion.capitalize(), ""
        return None, None

    def extraer_nacionalidad(token_victima, sent):
        nacionalidades = ['mexicano', 'mexicana', 'estadounidense', 'canadiense', 'español', 'colombiano', 'argentino', 'venezolano', 'peruano', 'chileno']
        texto_ = sent.text.lower()
        for nacionalidad in nacionalidades:
            if re.search(rf"\b{nacionalidad}\b", texto_):
                return nacionalidad.capitalize(), ""
        return None, None

    def analizar_victima(victima_token, sent):
        perfil = {}

        es_menor, _ = determinar_menor_de_edad(victima_token, sent)
        if es_menor is not None:
            perfil['menor_de_edad'] = 'Sí' if es_menor else 'No'

        edad, _ = extraer_edad(victima_token, sent)
        if edad:
            perfil['edad'] = edad
            if int(edad) < 18:
                perfil['menor_de_edad'] = 'Sí'

        genero, _ = determinar_genero(victima_token, sent)
        if genero:
            perfil['genero_victima'] = genero

        ocupacion, _ = extraer_ocupacion(victima_token, sent)
        if ocupacion:
            perfil['ocupacion_victima'] = ocupacion

        nacionalidad, _ = extraer_nacionalidad(victima_token, sent)
        if nacionalidad:
            perfil['nacionalidad_victima'] = nacionalidad

        return perfil if perfil else None

    def consolidar_perfiles(perfiles):
        perfil_final = {}
        for perfil in perfiles:
            for clave, valor in perfil.items():
                if clave not in perfil_final or not perfil_final[clave]:
                    perfil_final[clave] = valor
        return perfil_final

    for sent in doc.sents:
        for token in sent:
            if normalizar_texto(token.lemma_) in verbos_secuestro and token.pos_ == "VERB":
                victimas = []
                if token.dep_ in ("ROOT", "conj"):
                    for child in token.children:
                        if child.dep_ in ("obj", "dobj", "obl", "nsubj:pass", "iobj", "nsubj_pass", "nsubjpass"):
                            victimas.extend(obtener_victimas_desde_token(child))
                for victima in victimas:
                    identidad_victima = f"{victima.text}_{victima.i}"
                    if identidad_victima not in victimas_unicas:
                        victimas_unicas.add(identidad_victima)
                        perfil = analizar_victima(victima, sent)
                        if perfil:
                            perfiles_detectados.append(perfil)

    multiples_victimas = 'Sí' if len(perfiles_detectados) > 1 else 'No'
    perfil_consolidado = consolidar_perfiles(perfiles_detectados)
    perfil_consolidado['multiples_victimas'] = multiples_victimas

    return perfil_consolidado

def verificar_y_agregar_campos_perfil():
    """Verificamos y agregamos los campos necesarios para el perfil de la víctima."""
    conexion = conectar_bd()
    try:
        with conexion.cursor() as cursor:
            campos = ['edad_victima', 'menor_de_edad', 'genero_victima', 'ocupacion_victima', 'nacionalidad_victima', 'multiples_victimas']
            for campo in campos:
                cursor.execute(f"SHOW COLUMNS FROM extracciones LIKE '{campo}'")
                if not cursor.fetchone():
                    cursor.execute(f"ALTER TABLE extracciones ADD COLUMN {campo} VARCHAR(255)")
            conexion.commit()
    finally:
        conexion.close()

def actualizar_perfil_noticia(id_noticia, perfil):
    """Actualizamos el perfil de la víctima en la base de datos."""
    conexion = conectar_bd()
    try:
        with conexion.cursor() as cursor:
            sql = """
            UPDATE extracciones SET edad_victima = %s, menor_de_edad = %s, genero_victima = %s, 
            ocupacion_victima = %s, nacionalidad_victima = %s, multiples_victimas = %s WHERE id = %s
            """
            cursor.execute(sql, (
                perfil.get('edad', ''),
                perfil.get('menor_de_edad', ''),
                perfil.get('genero_victima', ''),
                perfil.get('ocupacion_victima', ''),
                perfil.get('nacionalidad_victima', ''),
                perfil.get('multiples_victimas', 'No'),
                id_noticia
            ))
            conexion.commit()
    finally:
        conexion.close()

def procesar_noticias_perfil_victima():
    """Procesamos las noticias para extraer el perfil de la víctima."""
    verificar_y_agregar_campos_perfil()
    noticias = obtener_noticias_relacionadas()
    for noticia in noticias:
        id_noticia = noticia['id']
        texto_noticia = noticia['noticia_corregida']
        perfil_victima = extraer_perfil_victima(texto_noticia)
        actualizar_perfil_noticia(id_noticia, perfil_victima)

    # Notificamos la finalización del proceso de extracción del perfil de la víctima
    print("Proceso de extracción del perfil de la víctima finalizado.")

def verificar_y_agregar_campo_tipo_secuestro():
    """Verificamos y agregamos el campo necesario para el tipo de secuestro."""
    conexion = conectar_bd()
    try:
        with conexion.cursor() as cursor:
            cursor.execute("SHOW COLUMNS FROM extracciones LIKE 'tipo_secuestro'")
            if not cursor.fetchone():
                cursor.execute("ALTER TABLE extracciones ADD COLUMN tipo_secuestro VARCHAR(255)")
                # No imprimimos mensajes innecesarios
            conexion.commit()
    finally:
        conexion.close()

def extraer_tipo_secuestro(texto):

    doc = nlp(texto)
    categorias_identificadas = set()
    justificaciones = []

    verbos_secuestro = {'secuestro', 'secuestrar', 'privar', 'plagiar', 'raptar', 'plagio', 'rapto', 'privado', 'privada'}
    lemmas_en_texto = [token.lemma_.lower() for token in doc]
    if any(lemma in verbos_secuestro for lemma in lemmas_en_texto):
        if len(categorias_identificadas) == 0:
            categorias_identificadas.add('Secuestro general')
            justificaciones.append("No se detectó un tipo específico, pero se mencionó 'secuestro' o similar.")
    else:
        justificaciones.append("No se detectó 'secuestro' o 'privar' en el texto.")

    tipo_secuestro = next(iter(categorias_identificadas)) if categorias_identificadas else ''
    return tipo_secuestro, '; '.join(justificaciones)


def actualizar_tipo_secuestro(id_noticia, tipo_secuestro):
    """Actualizamos el tipo de secuestro en la base de datos."""
    conexion = conectar_bd()
    try:
        with conexion.cursor() as cursor:
            sql = "UPDATE extracciones SET tipo_secuestro = %s WHERE id = %s"
            cursor.execute(sql, (tipo_secuestro, id_noticia))
            conexion.commit()
    finally:
        conexion.close()

def procesar_noticias_tipo_secuestro():
    """Procesamos las noticias para extraer el tipo de secuestro."""
    verificar_y_agregar_campo_tipo_secuestro()
    noticias = obtener_noticias_relacionadas()
    for noticia in noticias:
        id_noticia = noticia['id']
        texto_noticia = noticia['noticia_corregida']
        tipo_secuestro_, justificacion = extraer_tipo_secuestro(texto_noticia)
        actualizar_tipo_secuestro(id_noticia, tipo_secuestro_)

    # Notificamos la finalización del proceso de extracción del tipo de secuestro
    print("Proceso de extracción del tipo de secuestro finalizado.")

def marcar_noticias_repetidas():
    """Marcamos las noticias que se encuentran repetidas en la base de datos."""
    try:
        connection = mysql.connector.connect(
            host='localhost',
            database='noticias_prueba',
            user='root',
            password='Soccer.8a'
        )

        if connection.is_connected():
            cursor = connection.cursor()

            cursor.execute("SHOW COLUMNS FROM extracciones LIKE 'noticias_repetidas';")
            result = cursor.fetchone()
            if not result:
                cursor.execute("""
                    ALTER TABLE extracciones
                    ADD COLUMN noticias_repetidas TINYINT(1) DEFAULT 0;
                """)

            cursor.execute("""
                SELECT 
                    id,
                    municipio,
                    estado,
                    pais,
                    mes_secuestro,
                    año_secuestro,
                    tipo_secuestro,
                    captor,
                    lugar,
                    captura
                FROM extracciones WHERE relacion_spacy4='Sí';
            """)
            records = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            data = [dict(zip(columns, row)) for row in records]

            potential_duplicates = {}
            for row in data:
                key = (
                    row['municipio'],
                    row['estado'],
                    row['pais'],
                    row['mes_secuestro'],
                    row['año_secuestro']
                )
                if key in potential_duplicates:
                    potential_duplicates[key].append(row)
                else:
                    potential_duplicates[key] = [row]

            duplicates_to_mark = []
            for group in potential_duplicates.values():
                if len(group) > 1:
                    seen = {}
                    for entry in group:
                        sub_key = (
                            entry['tipo_secuestro'],
                            entry['captor'],
                            entry['lugar'],
                            entry['captura']
                        )
                        if sub_key in seen:
                            duplicates_to_mark.append(entry['id'])
                        else:
                            seen[sub_key] = entry['id']
            if duplicates_to_mark:
                format_strings = ','.join(['%s'] * len(duplicates_to_mark))
                cursor.execute(f"""
                    UPDATE extracciones
                    SET noticias_repetidas = 1
                    WHERE id IN ({format_strings});
                """, duplicates_to_mark)
                connection.commit()
    except Error as e:
        # No imprimimos errores según las indicaciones
        pass
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    # Notificamos la finalización del proceso de marcar noticias repetidas
    print("Proceso de marcar noticias repetidas finalizado.")

def crear_tabla_filtrada():
    """Creamos una tabla filtrada con las noticias procesadas."""
    engine = create_engine('mysql+pymysql://root:Soccer.8a@localhost:3306/noticias_prueba')

    query = """
    SELECT
        id,
        pais,
        estado,
        municipio,
        liberacion,
        tipo_liberacion,
        mes_secuestro,
        año_secuestro,
        captor,
        lugar,
        captura,
        tipo_secuestro
    FROM extracciones
    WHERE relacion_spacy4 = 'Sí'
      AND (noticias_repetidas IS NULL OR noticias_repetidas <> 1)
      AND año_secuestro > '2015'
      AND pais = 'México'
    """

    df = pd.read_sql(query, engine)
    campos_requeridos = [
        'pais', 'estado', 'municipio', 'liberacion', 'tipo_liberacion',
        'mes_secuestro', 'año_secuestro', 'captor', 'lugar', 'captura', 'tipo_secuestro'
    ]

    df_filtered = df.dropna(subset=campos_requeridos)
    df_filtered = df_filtered[(df_filtered[campos_requeridos] != '').all(axis=1)]
    df_filtered.reset_index(drop=True, inplace=True)

    df_filtered.to_sql('extracciones_filtradas', con=engine, if_exists='replace', index=False)

    engine.dispose()

if __name__ == "__main__":

    limpiar_noticias()                      # 1. Limpia y crea noticia_corregida
    procesar_noticias_relacion()            # 2. Clasifica si relacionadas con secuestro
    agregar_campos_lugares()                # 3. Agrega campos pais, estado, municipio, ciudad
    procesar_noticias_lugares()             # 3.1 Extrae lugares
    procesar_noticias_metodo_captura()      # 4. Detecta método de captura
    procesar_noticias_liberacion()          # 5. Clasifica la liberación
    procesar_noticias_fecha_secuestro()     # 6. Extrae fecha del secuestro
    procesar_noticias_perfil_victima()      # 7. Extrae perfil de la víctima
    procesar_noticias_tipo_secuestro()      # 8. Extrae tipo de secuestro
    marcar_noticias_repetidas()             # 9. Marca noticias repetidas
 #   crear_tabla_filtrada()                  # 10. Crea tabla filtrada final


    print("Todos los procesos han finalizado correctamente.")
