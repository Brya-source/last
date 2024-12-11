

# Cargar librerías
library(DBI)
library(RMySQL)
library(dplyr)
library(ggplot2)
library(tidyr)

# Establecer conexión a la base de datos
con <- dbConnect(RMySQL::MySQL(),
                 dbname = "noticias",
                 host = "localhost",
                 port = 3306,
                 user = "root",
                 password = "Soccer.8a")

# Cargar datos de la tabla 'extracciones_filtradas'
data <- dbGetQuery(con, "SELECT * FROM extracciones_filtradas")

library(dplyr)

# Asegurarse de que los campos 'mes_secuestro' y 'año_secuestro' sean numéricos
data <- data %>%
  mutate(
    mes_secuestro = as.numeric(mes_secuestro),
    año_secuestro = as.numeric(año_secuestro)
  )

# Filtrar datos válidos (meses entre 1 y 12)
data <- data %>%
  filter(!is.na(mes_secuestro), !is.na(año_secuestro), mes_secuestro >= 1 & mes_secuestro <= 12)

# Crear una columna de fecha (usando el primer día del mes)
data <- data %>%
  mutate(fecha_secuestro = as.Date(paste(año_secuestro, mes_secuestro, "01", sep = "-")))

# Conteo de secuestros por mes
secuestros_tiempo <- data %>%
  group_by(fecha_secuestro) %>%
  summarise(total_secuestros = n())

# Visualización
library(ggplot2)
ggplot(secuestros_tiempo, aes(x = fecha_secuestro, y = total_secuestros)) +
  geom_line(color = "blue") +
  geom_point(color = "red") +
  labs(title = "Tendencia de Secuestros 2016 - nov 2024",
       x = "Fecha",
       y = "Cantidad de Secuestros") +
  theme_minimal()
#-----------------------------------------------------------------------------
# Conteo de secuestros por estado
secuestros_estado <- data %>%
  group_by(estado) %>%
  summarise(total_secuestros = n()) %>%
  arrange(desc(total_secuestros))

# Visualización por estado
ggplot(secuestros_estado, aes(x = reorder(estado, -total_secuestros), y = total_secuestros)) +
  geom_bar(stat = "identity", fill = "steelblue") +
  coord_flip() +
  labs(title = "Distribución de Secuestros por Estado",
       x = "Estado",
       y = "Cantidad de Secuestros") +
  theme_minimal()
#------------------------------------------------------------------------------

# Conteo por tipo de liberación
tipo_liberacion_data <- data %>%
  group_by(tipo_liberacion) %>%
  summarise(total_secuestros = n())

# Visualización
ggplot(tipo_liberacion_data, aes(x = reorder(tipo_liberacion, -total_secuestros), y = total_secuestros, fill = tipo_liberacion)) +
  geom_bar(stat = "identity") +
  coord_flip() +
  labs(title = "Distribución por Tipo de Liberación",
       x = "Tipo de Liberación",
       y = "Cantidad de Secuestros") +
  theme_minimal()
#-------------------------------
# Conteo por tipo de captor
captor_data <- data %>%
  group_by(captor) %>%
  summarise(total_secuestros = n()) %>%
  arrange(desc(total_secuestros))

# Visualización
ggplot(captor_data, aes(x = reorder(captor, -total_secuestros), y = total_secuestros, fill = captor)) +
  geom_bar(stat = "identity") +
  coord_flip() +
  labs(title = "Distribución de Características del Captor",
       x = "Captor",
       y = "Cantidad de Secuestros") +
  theme_minimal()
#-----------------------------------------------
# Conteo por lugar
lugar_data <- data %>%
  group_by(lugar) %>%
  summarise(total_secuestros = n())

# Visualización
ggplot(lugar_data, aes(x = reorder(lugar, -total_secuestros), y = total_secuestros, fill = lugar)) +
  geom_bar(stat = "identity") +
  coord_flip() +
  labs(title = "Distribución por Lugar de Secuestro",
       x = "Lugar",
       y = "Cantidad de Secuestros") +
  theme_minimal()
#-----------------------------------------------------------------------
# Conteo por tipo de captura
captura_data <- data %>%
  group_by(captura) %>%
  summarise(total_secuestros = n()) %>%
  arrange(desc(total_secuestros))

# Visualización
ggplot(captura_data, aes(x = reorder(captura, -total_secuestros), y = total_secuestros, fill = captura)) +
  geom_bar(stat = "identity") +
  coord_flip() +
  labs(title = "Distribución por Tipo de Captura",
       x = "Tipo de Captura",
       y = "Cantidad de Secuestros") +
  theme_minimal()
#----------------------------------------------------------
# Conteo por tipo de secuestro
tipo_secuestro_data <- data %>%
  group_by(tipo_secuestro) %>%
  summarise(total_secuestros = n()) %>%
  arrange(desc(total_secuestros))

# Visualización
ggplot(tipo_secuestro_data, aes(x = reorder(tipo_secuestro, -total_secuestros), y = total_secuestros, fill = tipo_secuestro)) +
  geom_bar(stat = "identity") +
  coord_flip() +
  labs(title = "Distribución por Tipo de Secuestro",
       x = "Tipo de Secuestro",
       y = "Cantidad de Secuestros") +
  theme_minimal()
#---------------------------------------------------------
# Conteo de secuestros por liberación (Sí o No)
liberacion_data <- data %>%
  group_by(liberacion) %>%
  summarise(total_secuestros = n())

# Visualización
ggplot(liberacion_data, aes(x = liberacion, y = total_secuestros, fill = liberacion)) +
  geom_bar(stat = "identity") +
  labs(title = "Distribución de Secuestros por Liberación",
       x = "¿Hubo Liberación?",
       y = "Cantidad de Secuestros") +
  theme_minimal()
#----------------------------------------------

#---------------------------------------------------------------
# Visualización #No entiendo la gráfica
ggplot(data, aes(x = captor, fill = captura)) +
  geom_bar(position = "fill") +
  facet_wrap(~lugar) +
  labs(
    title = "Relación entre Lugar, Captor y Tipo de Captura",
    x = "Captor",
    y = "Proporción de Secuestros",
    fill = "Tipo de Captura"
  ) +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

#--------------------------------------------------------------

#------------------------------------------------------------------------
# Visualización (No entiendo este tipo de grafica) #falta entender o descartar
ggplot(data, aes(x = año_secuestro, fill = captura)) +
  geom_bar(position = "fill") +
  facet_wrap(~tipo_liberacion) +
  labs(
    title = "Relación entre Tipo de Liberación, Captura y Año",
    x = "Año",
    y = "Proporción de Secuestros",
    fill = "Tipo de Captura"
  ) +
  theme_minimal()

#-----------------------------------------------------------
# Visualización (muchos municipio mejor por estados) cambiar a estados con más concurrencia
#No es muy util la forma presentada
ggplot(data, aes(x = estado, fill = tipo_secuestro)) +
  geom_bar(position = "dodge") +
  facet_wrap(~lugar) +
  labs(
    title = "Relación entre estado, Lugar y Tipo de Secuestro",
    x = "estado",
    y = "Cantidad de Secuestros",
    fill = "Tipo de Secuestro"
  ) +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))
#---------------------------------------------------------------------------

#-----------------------------------------------------------------
# Conteo de secuestros por lugar y año
trend_data <- data %>%
  group_by(lugar, año_secuestro) %>%
  summarise(total_secuestros = n())

# Visualización
ggplot(trend_data, aes(x = año_secuestro, y = total_secuestros, color = lugar, group = lugar)) +
  geom_line(linewidth = 1) +
  labs(
    title = "Tendencia de Secuestros por Lugar y Año",
    x = "Año",
    y = "Cantidad de Secuestros",
    color = "Lugar"
  ) +
  theme_minimal()
#----------------------------------------------------------------------

#--------------------------------------------------------------------------
#funcional y podríamos hacer por cada estado
# Lista de estados con mayor incidencia
estados_seleccionados <- c("Ciudad e México", "Chiapas", "Estado de México", "Jalisco", "Oaxaca")

# Filtrar datos para los estados seleccionados
data_filtrada <- data %>%
  filter(estado %in% estados_seleccionados)

# Configuración de gráficos individuales para cada estado
library(ggplot2)

# Crear gráficos individuales
for (estado in estados_seleccionados) {
  # Filtrar datos para el estado actual
  data_estado <- data_filtrada %>%
    filter(estado == !!estado)
  
  # Crear el gráfico
  p <- ggplot(data_estado, aes(x = año_secuestro, fill = tipo_secuestro)) +
    geom_bar(position = "dodge") +
    labs(
      title = paste("Tipos de Secuestro en", estado, "por Año"),
      x = "Año",
      y = "Cantidad de Secuestros",
      fill = "Tipo de Secuestro"
    ) +
    theme_minimal() +
    theme(axis.text.x = element_text(angle = 45, hjust = 1))
  
  # Mostrar el gráfico
  print(p)
}

#------------------------------------------------------------------
#FALTA DESCARGAR DE AQUI EN ADELANTE
# Lista de lugares principales #bien varios graficos #Funcional, varios graficos
lugares <- c("casa", "transporte", "vía pública")

# Filtrar datos por los lugares seleccionados
data_filtrada_lugares <- data %>%
  filter(lugar %in% lugares)

# Crear gráficos individuales por lugar
library(ggplot2)

for (lugar_actual in lugares) {
  # Filtrar datos para el lugar actual
  data_lugar <- data_filtrada_lugares %>%
    filter(lugar == lugar_actual)
  
  # Verificar si hay datos para el lugar actual
  if (nrow(data_lugar) > 0) {
    # Crear el gráfico
    p <- ggplot(data_lugar, aes(x = estado, fill = tipo_secuestro)) +
      geom_bar(position = "dodge") +
      labs(
        title = paste("Relación entre Estado y Tipo de Secuestro en", lugar_actual),
        x = "Estado",
        y = "Cantidad de Secuestros",
        fill = "Tipo de Secuestro"
      ) +
      theme_minimal() +
      theme(
        axis.text.x = element_text(angle = 45, hjust = 1),
        plot.title = element_text(size = 16, face = "bold")
      )
    
    # Mostrar el gráfico
    print(p)
  } else {
    message(paste("No hay datos para el lugar:", lugar_actual))
  }
}

#-------------------------------------------------------
#funcional
# Agregar conteo por estado y año (sin separar por tipo de secuestro)
heatmap_data <- data %>%
  group_by(estado, año_secuestro) %>%
  summarise(total_secuestros = n())

# Visualización
library(ggplot2)
ggplot(heatmap_data, aes(x = año_secuestro, y = estado, fill = total_secuestros)) +
  geom_tile(color = "white") +  # Bordes blancos para mejor visibilidad
  scale_fill_gradient(low = "white", high = "red") +
  labs(
    title = "Total de Secuestros por Estado y Año",
    x = "Año",
    y = "Estado",
    fill = "Total de Secuestros"
  ) +
  theme_minimal() +
  theme(
    axis.text.x = element_text(angle = 45, hjust = 1),
    plot.title = element_text(size = 16, face = "bold"),
    axis.title.x = element_text(size = 12),
    axis.title.y = element_text(size = 12),
    legend.title = element_text(size = 10)
  )
#---------------------------------------------------------
#Cambiar por cada estado# ya tenemos este analisis pero solo con 5 estados
# Filtrar datos relevantes
data_filtrada <- data %>%
  group_by(estado, año_secuestro, tipo_secuestro) %>%
  summarise(total_secuestros = n(), .groups = "drop")

# Crear el gráfico
library(ggplot2)
ggplot(data_filtrada, aes(x = año_secuestro, y = total_secuestros, fill = tipo_secuestro)) +
  geom_bar(stat = "identity", position = "dodge") +
  facet_wrap(~estado) +
  labs(
    title = "Tipos de Secuestro por Estado y Año",
    x = "Año",
    y = "Cantidad de Secuestros",
    fill = "Tipo de Secuestro"
  ) +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

#----------------------------------------------------------
# Agrupar datos por estado, año y liberación #Cambiar tipo de gráfica o hacerlo entre cada estado 
data_liberacion <- data %>%
  group_by(estado, año_secuestro, liberacion) %>%
  summarise(total_secuestros = n(), .groups = "drop")

# Crear el gráfico
ggplot(data_liberacion, aes(x = año_secuestro, y = total_secuestros, fill = liberacion)) +
  geom_bar(stat = "identity", position = "fill") +
  facet_wrap(~estado) +
  labs(
    title = "Relación entre Número de Secuestros y Liberación por Estado",
    x = "Año",
    y = "Proporción de Secuestros",
    fill = "Liberación"
  ) +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))
#-------------------------------------------------------------
#no me sirve, era en relacion a la locacion, no al lugar
# Filtrar datos con liberación por operativo
data_operativo <- data %>%
  filter(tipo_liberacion == "Liberación en operativo") %>%
  group_by(lugar) %>%
  summarise(total_operativos = n(), .groups = "drop")

# Crear el gráfico
ggplot(data_operativo, aes(x = reorder(lugar, -total_operativos), y = total_operativos, fill = lugar)) +
  geom_bar(stat = "identity") +
  labs(
    title = "Liberaciones por Operativo según Lugar",
    x = "Lugar",
    y = "Cantidad de Liberaciones",
    fill = "Lugar"
  ) +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))
#------------------------------------------------

#-----------------------------------------------------------------------
# Agrupar datos por lugar y tipo de secuestro
data_lugar_secuestro <- data %>%
  group_by(lugar, tipo_secuestro) %>%
  summarise(total_secuestros = n(), .groups = "drop")

# Crear el gráfico
ggplot(data_lugar_secuestro, aes(x = reorder(lugar, -total_secuestros), y = total_secuestros, fill = tipo_secuestro)) +
  geom_bar(stat = "identity", position = "fill") +
  labs(
    title = "Proporción de Tipos de Secuestro según Lugar",
    x = "Lugar",
    y = "Proporción de Secuestros",
    fill = "Tipo de Secuestro"
  ) +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))
#-------------------------------------------------------------------
# Agrupar datos por lugar y tipo de secuestro
data_lugar_tipo <- data %>%
  group_by(lugar, tipo_secuestro) %>%
  summarise(total = n(), .groups = "drop")

# Visualización
ggplot(data_lugar_tipo, aes(x = lugar, y = total, fill = tipo_secuestro)) +
  geom_bar(stat = "identity", position = "dodge") +
  labs(
    title = "Distribución de Tipos de Secuestro según el Lugar",
    x = "Lugar del Secuestro",
    y = "Cantidad de Secuestros",
    fill = "Tipo de Secuestro"
  ) +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))
#-------------------------------------------------------------------
# Agrupar datos por fecha y tipo de secuestro
data_tiempo_tipo <- data %>%
  group_by(fecha_secuestro, tipo_secuestro) %>%
  summarise(total = n(), .groups = "drop")

# Visualización
ggplot(data_tiempo_tipo, aes(x = fecha_secuestro, y = total, color = tipo_secuestro)) +
  geom_line(size = 1) +
  labs(
    title = "Tendencia de Secuestros por Tipo a lo Largo del Tiempo",
    x = "Fecha",
    y = "Cantidad de Secuestros",
    color = "Tipo de Secuestro"
  ) +
  theme_minimal()
#-------------------------------------------------------------------
# Agrupar datos por estado y captor
data_estado_captor <- data %>%
  group_by(estado, captor) %>%
  summarise(total = n(), .groups = "drop")

# Visualización
ggplot(data_estado_captor, aes(x = captor, y = estado, fill = total)) +
  geom_tile(color = "white") +
  scale_fill_gradient(low = "white", high = "blue") +
  labs(
    title = "Mapa de Calor de Secuestros por Estado y Tipo de Captor",
    x = "Tipo de Captor",
    y = "Estado",
    fill = "Cantidad de Secuestros"
  ) +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))
#-------------------------------------------------------------------
# Agrupar datos por tipo de liberación y tipo de secuestro #Explicar esat gráfica
data_liberacion_secuestro <- data %>%
  group_by(tipo_liberacion, tipo_secuestro) %>%
  summarise(total = n(), .groups = "drop")

# Visualización
ggplot(data_liberacion_secuestro, aes(x = tipo_liberacion, y = total, fill = tipo_secuestro)) +
  geom_bar(stat = "identity", position = "fill") +
  labs(
    title = "Relación entre Tipo de Liberación y Tipo de Secuestro",
    x = "Tipo de Liberación",
    y = "Proporción de Secuestros",
    fill = "Tipo de Secuestro"
  ) +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))
#----------------------------------------------------------------
# Agrupar datos por estado y lugar #Explicar esta gráfica
data_estado_lugar <- data %>%
  group_by(estado, lugar) %>%
  summarise(total = n(), .groups = "drop")

# Visualización
ggplot(data_estado_lugar, aes(x = reorder(estado, -total), y = total, fill = lugar)) +
  geom_bar(stat = "identity") +
  coord_flip() +
  labs(
    title = "Secuestros por Estado y Lugar del Secuestro",
    x = "Estado",
    y = "Cantidad de Secuestros",
    fill = "Lugar del Secuestro"
  ) +
  theme_minimal()
#-----------------------------------------------------
#Esta tiene que estar relacionada por el año no solo por mes, hacer ese cambio
# Agrupar datos por mes y captor
data_mes_captor <- data %>%
  group_by(mes_secuestro, captor) %>%
  summarise(total = n(), .groups = "drop")

# Visualización
ggplot(data_mes_captor, aes(x = mes_secuestro, y = total, color = captor)) +
  geom_point(size = 3) +
  geom_line(aes(group = captor), size = 1) +
  labs(
    title = "Secuestros por Mes y Tipo de Captor",
    x = "Mes",
    y = "Cantidad de Secuestros",
    color = "Tipo de Captor"
  ) +
  theme_minimal()
#--------------------------------------
#no me gustó y no le entiendo
# Agrupar datos por año, lugar y liberación
data_ano_lugar_liberacion <- data %>%
  group_by(año_secuestro, lugar, liberacion) %>%
  summarise(total = n(), .groups = "drop")

# Visualización
ggplot(data_ano_lugar_liberacion, aes(x = año_secuestro, y = total, fill = liberacion)) +
  geom_bar(stat = "identity", position = "fill") +
  facet_wrap(~ lugar) +
  labs(
    title = "Proporción de Liberaciones por Año y Lugar del Secuestro",
    x = "Año",
    y = "Proporción de Secuestros",
    fill = "Liberación"
  ) +
  theme_minimal()

#-----------------------------------------------
# Agrupar datos por captor y captura
data_captor_captura <- data %>%
  group_by(captor, captura) %>%
  summarise(total = n(), .groups = "drop")

# Visualización
ggplot(data_captor_captura, aes(x = captor, y = total, fill = captura)) +
  geom_bar(stat = "identity", position = "dodge") +
  labs(
    title = "Relación entre Tipo de Captor y Captura",
    x = "Tipo de Captor",
    y = "Cantidad de Secuestros",
    fill = "Captura"
  ) +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))
#--------------------------------------------------

# Instalar paquetes si aún no lo has hecho
install.packages("geodata")
install.packages("sf")
install.packages("ggplot2")
install.packages("dplyr")

# Cargar las librerías
library(geodata)
library(sf)
library(ggplot2)
library(dplyr)

# Descargar y cargar el mapa de México a nivel de estados
mexico_map <- geodata::gadm(country = "MEX", level = 1, path = tempdir())

# Convertir a objeto sf si no lo es
mexico_map_sf <- st_as_sf(mexico_map)

# Nombres de los estados en el mapa
unique_states_map <- unique(mexico_map_sf$NAME_1)
print("Estados en el mapa:")
print(unique_states_map)

# Estandarizar nombres en tus datos
data_estado_total$estado <- tolower(iconv(data_estado_total$estado, to = "ASCII//TRANSLIT"))

# Estandarizar nombres en el mapa
mexico_map_sf$NAME_1 <- tolower(iconv(mexico_map_sf$NAME_1, to = "ASCII//TRANSLIT"))

# Nombres únicos en tus datos
unique_states_data <- unique(data_estado_total$estado)
print("Estados en tus datos (después de estandarizar):")
print(unique_states_data)

# Nombres únicos en el mapa
unique_states_map <- unique(mexico_map_sf$NAME_1)
print("Estados en el mapa (después de estandarizar):")
print(unique_states_map)

# Identificar estados no coincidentes
estados_no_coincidentes <- setdiff(unique_states_data, unique_states_map)
print("Estados no coincidentes después de estandarizar:")
print(estados_no_coincidentes)

# Crear tabla de equivalencias
equivalencias <- data.frame(
  estado_datos = c(
    "ciudad de mexico",
    "estado de mexico"
  ),
  estado_mapa = c(
    "distrito federal",
    "mexico"
  )
)

library(dplyr)

# Unir tus datos con la tabla de equivalencias
data_estado_total <- data_estado_total %>%
  left_join(equivalencias, by = c("estado" = "estado_datos")) %>%
  mutate(
    estado = ifelse(is.na(estado_mapa), estado, estado_mapa)
  ) %>%
  select(-estado_mapa)

# Verificar estados no coincidentes después de aplicar equivalencias
estados_no_coincidentes <- setdiff(unique(data_estado_total$estado), unique(mexico_map_sf$NAME_1))
print("Estados no coincidentes después de aplicar equivalencias:")
print(estados_no_coincidentes)

# Unir tus datos con el mapa
map_data <- mexico_map_sf %>%
  left_join(data_estado_total, by = c("NAME_1" = "estado"))

# Visualizar el mapa
ggplot(map_data) +
  geom_sf(aes(fill = total_secuestros)) +
  scale_fill_gradient(low = "white", high = "red", na.value = "grey80") +
  labs(
    title = "Distribución Geográfica de Secuestros en México",
    fill = "Cantidad de Secuestros"
  ) +
  theme_minimal()
#--------------------------------------------------------------------

# Agrupar datos por lugar y tipo de liberación
data_lugar_liberacion <- data %>%
  group_by(lugar, tipo_liberacion) %>%
  summarise(total = n(), .groups = "drop")

# Visualización
ggplot(data_lugar_liberacion, aes(x = lugar, y = total, fill = tipo_liberacion)) +
  geom_bar(stat = "identity", position = "fill") +
  labs(
    title = "Relación entre Lugar del Secuestro y Tipo de Liberación",
    x = "Lugar del Secuestro",
    y = "Proporción de Secuestros",
    fill = "Tipo de Liberación"
  ) +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

#---------------------------------------------------------------
#Mostrar mejor cada 5 municipios- Corregir
# Asegurarse de que los campos necesarios están presentes y correctos
library(dplyr)

# Convertir 'año_secuestro' a numérico si no lo está
data <- data %>%
  mutate(año_secuestro = as.numeric(año_secuestro))

# Filtrar datos válidos
data <- data %>%
  filter(!is.na(municipio), !is.na(año_secuestro), !is.na(tipo_secuestro))

# Conteo de secuestros por municipio
municipios_secuestros <- data %>%
  group_by(municipio) %>%
  summarise(total_secuestros = n()) %>%
  arrange(desc(total_secuestros)) %>%
  slice(1:15)

# Ver los 15 municipios seleccionados
print(municipios_secuestros)

# Obtener la lista de los 15 municipios
top_municipios <- municipios_secuestros$municipio

# Filtrar los datos para esos municipios
data_top_municipios <- data %>%
  filter(municipio %in% top_municipios)

library(ggplot2)

# Agrupar datos por municipio y año
data_municipio_año <- data_top_municipios %>%
  group_by(municipio, año_secuestro) %>%
  summarise(total_secuestros = n()) %>%
  ungroup()

# Visualización
ggplot(data_municipio_año, aes(x = año_secuestro, y = total_secuestros, fill = as.factor(año_secuestro))) +
  geom_bar(stat = "identity") +
  facet_wrap(~ municipio, scales = "free_y") +
  labs(
    title = "Secuestros por Año en los 15 Municipios con Mayor Incidencia",
    x = "Año",
    y = "Cantidad de Secuestros",
    fill = "Año"
  ) +
  theme_minimal() +
  theme(
    legend.position = "bottom",
    axis.text.x = element_text(angle = 45, hjust = 1)
  )
#-------------------------------------------------------------
# Agrupar datos por municipio y tipo de secuestro #Funcional, solo enrender la gráfica
data_municipio_tipo <- data_top_municipios %>%
  group_by(municipio, tipo_secuestro) %>%
  summarise(total_secuestros = n()) %>%
  ungroup()

# Visualización
ggplot(data_municipio_tipo, aes(x = reorder(municipio, total_secuestros), y = total_secuestros, fill = tipo_secuestro)) +
  geom_bar(stat = "identity") +
  coord_flip() +
  labs(
    title = "Distribución de Tipos de Secuestro en los 15 Municipios con Mayor Incidencia",
    x = "Municipio",
    y = "Cantidad de Secuestros",
    fill = "Tipo de Secuestro"
  ) +
  theme_minimal()
#-------------------------------------------------------------------

# Cargar las librerías
library(geodata)
library(sf)
library(ggplot2)
library(dplyr)

# Filtrar los datos para el año 2022
data_2022 <- data %>%
  filter(año_secuestro == 2022)

# Agrupar los datos por estado y contar los secuestros
data_estado_total <- data_2022 %>%
  group_by(estado) %>%
  summarise(total_secuestros = n()) %>%
  ungroup()

# Descargar y cargar el mapa de México a nivel de estados
mexico_map <- geodata::gadm(country = "MEX", level = 1, path = tempdir())

# Convertir a objeto sf si no lo es
mexico_map_sf <- st_as_sf(mexico_map)

# Estandarizar nombres en tus datos
data_estado_total$estado <- tolower(iconv(data_estado_total$estado, to = "ASCII//TRANSLIT"))

# Estandarizar nombres en el mapa
mexico_map_sf$NAME_1 <- tolower(iconv(mexico_map_sf$NAME_1, to = "ASCII//TRANSLIT"))

# Nombres únicos en tus datos
unique_states_data <- unique(data_estado_total$estado)
print("Estados en tus datos (después de estandarizar):")
print(unique_states_data)

# Nombres únicos en el mapa
unique_states_map <- unique(mexico_map_sf$NAME_1)
print("Estados en el mapa (después de estandarizar):")
print(unique_states_map)

# Identificar estados no coincidentes
estados_no_coincidentes <- setdiff(unique_states_data, unique_states_map)
print("Estados no coincidentes antes de aplicar equivalencias:")
print(estados_no_coincidentes)


# Unir tus datos con la tabla de equivalencias
data_estado_total <- data_estado_total %>%
  left_join(equivalencias, by = c("estado" = "estado_datos")) %>%
  mutate(
    estado = ifelse(is.na(estado_mapa), estado, estado_mapa)
  ) %>%
  select(-estado_mapa)

# Verificar estados no coincidentes después de aplicar equivalencias
estados_no_coincidentes <- setdiff(unique(data_estado_total$estado), unique_states_map)
print("Estados no coincidentes después de aplicar equivalencias:")
print(estados_no_coincidentes)

# Unir tus datos con el mapa
map_data <- mexico_map_sf %>%
  left_join(data_estado_total, by = c("NAME_1" = "estado"))

# Visualizar el mapa
ggplot(map_data) +
  geom_sf(aes(fill = total_secuestros)) +
  scale_fill_gradient(low = "cyan", high = "blue", na.value = "white") +
  labs(
    title = "Distribución Geográfica de Secuestros en México en 2022",
    fill = "Cantidad de Secuestros"
  ) +
  theme_minimal()
#-------------------------------------------------------------------

# Cargar las librerías
library(geodata)
library(sf)
library(ggplot2)
library(dplyr)

# Filtrar los datos para el año 2023
data_2023 <- data %>%
  filter(año_secuestro == 2023)

# Agrupar los datos por estado y contar los secuestros
data_estado_total <- data_2023 %>%
  group_by(estado) %>%
  summarise(total_secuestros = n()) %>%
  ungroup()

# Descargar y cargar el mapa de México a nivel de estados
mexico_map <- geodata::gadm(country = "MEX", level = 1, path = tempdir())

# Convertir a objeto sf si no lo es
mexico_map_sf <- st_as_sf(mexico_map)

# Estandarizar nombres en tus datos
data_estado_total$estado <- tolower(iconv(data_estado_total$estado, to = "ASCII//TRANSLIT"))

# Estandarizar nombres en el mapa
mexico_map_sf$NAME_1 <- tolower(iconv(mexico_map_sf$NAME_1, to = "ASCII//TRANSLIT"))

# Nombres únicos en tus datos
unique_states_data <- unique(data_estado_total$estado)
print("Estados en tus datos (después de estandarizar):")
print(unique_states_data)

# Nombres únicos en el mapa
unique_states_map <- unique(mexico_map_sf$NAME_1)
print("Estados en el mapa (después de estandarizar):")
print(unique_states_map)

# Identificar estados no coincidentes
estados_no_coincidentes <- setdiff(unique_states_data, unique_states_map)
print("Estados no coincidentes antes de aplicar equivalencias:")
print(estados_no_coincidentes)


# Unir tus datos con la tabla de equivalencias
data_estado_total <- data_estado_total %>%
  left_join(equivalencias, by = c("estado" = "estado_datos")) %>%
  mutate(
    estado = ifelse(is.na(estado_mapa), estado, estado_mapa)
  ) %>%
  select(-estado_mapa)

# Verificar estados no coincidentes después de aplicar equivalencias
estados_no_coincidentes <- setdiff(unique(data_estado_total$estado), unique_states_map)
print("Estados no coincidentes después de aplicar equivalencias:")
print(estados_no_coincidentes)

# Unir tus datos con el mapa
map_data <- mexico_map_sf %>%
  left_join(data_estado_total, by = c("NAME_1" = "estado"))

# Visualizar el mapa
ggplot(map_data) +
  geom_sf(aes(fill = total_secuestros)) +
  scale_fill_gradient(low = "white", high = "purple", na.value = "grey80") +
  labs(
    title = "Distribución Geográfica de Secuestros en México en 2023",
    fill = "Cantidad de Secuestros"
  ) +
  theme_minimal()
#-----------------------------------------------------------------
# Cargar las librerías
library(geodata)
library(sf)
library(ggplot2)
library(dplyr)

# Filtrar los datos para el año 2024
data_2024 <- data %>%
  filter(año_secuestro == 2024)

# Agrupar los datos por estado y contar los secuestros
data_estado_total <- data_2024 %>%
  group_by(estado) %>%
  summarise(total_secuestros = n()) %>%
  ungroup()

# Descargar y cargar el mapa de México a nivel de estados
mexico_map <- geodata::gadm(country = "MEX", level = 1, path = tempdir())

# Convertir a objeto sf si no lo es
mexico_map_sf <- st_as_sf(mexico_map)

# Estandarizar nombres en tus datos
data_estado_total$estado <- tolower(iconv(data_estado_total$estado, to = "ASCII//TRANSLIT"))

# Estandarizar nombres en el mapa
mexico_map_sf$NAME_1 <- tolower(iconv(mexico_map_sf$NAME_1, to = "ASCII//TRANSLIT"))

# Nombres únicos en tus datos
unique_states_data <- unique(data_estado_total$estado)
print("Estados en tus datos (después de estandarizar):")
print(unique_states_data)

# Nombres únicos en el mapa
unique_states_map <- unique(mexico_map_sf$NAME_1)
print("Estados en el mapa (después de estandarizar):")
print(unique_states_map)

# Identificar estados no coincidentes
estados_no_coincidentes <- setdiff(unique_states_data, unique_states_map)
print("Estados no coincidentes antes de aplicar equivalencias:")
print(estados_no_coincidentes)


# Unir tus datos con la tabla de equivalencias
data_estado_total <- data_estado_total %>%
  left_join(equivalencias, by = c("estado" = "estado_datos")) %>%
  mutate(
    estado = ifelse(is.na(estado_mapa), estado, estado_mapa)
  ) %>%
  select(-estado_mapa)

# Verificar estados no coincidentes después de aplicar equivalencias
estados_no_coincidentes <- setdiff(unique(data_estado_total$estado), unique_states_map)
print("Estados no coincidentes después de aplicar equivalencias:")
print(estados_no_coincidentes)

# Unir tus datos con el mapa
map_data <- mexico_map_sf %>%
  left_join(data_estado_total, by = c("NAME_1" = "estado"))

# Visualizar el mapa
ggplot(map_data) +
  geom_sf(aes(fill = total_secuestros)) +
  scale_fill_gradient(low = "cyan", high = "blue", na.value = "grey80") +
  labs(
    title = "Distribución Geográfica de Secuestros en México durante enero a octubre 2024",
    fill = "Cantidad de Secuestros"
  ) +
  theme_minimal()
#------------------------------------------------------------------
#Funcional
library(dplyr)
library(ggplot2)

# Convertir 'año_secuestro' a numérico si no lo está
data <- data %>%
  mutate(año_secuestro = as.numeric(año_secuestro))

# Filtrar datos válidos
data <- data %>%
  filter(!is.na(municipio), !is.na(año_secuestro), !is.na(tipo_secuestro))

# Conteo de secuestros por municipio
municipios_secuestros <- data %>%
  group_by(municipio) %>%
  summarise(total_secuestros = n()) %>%
  arrange(desc(total_secuestros)) %>%
  slice(1:15)

# Ver los 15 municipios seleccionados
print(municipios_secuestros)

# Obtener la lista de los 15 municipios
top_municipios <- municipios_secuestros$municipio

# Dividir los municipios en grupos de 3
lista_municipios_grupos <- split(top_municipios, ceiling(seq_along(top_municipios)/3))

# Iterar sobre cada grupo de municipios
for (i in seq_along(lista_municipios_grupos)) {
  # Obtener el grupo actual de municipios
  municipios_grupo <- lista_municipios_grupos[[i]]
  
  # Filtrar los datos para los municipios del grupo actual
  data_grupo <- data %>%
    filter(municipio %in% municipios_grupo)
  
  # Agrupar datos por municipio y año
  data_municipio_año <- data_grupo %>%
    group_by(municipio, año_secuestro) %>%
    summarise(total_secuestros = n()) %>%
    ungroup()
  
  # Crear la gráfica
  titulo_municipios <- paste(municipios_grupo, collapse = ", ")
  grafica <- ggplot(data_municipio_año, aes(x = año_secuestro, y = total_secuestros, fill = as.factor(año_secuestro))) +
    geom_bar(stat = "identity") +
    facet_wrap(~ municipio, scales = "free_y") +
    labs(
      title = paste("Secuestros por Año en:", titulo_municipios),
      x = "Año",
      y = "Cantidad de Secuestros",
      fill = "Año"
    ) +
    theme_minimal() +
    theme(
      legend.position = "bottom",
      axis.text.x = element_text(angle = 45, hjust = 1)
    )
  
  # Mostrar la gráfica
  print(grafica)
  
}
#---------------------------------------
# Cargar las librerías
library(geodata)
library(sf)
library(ggplot2)
library(dplyr)

# Filtrar los datos para el año 2022
data_2022 <- data %>%
  filter(año_secuestro == 2021)

# Agrupar los datos por estado y contar los secuestros
data_estado_total <- data_2022 %>%
  group_by(estado) %>%
  summarise(total_secuestros = n()) %>%
  ungroup()

# Descargar y cargar el mapa de México a nivel de estados
mexico_map <- geodata::gadm(country = "MEX", level = 1, path = tempdir())

# Convertir a objeto sf si no lo es
mexico_map_sf <- st_as_sf(mexico_map)

# Estandarizar nombres en tus datos
data_estado_total$estado <- tolower(iconv(data_estado_total$estado, to = "ASCII//TRANSLIT"))

# Estandarizar nombres en el mapa
mexico_map_sf$NAME_1 <- tolower(iconv(mexico_map_sf$NAME_1, to = "ASCII//TRANSLIT"))

# Nombres únicos en tus datos
unique_states_data <- unique(data_estado_total$estado)
print("Estados en tus datos (después de estandarizar):")
print(unique_states_data)

# Nombres únicos en el mapa
unique_states_map <- unique(mexico_map_sf$NAME_1)
print("Estados en el mapa (después de estandarizar):")
print(unique_states_map)

# Identificar estados no coincidentes
estados_no_coincidentes <- setdiff(unique_states_data, unique_states_map)
print("Estados no coincidentes antes de aplicar equivalencias:")
print(estados_no_coincidentes)


# Unir tus datos con la tabla de equivalencias
data_estado_total <- data_estado_total %>%
  left_join(equivalencias, by = c("estado" = "estado_datos")) %>%
  mutate(
    estado = ifelse(is.na(estado_mapa), estado, estado_mapa)
  ) %>%
  select(-estado_mapa)

# Verificar estados no coincidentes después de aplicar equivalencias
estados_no_coincidentes <- setdiff(unique(data_estado_total$estado), unique_states_map)
print("Estados no coincidentes después de aplicar equivalencias:")
print(estados_no_coincidentes)

# Unir tus datos con el mapa
map_data <- mexico_map_sf %>%
  left_join(data_estado_total, by = c("NAME_1" = "estado"))

# Visualizar el mapa
ggplot(map_data) +
  geom_sf(aes(fill = total_secuestros)) +
  scale_fill_gradient(low = "pink", high = "red", na.value = "white") +
  labs(
    title = "Distribución Geográfica de Secuestros en México en 2021",
    fill = "Cantidad de Secuestros"
  ) +
  theme_minimal()
#-----------------------------------------------------------
# Cargar las librerías
library(geodata)
library(sf)
library(ggplot2)
library(dplyr)

# Filtrar los datos para el año 2022
data_2022 <- data %>%
  filter(año_secuestro == 2020)

# Agrupar los datos por estado y contar los secuestros
data_estado_total <- data_2022 %>%
  group_by(estado) %>%
  summarise(total_secuestros = n()) %>%
  ungroup()

# Descargar y cargar el mapa de México a nivel de estados
mexico_map <- geodata::gadm(country = "MEX", level = 1, path = tempdir())

# Convertir a objeto sf si no lo es
mexico_map_sf <- st_as_sf(mexico_map)

# Estandarizar nombres en tus datos
data_estado_total$estado <- tolower(iconv(data_estado_total$estado, to = "ASCII//TRANSLIT"))

# Estandarizar nombres en el mapa
mexico_map_sf$NAME_1 <- tolower(iconv(mexico_map_sf$NAME_1, to = "ASCII//TRANSLIT"))

# Nombres únicos en tus datos
unique_states_data <- unique(data_estado_total$estado)
print("Estados en tus datos (después de estandarizar):")
print(unique_states_data)

# Nombres únicos en el mapa
unique_states_map <- unique(mexico_map_sf$NAME_1)
print("Estados en el mapa (después de estandarizar):")
print(unique_states_map)

# Identificar estados no coincidentes
estados_no_coincidentes <- setdiff(unique_states_data, unique_states_map)
print("Estados no coincidentes antes de aplicar equivalencias:")
print(estados_no_coincidentes)


# Unir tus datos con la tabla de equivalencias
data_estado_total <- data_estado_total %>%
  left_join(equivalencias, by = c("estado" = "estado_datos")) %>%
  mutate(
    estado = ifelse(is.na(estado_mapa), estado, estado_mapa)
  ) %>%
  select(-estado_mapa)

# Verificar estados no coincidentes después de aplicar equivalencias
estados_no_coincidentes <- setdiff(unique(data_estado_total$estado), unique_states_map)
print("Estados no coincidentes después de aplicar equivalencias:")
print(estados_no_coincidentes)

# Unir tus datos con el mapa
map_data <- mexico_map_sf %>%
  left_join(data_estado_total, by = c("NAME_1" = "estado"))

# Visualizar el mapa
ggplot(map_data) +
  geom_sf(aes(fill = total_secuestros)) +
  scale_fill_gradient(low = "yellow", high = "green", na.value = "white") +
  labs(
    title = "Distribución Geográfica de Secuestros en México en 2020",
    fill = "Cantidad de Secuestros"
  ) +
  theme_minimal()

# Instalar paquetes si es necesario
# install.packages(c("DBI", "RMySQL", "dplyr", "ggplot2", "tidyr", "reshape2"))

# Cargar librerías
library(DBI)
library(RMySQL)
library(dplyr)
library(ggplot2)
library(tidyr)
library(reshape2)

# Establecer conexión a la base de datos
con <- dbConnect(RMySQL::MySQL(),
                 dbname = "noticias",
                 host = "localhost",
                 port = 3306,
                 user = "root",
                 password = "Soccer.8a")

# Cargar datos de la tabla 'extracciones_filtradas'
data <- dbGetQuery(con, "SELECT * FROM extracciones_filtradas")

# Asegurarse de que los campos 'mes_secuestro' y 'año_secuestro' sean numéricos
data <- data %>%
  mutate(
    mes_secuestro = as.numeric(mes_secuestro),
    año_secuestro = as.numeric(año_secuestro)
  )

# Filtrar datos válidos (meses entre 1 y 12)
data <- data %>%
  filter(!is.na(mes_secuestro), !is.na(año_secuestro), mes_secuestro >= 1 & mes_secuestro <= 12)

# Crear una columna de fecha (usando el primer día del mes)
data <- data %>%
  mutate(fecha_secuestro = as.Date(paste(año_secuestro, mes_secuestro, "01", sep = "-")))

# --------------------------------
# Filtrar valores faltantes en 'liberacion'
data <- data %>%
  filter(!is.na(liberacion))

# --------------------------------
# Paso 1: Agrupar y Calcular Porcentajes

# Agrupar por año y liberación, contar los secuestros
liberacion_anual <- data %>%
  group_by(año_secuestro, liberacion) %>%
  summarise(total_secuestros = n()) %>%
  ungroup()

# Calcular el porcentaje de secuestros con y sin liberación para cada año
liberacion_anual <- liberacion_anual %>%
  group_by(año_secuestro) %>%
  mutate(porcentaje = (total_secuestros / sum(total_secuestros)) * 100) %>%
  ungroup()

# Verificar los datos
head(liberacion_anual)

# --------------------------------
# Paso 2: Crear la Gráfica de Barras Apiladas en Porcentajes

ggplot(liberacion_anual, aes(x = factor(año_secuestro), y = porcentaje, fill = liberacion)) +
  geom_bar(stat = "identity") +
  scale_y_continuous(labels = scales::percent_format(scale = 1)) +
  labs(title = "Porcentaje de Secuestros con y sin Liberación por Año",
       x = "Año",
       y = "Porcentaje de Secuestros (%)",
       fill = "¿Hubo Liberación?") +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  scale_fill_manual(values = c("Sí" = "#1f78b4", "No" = "#e31a1c"))

# Instalar paquetes si es necesario
# install.packages(c("DBI", "RMySQL", "dplyr", "ggplot2", "tidyr"))

# Cargar librerías
library(DBI)
library(RMySQL)
library(dplyr)
library(ggplot2)
library(tidyr)

# Establecer conexión a la base de datos
con <- dbConnect(RMySQL::MySQL(),
                 dbname = "noticias",
                 host = "localhost",
                 port = 3306,
                 user = "root",
                 password = "Soccer.8a")

# Cargar datos de la tabla 'extracciones_filtradas'
data <- dbGetQuery(con, "SELECT * FROM extracciones_filtradas")

# Asegurarse de que los campos 'mes_secuestro' y 'año_secuestro' sean numéricos
data <- data %>%
  mutate(
    mes_secuestro = as.numeric(mes_secuestro),
    año_secuestro = as.numeric(año_secuestro)
  )

# Filtrar datos válidos (meses entre 1 y 12)
data <- data %>%
  filter(!is.na(mes_secuestro), !is.na(año_secuestro), mes_secuestro >= 1 & mes_secuestro <= 12)

# Crear una columna de fecha (usando el primer día del mes)
data <- data %>%
  mutate(fecha_secuestro = as.Date(paste(año_secuestro, mes_secuestro, "01", sep = "-")))

# --------------------------------
# Paso 1: Conteo de Secuestros por Mes
secuestros_tiempo <- data %>%
  group_by(fecha_secuestro) %>%
  summarise(total_secuestros = n()) %>%
  ungroup()

# Paso 2: Calcular el Promedio Anual de Secuestros
promedio_anual <- data %>%
  group_by(año_secuestro) %>%
  summarise(promedio_secuestros = mean(n())) # Esto podría no ser correcto

# Nota: El cálculo anterior puede no ser correcto porque `n()` dentro de `summarise` sin agrupar por mes no funcionará como se espera. En su lugar, calcularemos el total por año y luego el promedio mensual.

# Correcto: Calcular el total de secuestros por año y luego el promedio mensual
promedio_anual <- data %>%
  group_by(año_secuestro) %>%
  summarise(total_secuestros_anual = n(),
            promedio_mensual = total_secuestros_anual / 12) %>%
  ungroup()

# Crear una columna de fecha representativa para cada año (ejemplo: 1 de julio de cada año)
promedio_anual <- promedio_anual %>%
  mutate(fecha_promedio = as.Date(paste(año_secuestro, "07", "01", sep = "-"))) # Centro del año

# --------------------------------
# Paso 3: Visualización

ggplot() +
  # Línea de tendencia mensual
  geom_line(data = secuestros_tiempo, aes(x = fecha_secuestro, y = total_secuestros), color = "blue") +
  
  # Puntos de secuestros mensuales
  geom_point(data = secuestros_tiempo, aes(x = fecha_secuestro, y = total_secuestros), color = "lightblue", alpha = 0.6, size = 1) +
  
  # Puntos de promedio anual
  geom_point(data = promedio_anual, aes(x = fecha_promedio, y = promedio_mensual), color = "red", size = 3) +
  
  # Línea de promedio anual
  geom_line(data = promedio_anual, aes(x = fecha_promedio, y = promedio_mensual), color = "red", linetype = "dashed") +
  
  # Etiquetas y Títulos
  labs(title = "Tendencia de Secuestros 2016 - Nov 2024",
       subtitle = "Línea azul: Secuestros mensuales | Puntos rojos: Promedio mensual anual",
       x = "Fecha",
       y = "Cantidad de Secuestros") +
  
  # Tema minimalista
  theme_minimal() +
  
  # Rotar etiquetas del eje X para mejor legibilidad
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  
  # Escala del eje Y
  scale_y_continuous(expand = expansion(mult = c(0, 0.05)))

# Instalar paquetes si es necesario
# install.packages(c("DBI", "RMySQL", "dplyr", "ggplot2", "tidyr"))

# Cargar librerías
library(DBI)
library(RMySQL)
library(dplyr)
library(ggplot2)
library(tidyr)

# Establecer conexión a la base de datos
con <- dbConnect(RMySQL::MySQL(),
                 dbname = "noticias",
                 host = "localhost",
                 port = 3306,
                 user = "root",
                 password = "Soccer.8a")

# Cargar datos de la tabla 'extracciones_filtradas'
data <- dbGetQuery(con, "SELECT * FROM extracciones_filtradas")

# Asegurarse de que los campos 'mes_secuestro' y 'año_secuestro' sean numéricos
data <- data %>%
  mutate(
    mes_secuestro = as.numeric(mes_secuestro),
    año_secuestro = as.numeric(año_secuestro)
  )

# Filtrar datos válidos (meses entre 1 y 12)
data <- data %>%
  filter(!is.na(mes_secuestro), !is.na(año_secuestro), mes_secuestro >= 1 & mes_secuestro <= 12)

# Crear una columna de fecha (usando el primer día del mes)
data <- data %>%
  mutate(fecha_secuestro = as.Date(paste(año_secuestro, mes_secuestro, "01", sep = "-")))

# -------------------------------
# Paso 1: Calcular el Promedio Mensual de Secuestros por Año

promedio_anual <- data %>%
  group_by(año_secuestro) %>%
  summarise(total_secuestros_anual = n()) %>%
  mutate(promedio_mensual = total_secuestros_anual / 12) %>%
  ungroup()

# Si hay años con menos de 12 meses de datos, ajusta el denominador
# Por ejemplo, contando los meses únicos por año
meses_por_año <- data %>%
  group_by(año_secuestro) %>%
  summarise(meses_disponibles = n_distinct(mes_secuestro)) 

promedio_anual <- promedio_anual %>%
  left_join(meses_por_año, by = "año_secuestro") %>%
  mutate(promedio_mensual = total_secuestros_anual / meses_disponibles) %>%
  select(año_secuestro, promedio_mensual)

# -------------------------------
# Paso 2: Crear la Gráfica de Promedios Anuales

ggplot(promedio_anual, aes(x = año_secuestro, y = promedio_mensual)) +
  geom_point(color = "red", size = 3) +                  # Puntos de promedio anual
  geom_line(color = "red", linetype = "dashed") +        # Línea que conecta los puntos
  labs(title = "Promedio Mensual de Secuestros por Año",
       subtitle = "2016 - Noviembre 2024",
       x = "Año",
       y = "Promedio Mensual de Secuestros") +
  theme_minimal() +
  theme(
    axis.text.x = element_text(angle = 45, hjust = 1),
    plot.title = element_text(face = "bold", size = 14),
    plot.subtitle = element_text(size = 12)
  ) +
  scale_x_continuous(breaks = unique(promedio_anual$año_secuestro))

#----------------------

# Cargar las librerías necesarias
library(dplyr)
library(ggplot2)

# Supongamos que tu dataset ya se llama 'data' y está correctamente procesado.

# 1. Agrupar datos por año y tipo de captor
data_anio_captor <- data %>%
  group_by(año_secuestro, captor) %>%
  summarise(total_secuestros = n(), .groups = "drop")

# 2. Visualización
ggplot(data_anio_captor, aes(x = año_secuestro, y = total_secuestros, color = captor)) +
  geom_point(size = 3) +
  geom_line(aes(group = captor), size = 1) +
  labs(
    title = "Secuestros por Año y Tipo de Captor",
    x = "Año",
    y = "Cantidad de Secuestros",
    color = "Tipo de Captor"
  ) +
  theme_minimal() +
  theme(
    axis.text.x = element_text(angle = 45, hjust = 1),
    plot.title = element_text(hjust = 0.5)
  )
#---------------------------------------------

# Instalar paquetes si es necesario
# install.packages(c("dplyr", "ggplot2", "DBI", "RMySQL", "tidyr"))

# Cargar librerías
library(DBI)
library(RMySQL)
library(dplyr)
library(ggplot2)
library(tidyr)

# Establecer conexión a la base de datos
con <- dbConnect(RMySQL::MySQL(),
                 dbname = "noticias",
                 host = "localhost",
                 port = 3306,
                 user = "root",
                 password = "Soccer.8a")

# Cargar datos de la tabla 'extracciones_filtradas'
data <- dbGetQuery(con, "SELECT * FROM extracciones_filtradas")

# Convertir 'año_secuestro' y 'mes_secuestro' a numérico si no lo están
data <- data %>%
  mutate(
    mes_secuestro = as.numeric(mes_secuestro),
    año_secuestro = as.numeric(año_secuestro)
  )

# Filtrar datos válidos (meses entre 1 y 12)
data <- data %>%
  filter(!is.na(mes_secuestro), !is.na(año_secuestro), mes_secuestro >= 1 & mes_secuestro <= 12)

# Crear una columna de fecha (usando el primer día del mes)
data <- data %>%
  mutate(fecha_secuestro = as.Date(paste(año_secuestro, mes_secuestro, "01", sep = "-")))

#-----------------------------------------------------------------------------
# 1. Conteo de secuestros por estado (modificado)
secuestros_estado <- data %>%
  group_by(estado) %>%
  summarise(total_secuestros = n()) %>%
  arrange(desc(total_secuestros))

# Visualización por estado (barras verticales y sin ordenar)
ggplot(secuestros_estado, aes(x = estado, y = total_secuestros)) +
  geom_bar(stat = "identity", fill = "steelblue") +
  labs(title = "Distribución de Secuestros por Estado",
       x = "Estado",
       y = "Cantidad de Secuestros") +
  theme_minimal() +
  theme(
    legend.position = "none",
    axis.text.x = element_text(angle = 45, hjust = 1)
  )

#-----------------------------------------------------------------------------
# 2. Conteo de secuestros por año
secuestros_anio <- data %>%
  group_by(año_secuestro) %>%
  summarise(total_secuestros = n(), .groups = "drop") %>%
  arrange(año_secuestro)

# Visualización: Gráfico de Líneas
ggplot(secuestros_anio, aes(x = año_secuestro, y = total_secuestros)) +
  geom_line(color = "darkgreen", size = 1) +
  geom_point(color = "darkgreen", size = 2) +
  labs(title = "Tendencia de Secuestros por Año",
       x = "Año",
       y = "Cantidad de Secuestros") +
  theme_minimal() +
  theme(
    axis.text.x = element_text(angle = 45, hjust = 1)
  )

#-----------------------------------------------------------------------------
# 3. Conteo de secuestros por estado y año
secuestros_estado_anio <- data %>%
  group_by(estado, año_secuestro) %>%
  summarise(total_secuestros = n(), .groups = "drop")

# Visualización: Gráfico de Líneas por Estado
ggplot(secuestros_estado_anio, aes(x = año_secuestro, y = total_secuestros, color = estado)) +
  geom_line(size = 1) +
  geom_point(size = 2) +
  labs(title = "Tendencia de Secuestros por Año y Estado",
       x = "Año",
       y = "Cantidad de Secuestros",
       color = "Estado") +
  theme_minimal() +
  theme(
    axis.text.x = element_text(angle = 45, hjust = 1)
  )

#-----------------------------------------------------------------------------
# 4. Visualización combinada con facetas: Secuestros por Año y Estado
ggplot(secuestros_estado_anio, aes(x = factor(año_secuestro), y = total_secuestros, fill = factor(año_secuestro))) +
  geom_bar(stat = "identity") +
  facet_wrap(~ estado, ncol = 3, scales = "free_y") + # 3 columnas para distribuir las facetas
  labs(title = "Distribución de Secuestros por Año en Cada Estado",
       x = "Año",
       y = "Cantidad de Secuestros",
       fill = "Año") +
  theme_minimal() +
  theme(
    legend.position = "none",
    axis.text.x = element_text(angle = 45, hjust = 1),
    plot.title = element_text(hjust = 0.5)
  )

# Cerrar la conexión a la base de datos
dbDisconnect(con)
#--------------------------------------------
# Cargar las librerías
library(geodata)
library(sf)
library(ggplot2)
library(dplyr)

# Filtrar los datos para el año 2022
data_2022 <- data %>%
  filter(año_secuestro == 2019)

# Agrupar los datos por estado y contar los secuestros
data_estado_total <- data_2022 %>%
  group_by(estado) %>%
  summarise(total_secuestros = n()) %>%
  ungroup()

# Descargar y cargar el mapa de México a nivel de estados
mexico_map <- geodata::gadm(country = "MEX", level = 1, path = tempdir())

# Convertir a objeto sf si no lo es
mexico_map_sf <- st_as_sf(mexico_map)

# Estandarizar nombres en tus datos
data_estado_total$estado <- tolower(iconv(data_estado_total$estado, to = "ASCII//TRANSLIT"))

# Estandarizar nombres en el mapa
mexico_map_sf$NAME_1 <- tolower(iconv(mexico_map_sf$NAME_1, to = "ASCII//TRANSLIT"))

# Nombres únicos en tus datos
unique_states_data <- unique(data_estado_total$estado)
print("Estados en tus datos (después de estandarizar):")
print(unique_states_data)

# Nombres únicos en el mapa
unique_states_map <- unique(mexico_map_sf$NAME_1)
print("Estados en el mapa (después de estandarizar):")
print(unique_states_map)

# Identificar estados no coincidentes
estados_no_coincidentes <- setdiff(unique_states_data, unique_states_map)
print("Estados no coincidentes antes de aplicar equivalencias:")
print(estados_no_coincidentes)


# Unir tus datos con la tabla de equivalencias
data_estado_total <- data_estado_total %>%
  left_join(equivalencias, by = c("estado" = "estado_datos")) %>%
  mutate(
    estado = ifelse(is.na(estado_mapa), estado, estado_mapa)
  ) %>%
  select(-estado_mapa)

# Verificar estados no coincidentes después de aplicar equivalencias
estados_no_coincidentes <- setdiff(unique(data_estado_total$estado), unique_states_map)
print("Estados no coincidentes después de aplicar equivalencias:")
print(estados_no_coincidentes)

# Unir tus datos con el mapa
map_data <- mexico_map_sf %>%
  left_join(data_estado_total, by = c("NAME_1" = "estado"))

# Visualizar el mapa
ggplot(map_data) +
  geom_sf(aes(fill = total_secuestros)) +
  scale_fill_gradient(low = "yellow", high = "brown", na.value = "white") +
  labs(
    title = "Distribución Geográfica de Secuestros en México en 2019",
    fill = "Cantidad de Secuestros"
  ) +
  theme_minimal()

#--------------------------------------------------------
# Instalar paquetes si es necesario
# install.packages(c("DBI", "RMySQL", "dplyr", "ggplot2", "tidyr"))

# Cargar librerías
library(DBI)
library(RMySQL)
library(dplyr)
library(ggplot2)
library(tidyr)

# Establecer conexión a la base de datos
con <- dbConnect(RMySQL::MySQL(),
                 dbname = "noticias",
                 host = "localhost",
                 port = 3306,
                 user = "root",
                 password = "Soccer.8a")

# Cargar datos de la tabla 'extracciones_filtradas'
data <- dbGetQuery(con, "SELECT * FROM extracciones_filtradas")

# Convertir 'año_secuestro' y 'mes_secuestro' a numérico si no lo están
data <- data %>%
  mutate(
    mes_secuestro = as.numeric(mes_secuestro),
    año_secuestro = as.numeric(año_secuestro)
  )

# Filtrar datos válidos (meses entre 1 y 12)
data <- data %>%
  filter(!is.na(mes_secuestro), !is.na(año_secuestro), mes_secuestro >= 1 & mes_secuestro <= 12)

# Crear una columna de fecha (usando el primer día del mes)
data <- data %>%
  mutate(fecha_secuestro = as.Date(paste(año_secuestro, mes_secuestro, "01", sep = "-")))

#-----------------------------------------------------------------------------
# 1. Conteo de secuestros por estado y seleccionar los 6 con más secuestros
secuestros_estado <- data %>%
  group_by(estado) %>%
  summarise(total_secuestros = n(), .groups = "drop") %>%
  arrange(desc(total_secuestros)) %>%
  slice(1:6) # Seleccionar los 6 primeros

# Ver los 6 municipios seleccionados (opcional)
print(secuestros_estado)

# 2. Obtener la lista de los 6 estados con más secuestros
top_estados <- secuestros_estado$estado

# 3. Filtrar los datos para esos 6 estados
data_top_estados <- data %>%
  filter(estado %in% top_estados)

# 4. Agrupar datos por estado, año y tipo de secuestro
data_estado_anio_secuestro <- data_top_estados %>%
  group_by(estado, año_secuestro, tipo_secuestro) %>%
  summarise(total_secuestros = n(), .groups = "drop")

# 5. Generar gráficas separadas para cada estado
# Crear una lista de los 6 estados
estados_list <- unique(data_estado_anio_secuestro$estado)

# Iterar sobre cada estado y crear una gráfica individual
for (estado_actual in estados_list) {
  
  # Filtrar los datos para el estado actual
  datos_estado <- data_estado_anio_secuestro %>%
    filter(estado == estado_actual)
  
  # Crear la gráfica para el estado actual
  grafica_estado <- ggplot(datos_estado, aes(x = factor(año_secuestro), y = total_secuestros, fill = tipo_secuestro)) +
    geom_bar(stat = "identity", position = "dodge") +
    labs(
      title = paste("Distribución de Tipos de Secuestro por Año en", estado_actual),
      x = "Año",
      y = "Cantidad de Secuestros",
      fill = "Tipo de Secuestro"
    ) +
    theme_minimal() +
    theme(
      axis.text.x = element_text(angle = 45, hjust = 1),
      plot.title = element_text(hjust = 0.5)
    )
  
  # Mostrar la gráfica en el panel de Plots
  print(grafica_estado)
  
  # Opcional: Pausar brevemente entre gráficas para facilitar la visualización
  Sys.sleep(1) # Pausa de 1 segundo entre gráficas
}

# Cerrar la conexión a la base de datos
dbDisconnect(con)

