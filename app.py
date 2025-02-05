import streamlit as st
import zipfile
import io
import xml.etree.ElementTree as ET
import pandas as pd
from st_aggrid import AgGrid, GridOptionsBuilder, DataReturnMode, GridUpdateMode

###############################################################################
#             INICIALIZACIÓN DE VARIABLES EN st.session_state                 #
###############################################################################
if 'df_recibidos' not in st.session_state:
    st.session_state.df_recibidos = pd.DataFrame()
if 'df_emitidos' not in st.session_state:
    st.session_state.df_emitidos = pd.DataFrame()
if 'filtered_df' not in st.session_state:
    st.session_state.filtered_df = pd.DataFrame()
if 'filtered_df_e' not in st.session_state:
    st.session_state.filtered_df_e = pd.DataFrame()

###############################################################################
#               SOLICITUD DEL RFC DE LA EMPRESA EN LA BARRA LATERAL           #
###############################################################################
company_rfc = st.sidebar.text_input("Ingrese el RFC de su empresa", value="")
if not company_rfc:
    st.warning("Por favor, ingrese el RFC de su empresa para continuar.")
    st.stop()

###############################################################################
#       MAPAS DE CÓDIGOS A DESCRIPCIONES (FORMA DE PAGO, USO CFDI, ETC.)      #
###############################################################################
codigo_map_forma_pago = {
    "01": "Efectivo","02": "Cheque Nominativo","03": "Transferencia Electrónica de Fondos SPEI",
    "04": "Tarjeta de Crédito","05": "Monedero Electrónico","06": "Dinero Electrónico",
    "8":  "Vales de Despensa","12": "Dación en Pago","13": "Pago por Subrogación",
    "14": "Pago por Consignación","15": "Condonación","17": "Compensación","23": "Novación",
    "24": "Confusión","25": "Remisión de Deuda","26": "Prescripción o Caducidad",
    "27": "A Satisfacción del Acreedor","28": "Tarjeta de Débito","29": "Tarjeta de Servicios",
    "30": "Aplicación de Anticipos","31": "Intermediario Pagos","99": "Por Definir",
}
codigo_map_uso_cfdi = {
    "G01": "Adquisición de mercancías","G02": "Devoluciones, descuentos o bonificaciones",
    "G03": "Gastos en general","I01": "Construcciones","I02": "Mobiliario y equipo de oficina por inversiones",
    "I03": "Equipo de transporte","I04": "Equipo de computo y accesorios","I05": "Dados, troqueles, moldes, matrices y herramental",
    "I06": "Comunicaciones telefónicas","I07": "Comunicaciones satelitales","I08": "Otra maquinaria y equipo",
    "D01": "Honorarios médicos, dentales y gastos hospitalarios","D02": "Gastos médicos por incapacidad o discapacidad",
    "D03": "Gastos funerarios","D04": "Donativos","D05": "Intereses reales efectivamente pagados por créditos hipotecarios (casa habitación)",
    "D06": "Aportaciones voluntarias al SAR","D07": "Primas por seguros de gastos médicos",
    "D08": "Gastos de transportación escolar obligatoria","D09": "Depósitos en cuentas para el ahorro, primas que tengan como base planes de pensiones",
    "D10": "Pagos por servicios educativos (colegiaturas)","S01": "Sin efectos fiscales","CP01": "Pagos","CN01": "Nómina",
}

# Columnas que deben ser numéricas (para sumas y cálculos)
resumen_cols = ["Sub Total", "Descuento", "Total impuesto Trasladado",
                "Total impuesto Retenido", "Total", "Traslado IVA 0.160000 %"]

###############################################################################
#  FUNCIÓN PARA DETECTAR Y CORREGIR VALORES NO NUMÉRICOS (APLICABLE A CUALQUIER DF)
###############################################################################
def detectar_y_corregir_valores(df, columnas_numericas):
    """
    Detecta valores no numéricos en las columnas_numericas de un DataFrame
    y permite corregirlos manualmente usando st.number_input.
    """
    for col in columnas_numericas:
        # Convertir la columna a float, forzando a NaN cualquier valor no numérico
        df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Filtrar filas con NaN en la columna col
        filas_nan = df[df[col].isna()]
        if not filas_nan.empty:
            st.warning(f"Se han encontrado valores no numéricos o vacíos en la columna '{col}'.")
            for idx, row in filas_nan.iterrows():
                # Mostrar un number_input para corregir el valor
                nuevo_valor = st.number_input(
                    f"Corregir '{col}' (Archivo: {row.get('XML', 'desconocido')}):",
                    min_value=0.0,
                    value=0.0,
                    key=f"{col}_{idx}"
                )
                df.at[idx, col] = nuevo_valor
    return df

###############################################################################
#          FUNCIÓN PARA FILTRAR DUPLICADOS (UUID) AL CARGAR ZIP               #
###############################################################################
def filtrar_duplicados_por_uuid(nuevo_df, df_existente, uuid_col="UUID"):
    """
    Retorna las filas de 'nuevo_df' cuyo UUID no aparece en df_existente.
    Evita insertar duplicados en la carga inicial.
    """
    if uuid_col not in df_existente.columns:
        return nuevo_df

    uuids_existentes = df_existente[uuid_col].unique()
    return nuevo_df[~nuevo_df[uuid_col].isin(uuids_existentes)]

###############################################################################
#     FUNCIÓN PARA ELIMINAR DUPLICADOS EXISTENTES EN UN DF (MODO DEFINITIVO)  #
###############################################################################
def eliminar_duplicados_en_df(df, uuid_col="UUID"):
    """
    Elimina filas con UUID duplicado (keep='first').
    Devuelve el DF limpio y la cantidad eliminada.
    """
    if uuid_col not in df.columns:
        return df, 0

    n_antes = len(df)
    df_sin_dup = df.drop_duplicates(subset=[uuid_col], keep='first')
    n_despues = len(df_sin_dup)
    eliminados = n_antes - n_despues
    return df_sin_dup, eliminados

###############################################################################
#     FUNCIÓN PARA MOSTRAR INTERFAZ DE ANÁLISIS Y ELIMINACIÓN DE DUPLICADOS   #
###############################################################################
def mostrar_eliminar_duplicados_ui(df, nombre_tabla="Recibidos"):
    """
    Muestra un AgGrid con filas (duplicadas por UUID) y permite seleccionar
    cuáles eliminar definitivamente de 'df'.
    """
    from st_aggrid import GridOptionsBuilder, AgGrid, DataReturnMode, GridUpdateMode

    st.subheader(f"Análisis de duplicados en {nombre_tabla}")

    if "UUID" not in df.columns:
        st.info(f"No existe la columna 'UUID' en la tabla '{nombre_tabla}'.")
        return None

    # Filtramos sólo duplicados
    df_dup = df.copy()
    counts = df_dup["UUID"].value_counts()
    uuids_duplicados = counts[counts > 1].index
    df_duplicados = df_dup[df_dup["UUID"].isin(uuids_duplicados)]

    if df_duplicados.empty:
        st.info("No se encontraron CFDIs duplicados (mismo UUID).")
        return None

    st.markdown(f"Se encontraron **{len(df_duplicados)}** filas con UUID repetido.")
    st.info("Selecciona las filas que deseas **eliminar** de la tabla principal.")

    gb = GridOptionsBuilder.from_dataframe(df_duplicados)
    gb.configure_selection("multiple", use_checkbox=True)
    gridOptions = gb.build()

    grid_response = AgGrid(
        df_duplicados,
        gridOptions=gridOptions,
        data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
        update_mode=GridUpdateMode.SELECTION_CHANGED,
        height=400,
        width=2500,
    )

    filas_seleccionadas = grid_response["selected_rows"]

    if st.button(f"Eliminar duplicados seleccionados en {nombre_tabla}"):
        if not filas_seleccionadas:
            st.warning("No has seleccionado ningún CFDI para eliminar.")
            return None

        # Extraemos los UUID de las filas seleccionadas
        uuids_a_eliminar = [row["UUID"] for row in filas_seleccionadas]
        df_sin_seleccion = df[~df["UUID"].isin(uuids_a_eliminar)]
        st.success(f"Se han eliminado {len(filas_seleccionadas)} filas duplicadas en {nombre_tabla}.")
        return df_sin_seleccion
    return None

###############################################################################
#       FUNCIÓN PARA VERIFICAR SI ZIP ES RECIBIDOS O EMITIDOS CORRECTOS       #
###############################################################################
def identificar_tipo_zip(rows, tipo_deseado):
    if not rows:
        return False
    if tipo_deseado == "Recibidos":
        return all(row.get("Rfc Receptor", "") == company_rfc for row in rows)
    elif tipo_deseado == "Emitidos":
        return all(row.get("Rfc Emisor", "") == company_rfc for row in rows)
    return False

###############################################################################
#     FUNCIÓN PARA PROCESAR ZIP Y DEVOLVER LISTA DE DICTS DE CADA XML         #
###############################################################################
def procesar_zip(uploaded_file):
    zip_bytes = uploaded_file.read()
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as thezip:
        rows = []
        ns = {'cfdi': 'http://www.sat.gob.mx/cfd/4',
              'tfd': 'http://www.sat.gob.mx/TimbreFiscalDigital'}
        for filename in thezip.namelist():
            if filename.lower().endswith(".xml"):
                with thezip.open(filename) as xml_file:
                    try:
                        tree = ET.parse(xml_file)
                        root = tree.getroot()
                    except ET.ParseError:
                        st.warning(f"Error al parsear el archivo XML: {filename}")
                        continue
                    row = {
                        "XML": filename,
                        "Rfc Emisor": "",
                        "Nombre Emisor": "",
                        "Régimen Fiscal Emisor": "",
                        "Rfc Receptor": "",
                        "Nombre Receptor": "",
                        "CP Receptor": "",
                        "Régimen Receptor": "",
                        "Uso Cfdi Receptor": "",
                        "Tipo": "",
                        "Serie": "",
                        "Folio": "",
                        "Fecha": "",
                        "Sub Total": "",
                        "Descuento": "",
                        "Total impuesto Trasladado": "",
                        "Nombre Impuesto": "",
                        "Total impuesto Retenido": "",
                        "Total": "",
                        "UUID": "",
                        "Método de Pago": "",
                        "Forma de Pago": "",
                        "Moneda": "",
                        "Tipo de Cambio": "",
                        "Versión": "",
                        "Estado": "",
                        "Estatus": "",
                        "Validación EFOS": "",
                        "Fecha Consulta": "",
                        "Conceptos": "",
                        "Relacionados": "",
                        "Tipo Relación": "",
                        "Traslado IVA 0.160000 %": ""
                    }

                    # Atributos del comprobante
                    row["Fecha"] = root.attrib.get("Fecha", "")
                    row["Sub Total"] = root.attrib.get("SubTotal", "")
                    row["Descuento"] = root.attrib.get("Descuento", "")
                    row["Total"] = root.attrib.get("Total", "")
                    row["Método de Pago"] = root.attrib.get("MetodoPago", "")
                    
                    forma_pago_codigo = root.attrib.get("FormaPago", "")
                    forma_pago_desc = codigo_map_forma_pago.get(forma_pago_codigo, "")
                    row["Forma de Pago"] = f"{forma_pago_codigo}-{forma_pago_desc}" if forma_pago_desc else forma_pago_codigo
                    row["Moneda"] = root.attrib.get("Moneda", "")
                    row["Tipo de Cambio"] = root.attrib.get("TipoCambio", "")
                    row["Versión"] = root.attrib.get("Version", "")
                    row["Serie"] = root.attrib.get("Serie", "")
                    row["Folio"] = root.attrib.get("Folio", "")
                    row["Tipo"] = root.attrib.get("TipoDeComprobante", "")

                    # Emisor
                    emisor = root.find("cfdi:Emisor", namespaces=ns)
                    if emisor is not None:
                        row["Rfc Emisor"] = emisor.attrib.get("Rfc", "")
                        row["Nombre Emisor"] = emisor.attrib.get("Nombre", "")
                        row["Régimen Fiscal Emisor"] = emisor.attrib.get("RegimenFiscal", "")

                    # Receptor
                    receptor = root.find("cfdi:Receptor", namespaces=ns)
                    if receptor is not None:
                        row["Rfc Receptor"] = receptor.attrib.get("Rfc", "")
                        row["Nombre Receptor"] = receptor.attrib.get("Nombre", "")
                        row["CP Receptor"] = receptor.attrib.get("DomicilioFiscalReceptor", "")
                        row["Régimen Receptor"] = receptor.attrib.get("RegimenFiscalReceptor", "")
                        uso_cfdi_codigo = receptor.attrib.get("UsoCFDI", "")
                        uso_cfdi_desc = codigo_map_uso_cfdi.get(uso_cfdi_codigo, "")
                        if uso_cfdi_codigo:
                            row["Uso Cfdi Receptor"] = f"{uso_cfdi_codigo}-{uso_cfdi_desc}" if uso_cfdi_desc else uso_cfdi_codigo

                    # Timbre Fiscal Digital
                    timbre = root.find(".//tfd:TimbreFiscalDigital", namespaces=ns)
                    if timbre is not None:
                        row["UUID"] = timbre.attrib.get("UUID", "")

                    # Impuestos
                    impuestos_elem = root.find("cfdi:Impuestos", namespaces=ns)
                    total_trasladado = None
                    if impuestos_elem is not None:
                        total_trasladado = impuestos_elem.attrib.get("TotalImpuestosTrasladados", None)
                    impuestos_nombres = set()
                    traslado_iva_016 = ""

                    # Si la etiqueta no tiene el total, calculamos manualmente
                    if total_trasladado is None:
                        total_trasladado_calc = 0.0
                        for traslado in root.findall(".//cfdi:Traslado", namespaces=ns):
                            try:
                                total_trasladado_calc += float(traslado.attrib.get("Importe", "0"))
                            except (ValueError, TypeError):
                                pass
                            imp = traslado.attrib.get("Impuesto", "")
                            if imp:
                                impuestos_nombres.add(imp)
                            # Caso específico de IVA 0.160000
                            if (
                                traslado.attrib.get("TasaOCuota") == "0.160000"
                                and traslado.attrib.get("Impuesto") == "002"
                            ):
                                traslado_iva_016 = traslado.attrib.get("Importe", "")
                        row["Total impuesto Trasladado"] = total_trasladado_calc
                    else:
                        # Asumir que es válido
                        row["Total impuesto Trasladado"] = total_trasladado
                        # Ver si hay desglose de los impuestos
                        for traslado in root.findall(".//cfdi:Traslado", namespaces=ns):
                            imp = traslado.attrib.get("Impuesto", "")
                            if imp:
                                impuestos_nombres.add(imp)
                            if (
                                traslado.attrib.get("TasaOCuota") == "0.160000"
                                and traslado.attrib.get("Impuesto") == "002"
                            ):
                                traslado_iva_016 = traslado.attrib.get("Importe", "")

                    # Retenciones
                    total_retenido = 0.0
                    for retencion in root.findall(".//cfdi:Retencion", namespaces=ns):
                        try:
                            total_retenido += float(retencion.attrib.get("Importe", "0"))
                        except (ValueError, TypeError):
                            pass
                    row["Total impuesto Retenido"] = total_retenido
                    row["Traslado IVA 0.160000 %"] = traslado_iva_016
                    row["Nombre Impuesto"] = ", ".join(impuestos_nombres)

                    # Conceptos
                    conceptos = root.findall("cfdi:Conceptos/cfdi:Concepto", namespaces=ns)
                    lista_conceptos = [
                        f"{c.attrib.get('Descripcion', '')}: {c.attrib.get('Importe', '')}" for c in conceptos
                    ]
                    row["Conceptos"] = "; ".join(lista_conceptos)
                    rows.append(row)
    return rows

###############################################################################
#   FUNCIONES PARA MOSTRAR SUMATORIAS Y TABLAS
###############################################################################
def mostrar_sumatorias(df, columnas_sumar):
    sumas = {}
    for col in columnas_sumar:
        df[col] = pd.to_numeric(df[col], errors='coerce')
        sumas[col] = df[col].sum()
    return sumas

def mostrar_tabla_seccion(df, titulo, ancho=2500):
    st.subheader(titulo)
    if df.empty:
        st.write(f"No hay datos para {titulo.lower()}.")
    else:
        st.dataframe(df, width=ancho)

###############################################################################
# FUNCIONES DE EXPORTACIÓN (CSV, EXCEL)
###############################################################################
def exportar_csv_single(df):
    return df.to_csv(index=False).encode('utf-8')

def exportar_csv_multiple(dfs, nombres):
    with io.BytesIO() as buffer:
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            for df, name in zip(dfs, nombres):
                csv = df.to_csv(index=False)
                zf.writestr(f"{name}.csv", csv)
        buffer.seek(0)
        return buffer.read()

def exportar_excel_single(df, sheet_name="Datos"):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)
    return output.getvalue()

def exportar_datos(df_recibidos, df_emitidos, formato="Excel"):
    """
    Exporta los datos (recibidos y emitidos) en el formato elegido.
    Aplica corrección de valores no numéricos antes de exportar.
    """
    # Antes de exportar, aseguramos que no haya valores erróneos
    df_recibidos = detectar_y_corregir_valores(df_recibidos, resumen_cols)
    df_emitidos = detectar_y_corregir_valores(df_emitidos, resumen_cols)

    output = io.BytesIO()
    if formato == "Excel":
        df_deducibles = df_recibidos[df_recibidos["Deducible"] == True]
        df_no_deducibles = df_recibidos[df_recibidos["Deducible"] == False]
        df_emitidos_seleccionados = df_emitidos[df_emitidos["Seleccionar"] == True]
        df_emitidos_no_seleccionados = df_emitidos[df_emitidos["Seleccionar"] == False]
        resumen_df = pd.DataFrame([mostrar_sumatorias(df_recibidos, resumen_cols)])
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            df_recibidos.to_excel(writer, sheet_name="Recibidos", index=False)
            df_deducibles.to_excel(writer, sheet_name="Deducibles", index=False)
            df_no_deducibles.to_excel(writer, sheet_name="No Deducibles", index=False)
            df_emitidos.to_excel(writer, sheet_name="Emitidos", index=False)
            df_emitidos_seleccionados.to_excel(writer, sheet_name="Emitidos Seleccionados", index=False)
            df_emitidos_no_seleccionados.to_excel(writer, sheet_name="Emitidos No Seleccionados", index=False)
            resumen_df.to_excel(writer, sheet_name="Resumen", index=False)
        mime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        file_name = "CFDIs_Fiscales.xlsx"
    elif formato == "CSV":
        # Para exportar a CSV unificado
        df_recibidos_export = df_recibidos.copy()
        df_emitidos_export = df_emitidos.copy()
        df_recibidos_export.insert(0, "Tipo", "Recibidos")
        df_emitidos_export.insert(0, "Tipo", "Emitidos")
        df_combined = pd.concat([df_recibidos_export, df_emitidos_export])
        output.write(df_combined.to_csv(index=False).encode("utf-8"))
        mime = "text/csv"
        file_name = "CFDIs_Fiscales.csv"
    output.seek(0)
    return output, file_name, mime

###############################################################################
# SECCIÓN DE CFDIs RECIBIDOS
###############################################################################
def section_recibidos():
    st.header("CFDIs Recibidos")
    
    # Exportar Recibidos (se corrigen valores antes de exportar)
    with st.expander("Exportar Recibidos"):
        formato_recibidos = st.radio("Seleccionar formato", ["CSV", "Excel", "PDF"], key="formato_export_recibidos")
        alcance_recibidos = st.radio("Exportar", ["Tabla Actual", "Toda la Sección"], key="alcance_export_recibidos")
        if st.button("Exportar Recibidos"):
            if "df_recibidos" in st.session_state and not st.session_state.df_recibidos.empty:
                if alcance_recibidos == "Tabla Actual":
                    df_exportar_recibidos = st.session_state.filtered_df.copy()
                else:
                    df_exportar_recibidos = st.session_state.df_recibidos.copy()

                # Corregir valores no numéricos en df_exportar_recibidos
                df_exportar_recibidos = detectar_y_corregir_valores(df_exportar_recibidos, resumen_cols)
                
                if formato_recibidos == "CSV":
                    datos_csv = exportar_csv_single(df_exportar_recibidos)
                    st.download_button(
                        label="Descargar CSV",
                        data=datos_csv,
                        file_name="recibidos_exportados.csv",
                        mime="text/csv"
                    )
                elif formato_recibidos == "Excel":
                    datos_excel = exportar_excel_single(df_exportar_recibidos, "Recibidos")
                    st.download_button(
                        label="Descargar Excel",
                        data=datos_excel,
                        file_name="recibidos_exportados.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                elif formato_recibidos == "PDF":
                    st.warning("Exportación a PDF no implementada en este ejemplo.")
            else:
                st.warning("No hay datos disponibles para exportar en Recibidos.")
    
    # Cargar Archivos ZIP de Recibidos
    uploaded_file_recibidos = st.file_uploader("Cargar archivo ZIP con XMLs Recibidos", type=["zip"], key="recibidos_file")
    if uploaded_file_recibidos is not None:
        rows = procesar_zip(uploaded_file_recibidos)
        if rows:
            # Verificar que los CFDIs en el ZIP son recibidos para la empresa
            if identificar_tipo_zip(rows, "Recibidos"):
                new_df = pd.DataFrame(rows)
                new_df["Deducible"] = True
                # Filtrar duplicados antes de concatenar
                new_df = filtrar_duplicados_por_uuid(new_df, st.session_state.df_recibidos, "UUID")
                if not new_df.empty:
                    # Corregimos valores no numéricos antes de guardar
                    new_df = detectar_y_corregir_valores(new_df, resumen_cols)
                    st.session_state.df_recibidos = pd.concat([st.session_state.df_recibidos, new_df], ignore_index=True)
                    st.success(f"Se han cargado {len(new_df)} CFDIs Recibidos.")
                else:
                    st.info("Todos los CFDIs en el ZIP ya existen (UUIDs duplicados).")
            else:
                st.warning("El archivo ZIP no contiene CFDIs Recibidos válidos para tu empresa.")
        else:
            st.info("No se encontraron archivos XML en el ZIP de Recibidos.")
    
    # Mostrar Información Solo Si Hay Datos Cargados
    if not st.session_state.df_recibidos.empty:
        columnas_sumar = resumen_cols
        
        # Solicitar Selección de Periodo Antes de Mostrar Datos
        st.subheader("Selecciona el Periodo que Deseas Visualizar")
        df_rec = st.session_state.df_recibidos.copy()
        df_rec["Periodo"] = df_rec["Fecha"].apply(lambda x: x[:7] if isinstance(x, str) else "")
        periodos = sorted(df_rec["Periodo"].dropna().unique().tolist())
        
        if periodos:
            periodo_seleccionado = st.selectbox("Periodo", options=periodos, index=len(periodos)-1, key="seleccion_periodo_recibidos")
            df_rec_filtrado = df_rec[df_rec["Periodo"] == periodo_seleccionado]
        else:
            st.warning("No hay periodos disponibles para seleccionar.")
            df_rec_filtrado = pd.DataFrame()
        
        if not df_rec_filtrado.empty:
            st.sidebar.header("Filtros Adicionales")
            emisores = ["Todos"] + sorted(df_rec_filtrado.apply(lambda row: f"{row['Rfc Emisor']} - {row['Nombre Emisor']}", axis=1).unique().tolist())
            emisores_seleccionado = st.sidebar.selectbox("Filtrar por Emisor (RFC - Nombre)", options=emisores, key="filtro_recibidos_emisor")
            
            uso_cfdi_opciones = ["Todos"] + sorted(df_rec_filtrado["Uso Cfdi Receptor"].unique().tolist())
            uso_cfdi_seleccionado = st.sidebar.selectbox("Filtrar por Uso CFDI Receptor", options=uso_cfdi_opciones, key="filtro_recibidos_uso")
            
            forma_pago_opciones = ["Todos"] + sorted(df_rec_filtrado["Forma de Pago"].unique().tolist())
            forma_pago_seleccionado = st.sidebar.selectbox("Filtrar por Forma de Pago", options=forma_pago_opciones, key="filtro_recibidos_forma")
            
            col_sel, col_desel = st.sidebar.columns(2)
            if col_sel.button("Seleccionar todos", key="sel_all_recibidos"):
                st.session_state.df_recibidos.loc[df_rec_filtrado.index, "Deducible"] = True
            if col_desel.button("Deseleccionar todos", key="desel_all_recibidos"):
                st.session_state.df_recibidos.loc[df_rec_filtrado.index, "Deducible"] = False
            
            if emisores_seleccionado != "Todos":
                rfc_emisor_filtrar = emisores_seleccionado.split(' - ')[0]
                df_rec_filtrado = df_rec_filtrado[df_rec_filtrado["Rfc Emisor"] == rfc_emisor_filtrar]
            if uso_cfdi_seleccionado != "Todos":
                df_rec_filtrado = df_rec_filtrado[df_rec_filtrado["Uso Cfdi Receptor"] == uso_cfdi_seleccionado]
            if forma_pago_seleccionado != "Todos":
                df_rec_filtrado = df_rec_filtrado[df_rec_filtrado["Forma de Pago"] == forma_pago_seleccionado]
        
            # Mostrar AgGrid con Datos Filtrados
            gb = GridOptionsBuilder.from_dataframe(df_rec_filtrado)
            gb.configure_column("Deducible", editable=True, cellEditor='agCheckboxCellEditor', pinned=True)
            gb.configure_default_column(editable=True, resizable=True)
            gridOptions = gb.build()
            grid_response = AgGrid(
                df_rec_filtrado,
                gridOptions=gridOptions,
                data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
                update_mode=GridUpdateMode.VALUE_CHANGED,
                height=600,
                width=2500
            )
            edited_df = pd.DataFrame(grid_response['data'])
            
            # Actualizar el Estado de 'Deducible' en el DataFrame Original
            for _, row in edited_df.iterrows():
                identifier = row["XML"]
                st.session_state.df_recibidos.loc[st.session_state.df_recibidos["XML"] == identifier, "Deducible"] = row["Deducible"]
        
            # Guardar DataFrame Filtrado para Exportaciones Parciales (Tabla Actual)
            st.session_state.filtered_df = df_rec_filtrado.copy()
        
            # Tabs para Deducibles y No Deducibles
            tabs_recibidos = st.tabs(["Deducibles", "No Deducibles"])
            with tabs_recibidos[0]:
                deducible_df = df_rec_filtrado[df_rec_filtrado["Deducible"] == True]
                mostrar_tabla_seccion(deducible_df, "XMLs Deducibles")
                st.markdown("**Sumatorias para XMLs Deducibles:**")
                st.table(pd.DataFrame([mostrar_sumatorias(deducible_df, columnas_sumar)]))
            with tabs_recibidos[1]:
                no_deducible_df = df_rec_filtrado[df_rec_filtrado["Deducible"] == False]
                mostrar_tabla_seccion(no_deducible_df, "XMLs No Deducibles")
                st.markdown("**Sumatorias para XMLs No Deducibles:**")
                st.table(pd.DataFrame([mostrar_sumatorias(no_deducible_df, columnas_sumar)]))
        else:
            st.warning("No hay datos disponibles para el periodo seleccionado.")

###############################################################################
# SECCIÓN DE CFDIs EMITIDOS
###############################################################################
def section_emitidos():
    st.header("CFDIs Emitidos")
    
    # Exportar Emitidos (se corrigen valores antes de exportar)
    with st.expander("Exportar Emitidos"):
        formato_emitidos = st.radio("Seleccionar formato", ["CSV", "Excel", "PDF"], key="formato_export_emitidos")
        alcance_emitidos = st.radio("Exportar", ["Tabla Actual", "Toda la Sección"], key="alcance_export_emitidos")
        if st.button("Exportar Emitidos"):
            if "df_emitidos" in st.session_state and not st.session_state.df_emitidos.empty:
                if alcance_emitidos == "Tabla Actual":
                    df_exportar_emitidos = st.session_state.filtered_df_e.copy()
                else:
                    df_exportar_emitidos = st.session_state.df_emitidos.copy()

                # Corregir valores no numéricos
                df_exportar_emitidos = detectar_y_corregir_valores(df_exportar_emitidos, resumen_cols)
                
                if formato_emitidos == "CSV":
                    datos_csv = exportar_csv_single(df_exportar_emitidos)
                    st.download_button(
                        label="Descargar CSV",
                        data=datos_csv,
                        file_name="emitidos_exportados.csv",
                        mime="text/csv"
                    )
                elif formato_emitidos == "Excel":
                    datos_excel = exportar_excel_single(df_exportar_emitidos, "Emitidos")
                    st.download_button(
                        label="Descargar Excel",
                        data=datos_excel,
                        file_name="emitidos_exportados.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                elif formato_emitidos == "PDF":
                    st.warning("Exportación a PDF no implementada en este ejemplo.")
            else:
                st.warning("No hay datos disponibles para exportar en Emitidos.")

    # Cargar Archivos ZIP de Emitidos
    uploaded_file_emitidos = st.file_uploader("Cargar archivo ZIP con XMLs Emitidos", type=["zip"], key="emitidos_file")
    if uploaded_file_emitidos is not None:
        rows = procesar_zip(uploaded_file_emitidos)
        if rows:
            # Verificar que los CFDIs en el ZIP son emitidos para la empresa
            if identificar_tipo_zip(rows, "Emitidos"):
                new_df = pd.DataFrame(rows)
                new_df["Seleccionar"] = True
                # Filtrar duplicados
                new_df = filtrar_duplicados_por_uuid(new_df, st.session_state.df_emitidos, "UUID")
                if not new_df.empty:
                    # Corregir valores no numéricos antes de guardar
                    new_df = detectar_y_corregir_valores(new_df, resumen_cols)
                    st.session_state.df_emitidos = pd.concat([st.session_state.df_emitidos, new_df], ignore_index=True)
                    st.success(f"Se han cargado {len(new_df)} CFDIs Emitidos.")
                else:
                    st.info("Todos los CFDIs en el ZIP ya existen (UUIDs duplicados).")
            else:
                st.warning("El archivo ZIP no contiene CFDIs Emitidos válidos para tu empresa.")
        else:
            st.info("No se encontraron archivos XML en el ZIP de Emitidos.")

    # Mostrar Información Solo Si Hay Datos Cargados
    if not st.session_state.df_emitidos.empty:
        columnas_sumar = resumen_cols
        
        st.subheader("Selecciona el Periodo que Deseas Visualizar")
        df_emit = st.session_state.df_emitidos.copy()
        df_emit["Periodo"] = df_emit["Fecha"].apply(lambda x: x[:7] if isinstance(x, str) else "")
        periodos = sorted(df_emit["Periodo"].dropna().unique().tolist())
        
        if periodos:
            periodo_seleccionado_e = st.selectbox("Periodo", options=periodos, index=len(periodos)-1, key="seleccion_periodo_emitidos")
            df_emit_filtrado = df_emit[df_emit["Periodo"] == periodo_seleccionado_e]
        else:
            st.warning("No hay periodos disponibles para seleccionar.")
            df_emit_filtrado = pd.DataFrame()
        
        if not df_emit_filtrado.empty:
            st.sidebar.header("Filtros Adicionales")
            emisores_e = ["Todos"] + sorted(df_emit_filtrado.apply(lambda row: f"{row['Rfc Emisor']} - {row['Nombre Emisor']}", axis=1).unique().tolist())
            emisores_seleccionado_e = st.sidebar.selectbox("Filtrar por Emisor (RFC - Nombre)", options=emisores_e, key="filtro_emitidos_emisor")
            
            uso_cfdi_opciones_e = ["Todos"] + sorted(df_emit_filtrado["Uso Cfdi Receptor"].unique().tolist())
            uso_cfdi_seleccionado_e = st.sidebar.selectbox("Filtrar por Uso CFDI Receptor", options=uso_cfdi_opciones_e, key="filtro_emitidos_uso")
            
            forma_pago_opciones_e = ["Todos"] + sorted(df_emit_filtrado["Forma de Pago"].unique().tolist())
            forma_pago_seleccionado_e = st.sidebar.selectbox("Filtrar por Forma de Pago", options=forma_pago_opciones_e, key="filtro_emitidos_forma")
            
            col_sel_e, col_desel_e = st.sidebar.columns(2)
            if col_sel_e.button("Seleccionar todos", key="sel_all_emitidos"):
                st.session_state.df_emitidos.loc[df_emit_filtrado.index, "Seleccionar"] = True
            if col_desel_e.button("Deseleccionar todos", key="desel_all_emitidos"):
                st.session_state.df_emitidos.loc[df_emit_filtrado.index, "Seleccionar"] = False
            
            if emisores_seleccionado_e != "Todos":
                rfc_emisor_filtrar_e = emisores_seleccionado_e.split(' - ')[0]
                df_emit_filtrado = df_emit_filtrado[df_emit_filtrado["Rfc Emisor"] == rfc_emisor_filtrar_e]
            if uso_cfdi_seleccionado_e != "Todos":
                df_emit_filtrado = df_emit_filtrado[df_emit_filtrado["Uso Cfdi Receptor"] == uso_cfdi_seleccionado_e]
            if forma_pago_seleccionado_e != "Todos":
                df_emit_filtrado = df_emit_filtrado[df_emit_filtrado["Forma de Pago"] == forma_pago_seleccionado_e]
        
            # AgGrid
            gb_e = GridOptionsBuilder.from_dataframe(df_emit_filtrado)
            gb_e.configure_column("Seleccionar", editable=True, cellEditor='agCheckboxCellEditor', pinned=True)
            gb_e.configure_default_column(editable=True, resizable=True)
            gridOptions_e = gb_e.build()
            grid_response_e = AgGrid(
                df_emit_filtrado,
                gridOptions=gridOptions_e,
                data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
                update_mode=GridUpdateMode.VALUE_CHANGED,
                height=600,
                width=2500
            )
            edited_df_e = pd.DataFrame(grid_response_e["data"])

            # Actualizar 'Seleccionar'
            for _, row in edited_df_e.iterrows():
                identifier = row["XML"]
                st.session_state.df_emitidos.loc[st.session_state.df_emitidos["XML"] == identifier, "Seleccionar"] = row["Seleccionar"]

            # Guardar DF filtrado para exportaciones parciales
            st.session_state.filtered_df_e = df_emit_filtrado.copy()
        
            # Tabs para Seleccionados y No Seleccionados
            tabs_emitidos = st.tabs(["CFDIs Seleccionados", "CFDIs No Seleccionados"])
            with tabs_emitidos[0]:
                seleccionados_df = df_emit_filtrado[df_emit_filtrado["Seleccionar"] == True]
                mostrar_tabla_seccion(seleccionados_df, "CFDIs Seleccionados")
                st.markdown("**Sumatorias para CFDIs Seleccionados:**")
                st.table(pd.DataFrame([mostrar_sumatorias(seleccionados_df, columnas_sumar)]))
            with tabs_emitidos[1]:
                no_seleccionados_df = df_emit_filtrado[df_emit_filtrado["Seleccionar"] == False]
                mostrar_tabla_seccion(no_seleccionados_df, "CFDIs No Seleccionados")
                st.markdown("**Sumatorias para CFDIs No Seleccionados:**")
                st.table(pd.DataFrame([mostrar_sumatorias(no_seleccionados_df, columnas_sumar)]))
        else:
            st.warning("No hay datos disponibles para el periodo seleccionado.")

###############################################################################
# SECCIÓN DE RESUMEN
###############################################################################
def section_resumen():
    st.header("Resumen")
    
    st.subheader("Selecciona el Periodo que Deseas Visualizar")
    
    # Periodos en Recibidos
    periodos_rec = []
    if "df_recibidos" in st.session_state and not st.session_state.df_recibidos.empty:
        df_rec = st.session_state.df_recibidos.copy()
        df_rec["Periodo"] = df_rec["Fecha"].apply(lambda x: x[:7] if isinstance(x, str) else "")
        periodos_rec = df_rec["Periodo"].dropna().unique().tolist()
    
    # Periodos en Emitidos
    periodos_emit = []
    if "df_emitidos" in st.session_state and not st.session_state.df_emitidos.empty:
        df_emit = st.session_state.df_emitidos.copy()
        df_emit["Periodo"] = df_emit["Fecha"].apply(lambda x: x[:7] if isinstance(x, str) else "")
        periodos_emit = df_emit["Periodo"].dropna().unique().tolist()
    
    periodos_combined = sorted(list(set(periodos_rec + periodos_emit)))
    
    if periodos_combined:
        periodo_seleccionado = st.selectbox(
            "Periodo",
            options=periodos_combined,
            index=len(periodos_combined)-1,
            key="seleccion_periodo_resumen"
        )
    else:
        st.warning("No hay periodos disponibles para seleccionar.")
        periodo_seleccionado = None
    
    # Filtrar Recibidos
    if periodo_seleccionado and "df_recibidos" in st.session_state and not st.session_state.df_recibidos.empty:
        df_rec_filtrado = st.session_state.df_recibidos.copy()
        df_rec_filtrado["Periodo"] = df_rec_filtrado["Fecha"].apply(lambda x: x[:7] if isinstance(x, str) else "")
        df_rec_filtrado = df_rec_filtrado[df_rec_filtrado["Periodo"] == periodo_seleccionado]
    else:
        df_rec_filtrado = pd.DataFrame()
    
    # Filtrar Emitidos
    if periodo_seleccionado and "df_emitidos" in st.session_state and not st.session_state.df_emitidos.empty:
        df_emit_filtrado = st.session_state.df_emitidos.copy()
        df_emit_filtrado["Periodo"] = df_emit_filtrado["Fecha"].apply(lambda x: x[:7] if isinstance(x, str) else "")
        df_emit_filtrado = df_emit_filtrado[df_emit_filtrado["Periodo"] == periodo_seleccionado]
    else:
        df_emit_filtrado = pd.DataFrame()
    
    with st.container():
        # Sumatorias para Recibidos
        if not df_rec_filtrado.empty:
            st.markdown("**Sumatorias para XMLs Deducibles:**")
            deducible_df = df_rec_filtrado[df_rec_filtrado["Deducible"] == True]
            if not deducible_df.empty:
                sum_deducibles = mostrar_sumatorias(deducible_df, resumen_cols)
                st.table(pd.DataFrame([sum_deducibles]))
            
            st.markdown("**Sumatorias para XMLs No Deducibles:**")
            no_deducible_df = df_rec_filtrado[df_rec_filtrado["Deducible"] == False]
            if not no_deducible_df.empty:
                sum_no_deducibles = mostrar_sumatorias(no_deducible_df, resumen_cols)
                st.table(pd.DataFrame([sum_no_deducibles]))
        else:
            st.write("No hay datos de Recibidos para el periodo seleccionado.")
        
        # Sumatorias para Emitidos
        if not df_emit_filtrado.empty:
            st.markdown("**Sumatorias para XMLs Emitidos:**")
            seleccionados_df = df_emit_filtrado[df_emit_filtrado["Seleccionar"] == True]
            if not seleccionados_df.empty:
                sum_emitidos_sel = mostrar_sumatorias(seleccionados_df, resumen_cols)
                st.table(pd.DataFrame([sum_emitidos_sel]))
            
            st.markdown("**Sumatorias para XMLs Emitidos No Seleccionados:**")
            no_seleccionados_df = df_emit_filtrado[df_emit_filtrado["Seleccionar"] == False]
            if not no_seleccionados_df.empty:
                sum_emitidos_no_sel = mostrar_sumatorias(no_seleccionados_df, resumen_cols)
                st.table(pd.DataFrame([sum_emitidos_no_sel]))
        else:
            st.write("No hay datos de Emitidos para el periodo seleccionado.")
    
    with st.container():
        # Tablas Detalladas para Recibidos
        if not df_rec_filtrado.empty:
            columnas_mostrar = [
                "Rfc Emisor", "Nombre Emisor", "Sub Total", "Descuento",
                "Total impuesto Trasladado", "Total impuesto Retenido", "Total"
            ]
            # Deducibles
            if not df_rec_filtrado[df_rec_filtrado["Deducible"] == True].empty:
                mostrar_tabla_seccion(
                    df_rec_filtrado[df_rec_filtrado["Deducible"] == True][columnas_mostrar],
                    "XMLs Deducibles"
                )
            # No Deducibles
            if not df_rec_filtrado[df_rec_filtrado["Deducible"] == False].empty:
                mostrar_tabla_seccion(
                    df_rec_filtrado[df_rec_filtrado["Deducible"] == False][columnas_mostrar],
                    "XMLs No Deducibles"
                )
        else:
            st.write("No hay datos de Recibidos para el periodo seleccionado.")
        
        # Tablas Detalladas para Emitidos
        if not df_emit_filtrado.empty:
            columnas_mostrar = [
                "Rfc Emisor", "Nombre Emisor", "Sub Total", "Descuento",
                "Total impuesto Trasladado", "Total impuesto Retenido", "Total"
            ]
            # Seleccionados
            if not df_emit_filtrado[df_emit_filtrado["Seleccionar"] == True].empty:
                mostrar_tabla_seccion(
                    df_emit_filtrado[df_emit_filtrado["Seleccionar"] == True][columnas_mostrar],
                    "CFDIs Emitidos"
                )
            # No Seleccionados
            if not df_emit_filtrado[df_emit_filtrado["Seleccionar"] == False].empty:
                mostrar_tabla_seccion(
                    df_emit_filtrado[df_emit_filtrado["Seleccionar"] == False][columnas_mostrar],
                    "CFDIs Emitidos No Seleccionados"
                )
        else:
            st.write("No hay datos de Emitidos para el periodo seleccionado.")

###############################################################################
#                 SECCIÓN UNIFICADA DE AVANCE                                  #
###############################################################################
def section_avance():
    st.header("📁 Gestión de Avance")
    
    avance_tabs = st.tabs(["💾 Guardar Avance", "📥 Cargar Avance"])
    
    with avance_tabs[0]:
        st.subheader("💾 Guardar Avance")
        if st.button("Guardar Avance"):
            guardar_avance()
    
    with avance_tabs[1]:
        st.subheader("📥 Cargar Avance")
        uploaded_file = st.file_uploader("📂 Cargar archivo Excel con avances", type=["xlsx"], key="cargar_avance_tab")
        if uploaded_file is not None:
            try:
                df_recibidos, df_emitidos = cargar_progreso(uploaded_file)
                
                # Procesar CFDIs Recibidos
                if not df_recibidos.empty:
                    # Filtrar duplicados
                    df_recibidos = filtrar_duplicados_por_uuid(df_recibidos, st.session_state.df_recibidos, "UUID")
                    # Corregir datos no numéricos
                    df_recibidos = detectar_y_corregir_valores(df_recibidos, resumen_cols)
                    st.session_state.df_recibidos = pd.concat([st.session_state.df_recibidos, df_recibidos], ignore_index=True)
                
                # Procesar CFDIs Emitidos
                if not df_emitidos.empty:
                    # Filtrar duplicados
                    df_emitidos = filtrar_duplicados_por_uuid(df_emitidos, st.session_state.df_emitidos, "UUID")
                    # Corregir datos no numéricos
                    df_emitidos = detectar_y_corregir_valores(df_emitidos, resumen_cols)
                    st.session_state.df_emitidos = pd.concat([st.session_state.df_emitidos, df_emitidos], ignore_index=True)
                
                st.success("✅ Avance cargado exitosamente.")
            except Exception as e:
                st.error(f"❌ Error al cargar el archivo: {e}")

###############################################################################
#     FUNCIÓN PARA CARGAR PROGRESO DESDE UN ARCHIVO EXCEL                     #
###############################################################################
def cargar_progreso(file):
    """
    Carga datos desde un archivo Excel con sheets:
    - Recibidos
    - Emitidos
    y los retorna como df_recibidos, df_emitidos
    """
    try:
        df_recibidos = pd.read_excel(file, sheet_name="Recibidos")
        df_emitidos = pd.read_excel(file, sheet_name="Emitidos")
        return df_recibidos, df_emitidos
    except Exception as e:
        st.error(f"Error al cargar el archivo: {e}")
        return pd.DataFrame(), pd.DataFrame()

###############################################################################
#     FUNCIÓN PARA GUARDAR AVANCE EN UN ARCHIVO EXCEL                        #
###############################################################################
def guardar_avance():
    """
    Guarda en un archivo Excel (con múltiples sheets) los datos actuales
    de df_recibidos y df_emitidos, corrigiendo antes los valores no numéricos.
    """
    st.header("Guardar Avance")
    if st.button("💾 Guardar Avance"):
        if "df_recibidos" in st.session_state and "df_emitidos" in st.session_state:
            df_recibidos = st.session_state.df_recibidos.copy()
            df_emitidos = st.session_state.df_emitidos.copy()
            
            # Corregimos valores no numéricos antes de guardar
            df_recibidos = detectar_y_corregir_valores(df_recibidos, resumen_cols)
            df_emitidos = detectar_y_corregir_valores(df_emitidos, resumen_cols)
            
            # Exportamos a Excel
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                df_recibidos.to_excel(writer, sheet_name="Recibidos", index=False)
                df_emitidos.to_excel(writer, sheet_name="Emitidos", index=False)
            output.seek(0)
            st.download_button(
                label="📥 Descargar archivo de avance",
                data=output,
                file_name="avance_cfds.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            st.warning("No hay datos disponibles para guardar. Verifica que estén cargados los CFDIs Emitidos y Recibidos.")

###############################################################################
#                             MAIN APP                                        #
###############################################################################
st.title("Procesador de XMLs desde ZIP - CFDI 4.0")

# Opciones de Avance en la barra lateral
with st.sidebar.expander("📁 Guardar o Cargar Avance"):
    section_avance()

# Selección de sección
seccion = st.sidebar.radio("Tipo de CFDIS", ["Recibidos", "Emitidos", "Resumen"], key="seccion")

# Exportar todos los datos (Recibidos y Emitidos) con corrección
with st.sidebar.expander("📤 Exportar Todos los Datos"):
    form_exp = st.radio("Formato de Exportación", ["Excel","CSV"], key="formato_export_todos")
    if st.button("Exportar Todos los Datos"):
        if "df_recibidos" in st.session_state and not st.session_state.df_recibidos.empty and \
           "df_emitidos" in st.session_state and not st.session_state.df_emitidos.empty:
            df_recibidos = st.session_state.df_recibidos.copy()
            df_emitidos = st.session_state.df_emitidos.copy()
            
            # Se corrigen valores antes de exportar
            export_data, export_file_name, export_mime = exportar_datos(df_recibidos, df_emitidos, form_exp)
            st.download_button(
                label=f"📥 Descargar {form_exp}",
                data=export_data,
                file_name=export_file_name,
                mime=export_mime
            )
        else:
            st.warning("No hay datos suficientes para exportar. Verifica que estén cargados los CFDIs Emitidos y Recibidos.")

# Navegación de secciones
if seccion == "Recibidos":
    section_recibidos()
elif seccion == "Emitidos":
    section_emitidos()
elif seccion == "Resumen":
    section_resumen()
