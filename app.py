import zipfile
import io
import xml.etree.ElementTree as ET
import pandas as pd
from st_aggrid import AgGrid, GridOptionsBuilder, DataReturnMode, GridUpdateMode

st.title("Procesador de XMLs desde ZIP - CFDI 4.0")

# Diccionarios para mapeos
codigo_map_forma_pago = {
    "01": "Efectivo",
    "02": "Cheque Nominativo",
    "03": "Transferencia Electrónica de Fondos SPEI",
    "04": "Tarjeta de Crédito",
    "05": "Monedero Electrónico",
    "06": "Dinero Electrónico",
    "8":  "Vales de Despensa",
    "12": "Dación en Pago",
    "13": "Pago por Subrogación",
    "14": "Pago por Consignación",
    "15": "Condonación",
    "17": "Compensación",
    "23": "Novación",
    "24": "Confusión",
    "25": "Remisión de Deuda",
    "26": "Prescripción o Caducidad",
    "27": "A Satisfacción del Acreedor",
    "28": "Tarjeta de Débito",
    "29": "Tarjeta de Servicios",
    "30": "Aplicación de Anticipos",
    "31": "Intermediario Pagos",
    "99": "Por Definir",
}

codigo_map_uso_cfdi = {
    "G01": "Adquisición de mercancías",
    "G02": "Devoluciones, descuentos o bonificaciones",
    "G03": "Gastos en general",
    "I01": "Construcciones",
    "I02": "Mobiliario y equipo de oficina por inversiones",
    "I03": "Equipo de transporte",
    "I04": "Equipo de computo y accesorios",
    "I05": "Dados, troqueles, moldes, matrices y herramental",
    "I06": "Comunicaciones telefónicas",
    "I07": "Comunicaciones satelitales",
    "I08": "Otra maquinaria y equipo",
    "D01": "Honorarios médicos, dentales y gastos hospitalarios",
    "D02": "Gastos médicos por incapacidad o discapacidad",
    "D03": "Gastos funerarios",
    "D04": "Donativos",
    "D05": "Intereses reales efectivamente pagados por créditos hipotecarios (casa habitación)",
    "D06": "Aportaciones voluntarias al SAR",
    "D07": "Primas por seguros de gastos médicos",
    "D08": "Gastos de transportación escolar obligatoria",
    "D09": "Depósitos en cuentas para el ahorro, primas que tengan como base planes de pensiones",
    "D10": "Pagos por servicios educativos (colegiaturas)",
    "S01": "Sin efectos fiscales",
    "CP01": "Pagos",
    "CN01": "Nómina",
}

def procesar_zip(uploaded_file):
    zip_bytes = uploaded_file.read()
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as thezip:
        rows = []
        ns = {
            'cfdi': 'http://www.sat.gob.mx/cfd/4',
            'tfd': 'http://www.sat.gob.mx/TimbreFiscalDigital'
        }
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

                    comprobante = root
                    row["Fecha"] = comprobante.attrib.get("Fecha", "")
                    row["Sub Total"] = comprobante.attrib.get("SubTotal", "")
                    row["Descuento"] = comprobante.attrib.get("Descuento", "")
                    row["Total"] = comprobante.attrib.get("Total", "")
                    row["Método de Pago"] = comprobante.attrib.get("MetodoPago", "")

                    forma_pago_codigo = comprobante.attrib.get("FormaPago", "")
                    forma_pago_desc = codigo_map_forma_pago.get(forma_pago_codigo, "")
                    if forma_pago_desc:
                        row["Forma de Pago"] = f"{forma_pago_codigo}-{forma_pago_desc}"
                    else:
                        row["Forma de Pago"] = forma_pago_codigo

                    row["Moneda"] = comprobante.attrib.get("Moneda", "")
                    row["Tipo de Cambio"] = comprobante.attrib.get("TipoCambio", "")
                    row["Versión"] = comprobante.attrib.get("Version", "")
                    row["Serie"] = comprobante.attrib.get("Serie", "")
                    row["Folio"] = comprobante.attrib.get("Folio", "")
                    row["Tipo"] = comprobante.attrib.get("TipoDeComprobante", "")

                    emisor = comprobante.find("cfdi:Emisor", namespaces=ns)
                    if emisor is not None:
                        row["Rfc Emisor"] = emisor.attrib.get("Rfc", "")
                        row["Nombre Emisor"] = emisor.attrib.get("Nombre", "")
                        row["Régimen Fiscal Emisor"] = emisor.attrib.get("RegimenFiscal", "")

                    receptor = comprobante.find("cfdi:Receptor", namespaces=ns)
                    if receptor is not None:
                        row["Rfc Receptor"] = receptor.attrib.get("Rfc", "")
                        row["Nombre Receptor"] = receptor.attrib.get("Nombre", "")
                        row["CP Receptor"] = receptor.attrib.get("DomicilioFiscalReceptor", "")
                        row["Régimen Receptor"] = receptor.attrib.get("RegimenFiscalReceptor", "")
                        uso_cfdi_codigo = receptor.attrib.get("UsoCFDI", "")
                        uso_cfdi_desc = codigo_map_uso_cfdi.get(uso_cfdi_codigo, "")
                        if uso_cfdi_desc:
                            row["Uso Cfdi Receptor"] = f"{uso_cfdi_codigo}-{uso_cfdi_desc}"
                        else:
                            row["Uso Cfdi Receptor"] = uso_cfdi_codigo

                    timbre = comprobante.find(".//tfd:TimbreFiscalDigital", namespaces=ns)
                    if timbre is not None:
                        row["UUID"] = timbre.attrib.get("UUID", "")

                    total_trasladado = 0.0
                    impuestos_nombres = set()
                    traslado_iva_016 = ""
                    for traslado in comprobante.findall(".//cfdi:Traslado", namespaces=ns):
                        importe = traslado.attrib.get("Importe", "0")
                        try:
                            total_trasladado += float(importe)
                        except (ValueError, TypeError):
                            pass
                        imp = traslado.attrib.get("Impuesto", "")
                        if imp:
                            impuestos_nombres.add(imp)
                        if traslado.attrib.get("TasaOCuota") == "0.160000" and traslado.attrib.get("Impuesto") == "002":
                            traslado_iva_016 = importe

                    total_retenido = 0.0
                    for retencion in comprobante.findall(".//cfdi:Retencion", namespaces=ns):
                        importe = retencion.attrib.get("Importe", "0")
                        try:
                            total_retenido += float(importe)
                        except (ValueError, TypeError):
                            pass

                    row["Total impuesto Trasladado"] = total_trasladado
                    row["Nombre Impuesto"] = ", ".join(impuestos_nombres)
                    row["Total impuesto Retenido"] = total_retenido
                    row["Traslado IVA 0.160000 %"] = traslado_iva_016

                    conceptos = comprobante.findall("cfdi:Conceptos/cfdi:Concepto", namespaces=ns)
                    lista_conceptos = []
                    for concepto in conceptos:
                        desc = concepto.attrib.get("Descripcion", "")
                        importe = concepto.attrib.get("Importe", "")
                        lista_conceptos.append(f"{desc}: {importe}")
                    row["Conceptos"] = "; ".join(lista_conceptos)

                    rows.append(row)
    return rows

def mostrar_sumatorias(df, columnas_sumar):
    sumas = {}
    for col in columnas_sumar:
        sumas[col] = pd.to_numeric(df[col], errors='coerce').sum()
    return sumas

def mostrar_tabla_seccion(df, titulo, ancho=2000):
    st.subheader(titulo)
    if df.empty:
        st.write(f"No hay datos para {titulo.lower()}.")
    else:
        st.dataframe(df, width=ancho)

# Configuración de navegación
seccion = st.sidebar.radio("Seleccione Sección", ["Recibidos", "Emitidos", "Resumen"])

resumen_cols = [
    "Sub Total", "Descuento", "Total impuesto Trasladado",
    "Total impuesto Retenido", "Total", "Traslado IVA 0.160000 %"
]

if seccion == "Recibidos":
    st.header("CFDIs Recibidos")
    uploaded_file_recibidos = st.file_uploader("Cargar archivo ZIP con XMLs Recibidos", type=["zip"], key="recibidos_file")
    if uploaded_file_recibidos is not None and "df_recibidos" not in st.session_state:
        rows = procesar_zip(uploaded_file_recibidos)
        if rows:
            st.session_state.df_recibidos = pd.DataFrame(rows)
            st.session_state.df_recibidos["Deducible"] = True
        else:
            st.info("No se encontraron archivos XML en el ZIP de Recibidos.")

    if "df_recibidos" in st.session_state:
        columnas_sumar = resumen_cols
        st.sidebar.header("Filtros Recibidos")
        emisores = ["Todos"] + sorted(
            st.session_state.df_recibidos.apply(lambda row: f"{row['Rfc Emisor']} - {row['Nombre Emisor']}", axis=1).unique().tolist()
        )
        emisores_seleccionado = st.sidebar.selectbox("Filtrar por Emisor (RFC - Nombre)", options=emisores, key="filtro_recibidos_emisor")
        uso_cfdi_opciones = ["Todos"] + sorted(st.session_state.df_recibidos["Uso Cfdi Receptor"].unique().tolist())
        uso_cfdi_seleccionado = st.sidebar.selectbox("Filtrar por Uso CFDI Receptor", options=uso_cfdi_opciones, key="filtro_recibidos_uso")
        forma_pago_opciones = ["Todos"] + sorted(st.session_state.df_recibidos["Forma de Pago"].unique().tolist())
        forma_pago_seleccionado = st.sidebar.selectbox("Filtrar por Forma de Pago", options=forma_pago_opciones, key="filtro_recibidos_forma")

        col_sel, col_desel = st.columns(2)
        if col_sel.button("Seleccionar todos", key="sel_all_recibidos"):
            st.session_state.df_recibidos["Deducible"] = True
        if col_desel.button("Deseleccionar todos", key="desel_all_recibidos"):
            st.session_state.df_recibidos["Deducible"] = False

        filtered_df = st.session_state.df_recibidos.copy()
        if emisores_seleccionado != "Todos":
            rfc_emisor_filtrar = emisores_seleccionado.split(' - ')[0]
            filtered_df = filtered_df[filtered_df["Rfc Emisor"] == rfc_emisor_filtrar]
        if uso_cfdi_seleccionado != "Todos":
            filtered_df = filtered_df[filtered_df["Uso Cfdi Receptor"] == uso_cfdi_seleccionado]
        if forma_pago_seleccionado != "Todos":
            filtered_df = filtered_df[filtered_df["Forma de Pago"] == forma_pago_seleccionado]

        gb = GridOptionsBuilder.from_dataframe(filtered_df)
        gb.configure_column("Deducible", editable=True, cellEditor='agCheckboxCellEditor', pinned=True)
        gb.configure_default_column(editable=True, resizable=True)
        gridOptions = gb.build()

        grid_response = AgGrid(
            filtered_df,
            gridOptions=gridOptions,
            data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
            update_mode=GridUpdateMode.MODEL_CHANGED,
            height=600,
            width=2000,
            reload_data=True
        )

        edited_df = pd.DataFrame(grid_response['data'])
        for _, row in edited_df.iterrows():
            identifier = row["XML"]
            st.session_state.df_recibidos.loc[st.session_state.df_recibidos["XML"] == identifier, "Deducible"] = row["Deducible"]

        tabs_recibidos = st.tabs(["Deducibles", "No Deducibles"])

        with tabs_recibidos[0]:
            deducible_df = st.session_state.df_recibidos[st.session_state.df_recibidos["Deducible"] == True]
            mostrar_tabla_seccion(deducible_df, "XMLs Deducibles")
            st.markdown("**Sumatorias para XMLs Deducibles:**")
            st.table(pd.DataFrame([mostrar_sumatorias(deducible_df, columnas_sumar)]))

        with tabs_recibidos[1]:
            no_deducible_df = st.session_state.df_recibidos[st.session_state.df_recibidos["Deducible"] == False]
            mostrar_tabla_seccion(no_deducible_df, "XMLs No Deducibles")
            st.markdown("**Sumatorias para XMLs No Deducibles:**")
            st.table(pd.DataFrame([mostrar_sumatorias(no_deducible_df, columnas_sumar)]))

elif seccion == "Emitidos":
    st.header("CFDIs Emitidos")
    uploaded_file_emitidos = st.file_uploader("Cargar archivo ZIP con XMLs Emitidos", type=["zip"], key="emitidos_file")
    if uploaded_file_emitidos is not None and "df_emitidos" not in st.session_state:
        rows = procesar_zip(uploaded_file_emitidos)
        if rows:
            st.session_state.df_emitidos = pd.DataFrame(rows)
            st.session_state.df_emitidos["Seleccionar"] = True
        else:
            st.info("No se encontraron archivos XML en el ZIP de Emitidos.")

    if "df_emitidos" in st.session_state:
        columnas_sumar = resumen_cols
        st.sidebar.header("Filtros Emitidos")
        emisores_e = ["Todos"] + sorted(
            st.session_state.df_emitidos.apply(lambda row: f"{row['Rfc Emisor']} - {row['Nombre Emisor']}", axis=1).unique().tolist()
        )
        emisores_seleccionado_e = st.sidebar.selectbox("Filtrar por Emisor (RFC - Nombre)", options=emisores_e, key="filtro_emitidos_emisor")
        uso_cfdi_opciones_e = ["Todos"] + sorted(st.session_state.df_emitidos["Uso Cfdi Receptor"].unique().tolist())
        uso_cfdi_seleccionado_e = st.sidebar.selectbox("Filtrar por Uso CFDI Receptor", options=uso_cfdi_opciones_e, key="filtro_emitidos_uso")
        forma_pago_opciones_e = ["Todos"] + sorted(st.session_state.df_emitidos["Forma de Pago"].unique().tolist())
        forma_pago_seleccionado_e = st.sidebar.selectbox("Filtrar por Forma de Pago", options=forma_pago_opciones_e, key="filtro_emitidos_forma")

        col_sel_e, col_desel_e = st.columns(2)
        if col_sel_e.button("Seleccionar todos", key="sel_all_emitidos"):
            st.session_state.df_emitidos["Seleccionar"] = True
        if col_desel_e.button("Deseleccionar todos", key="desel_all_emitidos"):
            st.session_state.df_emitidos["Seleccionar"] = False

        filtered_df_e = st.session_state.df_emitidos.copy()
        if emisores_seleccionado_e != "Todos":
            rfc_emisor_filtrar_e = emisores_seleccionado_e.split(' - ')[0]
            filtered_df_e = filtered_df_e[filtered_df_e["Rfc Emisor"] == rfc_emisor_filtrar_e]
        if uso_cfdi_seleccionado_e != "Todos":
            filtered_df_e = filtered_df_e[filtered_df_e["Uso Cfdi Receptor"] == uso_cfdi_seleccionado_e]
        if forma_pago_seleccionado_e != "Todos":
            filtered_df_e = filtered_df_e[filtered_df_e["Forma de Pago"] == forma_pago_seleccionado_e]

        gb_e = GridOptionsBuilder.from_dataframe(filtered_df_e)
        gb_e.configure_column("Seleccionar", editable=True, cellEditor='agCheckboxCellEditor', pinned=True)
        gb_e.configure_default_column(editable=True, resizable=True)
        gridOptions_e = gb_e.build()

        grid_response_e = AgGrid(
            filtered_df_e,
            gridOptions=gridOptions_e,
            data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
            update_mode=GridUpdateMode.MODEL_CHANGED,
            height=600,
            width=2000,
            reload_data=True
        )

        edited_df_e = pd.DataFrame(grid_response_e['data'])
        for _, row in edited_df_e.iterrows():
            identifier = row["XML"]
            st.session_state.df_emitidos.loc[st.session_state.df_emitidos["XML"] == identifier, "Seleccionar"] = row["Seleccionar"]

        tabs_emitidos = st.tabs(["CFDIs Seleccionados", "CFDIs No Seleccionados"])

        with tabs_emitidos[0]:
            seleccionados_df = st.session_state.df_emitidos[st.session_state.df_emitidos["Seleccionar"] == True]
            mostrar_tabla_seccion(seleccionados_df, "CFDIs Seleccionados")
            st.markdown("**Sumatorias para CFDIs Seleccionados:**")
            st.table(pd.DataFrame([mostrar_sumatorias(seleccionados_df, columnas_sumar)]))

        with tabs_emitidos[1]:
            no_seleccionados_df = st.session_state.df_emitidos[st.session_state.df_emitidos["Seleccionar"] == False]
            mostrar_tabla_seccion(no_seleccionados_df, "CFDIs No Seleccionados")
            st.markdown("**Sumatorias para CFDIs No Seleccionados:**")
            st.table(pd.DataFrame([mostrar_sumatorias(no_seleccionados_df, columnas_sumar)]))

elif seccion == "Resumen":
    st.header("Resumen")
    
    # Se definen las columnas numéricas para sumar
    resumen_cols = [
        "Sub Total", "Descuento", "Total impuesto Trasladado",
        "Total impuesto Retenido", "Total", "Traslado IVA 0.160000 %"
    ]
    
    # Resumen Recibidos con agrupación por RFC, Nombre Emisor y Conceptos
    if "df_recibidos" in st.session_state:
        df_rec = st.session_state.df_recibidos
        if not df_rec.empty:
            st.subheader("Resumen Recibidos")
            # Calcular y mostrar la sumatoria global para Recibidos
            global_sumas_rec = {}
            for col in resumen_cols:
                global_sumas_rec[col] = pd.to_numeric(df_rec[col], errors='coerce').sum()
            st.markdown("**Sumatoria Global de Recibidos:**")
            st.table(pd.DataFrame([global_sumas_rec]))
            
            # Agrupar por RFC, Nombre Emisor y Conceptos y sumar valores numéricos
            resumen_rec = df_rec.groupby(["Rfc Emisor", "Nombre Emisor", "Conceptos"])[resumen_cols].sum().reset_index()
            st.dataframe(resumen_rec)
        else:
            st.write("No hay datos de Recibidos.")
    else:
        st.write("No hay datos de Recibidos.")
    
    # Resumen Emitidos con agrupación por RFC, Nombre Emisor y Conceptos
    if "df_emitidos" in st.session_state:
        df_emit = st.session_state.df_emitidos
        if not df_emit.empty:
            st.subheader("Resumen Emitidos")
            # Calcular y mostrar la sumatoria global para Emitidos
            global_sumas_emit = {}
            for col in resumen_cols:
                global_sumas_emit[col] = pd.to_numeric(df_emit[col], errors='coerce').sum()
            st.markdown("**Sumatoria Global de Emitidos:**")
            st.table(pd.DataFrame([global_sumas_emit]))
            
            # Agrupar por RFC, Nombre Emisor y Conceptos y sumar valores numéricos
            resumen_emit = df_emit.groupby(["Rfc Emisor", "Nombre Emisor", "Conceptos"])[resumen_cols].sum().reset_index()
            st.dataframe(resumen_emit)
