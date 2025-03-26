import tkinter as tk
from tkinter import filedialog, messagebox
import pandas as pd
import numpy as np

def cargar_archivo():
    filepath = filedialog.askopenfilename(
        title="Seleccionar archivo Excel",
        filetypes=[("Archivos Excel", "*.xlsx")]
    )
    if filepath:
        try:
            global df, current_index
            df = pd.read_excel(filepath, dtype=str).replace({np.nan: ""})
            df["Seleccionado"] = False
            current_index = 0
            mostrar_factura()
        except Exception as e:
            messagebox.showerror("Error", f"No se pudo cargar el archivo:\n{e}")

def mostrar_factura():
    global current_index
    if df.empty:
        messagebox.showwarning("Advertencia", "No hay datos para mostrar.")
        return

    if current_index < 0:
        current_index = 0
    elif current_index >= len(df):
        current_index = len(df) - 1

    factura_data = df.iloc[current_index]

    campos_prioritarios = [
        f"**Nombre Emisor**: {factura_data['Nombre Emisor']}",
        f"**Uso CFDI Receptor**: {factura_data['Uso Cfdi Receptor']}",
        f"**Sub Total**: {factura_data['Sub Total']}",
        f"**Descuento**: {factura_data['Descuento']}",
        f"**Total Impuesto Trasladado**: {factura_data['Total impuesto Trasladado']}",
        f"**Total**: {factura_data['Total']}",
        f"**Método de Pago**: {factura_data['Método de Pago']}",
        f"**Forma de Pago**: {factura_data['Forma de Pago']}",
    ]

    concepto_texto = ""
    if "Concepto" in factura_data and factura_data["Concepto"]:
        concepto_texto = "**Concepto**: " + "\n".join([
            f"{line.strip()}" for line in str(factura_data["Concepto"]).split('\n')
        ])

    otros_campos = [
        f"{col}: {factura_data[col]}" for col in factura_data.index if col not in [
            "Nombre Emisor", "Uso Cfdi Receptor", "Sub Total", "Descuento", 
            "Total impuesto Trasladado", "Total", "Método de Pago", "Forma de Pago", "Concepto"
        ]
    ]

    texto_factura.set(f"Factura {current_index + 1} de {len(df)}\n" + "\n".join(
        campos_prioritarios + [concepto_texto] + otros_campos
    ))
    checkbox_var.set(bool(factura_data["Seleccionado"]))

def marcar_actual():
    global current_index
    if df.empty:
        return
    df.at[current_index, "Seleccionado"] = True
    mostrar_factura()

def desmarcar_actual():
    global current_index
    if df.empty:
        return
    df.at[current_index, "Seleccionado"] = False
    mostrar_factura()

def actualizar_seleccion():
    global current_index
    if df.empty:
        return
    df.at[current_index, "Seleccionado"] = checkbox_var.get()

def siguiente_factura():
    global current_index
    current_index += 1
    mostrar_factura()

def anterior_factura():
    global current_index
    current_index -= 1
    mostrar_factura()

def exportar_archivo():
    seleccionados = df[df["Seleccionado"]]
    no_seleccionados = df[~df["Seleccionado"]]
    if seleccionados.empty:
        messagebox.showwarning("Advertencia", "No hay facturas seleccionadas para exportar.")
        return

    filepath = filedialog.asksaveasfilename(
        title="Guardar archivo Excel",
        defaultextension=".xlsx",
        filetypes=[("Archivos Excel", "*.xlsx")]
    )
    if filepath:
        try:
            # Convertir columnas numéricas a su tipo correcto antes de exportar
            columnas_a_convertir = [
                "Sub Total", "Descuento", "Total impuesto Trasladado", "Total"
            ]
            for columna in columnas_a_convertir:
                if columna in seleccionados.columns:
                    seleccionados[columna] = pd.to_numeric(seleccionados[columna], errors="coerce")
                if columna in no_seleccionados.columns:
                    no_seleccionados[columna] = pd.to_numeric(no_seleccionados[columna], errors="coerce")

            with pd.ExcelWriter(filepath, engine='xlsxwriter') as writer:
                seleccionados.to_excel(writer, sheet_name="Deducibles", index=False)
                no_seleccionados.to_excel(writer, sheet_name="No Deducibles", index=False)

            messagebox.showinfo("Éxito", "Archivo exportado correctamente.")
        except Exception as e:
            messagebox.showerror("Error", f"No se pudo guardar el archivo:\n{e}")

# Crear ventana principal
root = tk.Tk()
root.title("Gestión de XML para Declaración Mensual")

# Crear widgets
frame_botones = tk.Frame(root)
frame_botones.pack(pady=10)

btn_cargar = tk.Button(frame_botones, text="Cargar Archivo Excel", command=cargar_archivo)
btn_cargar.pack(side=tk.LEFT, padx=5)

btn_anterior = tk.Button(frame_botones, text="Anterior", command=anterior_factura)
btn_anterior.pack(side=tk.LEFT, padx=5)

btn_siguiente = tk.Button(frame_botones, text="Siguiente", command=siguiente_factura)
btn_siguiente.pack(side=tk.LEFT, padx=5)

btn_marcar = tk.Button(frame_botones, text="Marcar", command=marcar_actual)
btn_marcar.pack(side=tk.LEFT, padx=5)

btn_desmarcar = tk.Button(frame_botones, text="Desmarcar", command=desmarcar_actual)
btn_desmarcar.pack(side=tk.LEFT, padx=5)

btn_exportar = tk.Button(frame_botones, text="Exportar Seleccionados", command=exportar_archivo)
btn_exportar.pack(side=tk.LEFT, padx=5)

texto_factura = tk.StringVar()
label_factura = tk.Label(root, textvariable=texto_factura, justify=tk.LEFT, anchor="w")
label_factura.pack(fill=tk.BOTH, padx=10, pady=10)

checkbox_var = tk.BooleanVar()
checkbox = tk.Checkbutton(root, text="Seleccionado", variable=checkbox_var, command=actualizar_seleccion)
checkbox.pack(pady=5)

# Variables globales
df = pd.DataFrame()
current_index = 0

root.mainloop()
