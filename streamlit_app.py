import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import tempfile
import os
import plotly.graph_objects as go
from fpdf import FPDF

# ==========================================
# 1. CONFIGURACIÓN Y ESTILOS
# ==========================================
st.set_page_config(page_title="Control de Golpes de Matrices", layout="wide", page_icon="⚙️")

st.markdown("""
<style>
    .header-style { font-size: 26px; font-weight: bold; margin-bottom: 5px; color: #1F2937; text-align: center; }
</style>
""", unsafe_allow_html=True)

st.markdown('<div class="header-style">⚙️ Reporte Auxiliar: Control de Golpes de Matrices (FAMMA)</div>', unsafe_allow_html=True)
st.write("<p style='text-align: center;'>Cruce automático de Catálogo, Producción y Mantenimiento.</p>", unsafe_allow_html=True)
st.divider()

# ==========================================
# 2. ENLACES DE GOOGLE SHEETS
# ==========================================
URL_CATALOGO = "https://docs.google.com/spreadsheets/d/1feaeFLl2UslCsO4mzldUVFuhY1bdnUiQPatRM2m0sW0/export?format=csv&gid=1862158700"
URL_PRODUCCION = "https://docs.google.com/spreadsheets/d/1c4aEFtCS-sJZFcH6iLb8AdBVsPrz0pNWayHR2-Dhfm8/export?format=csv&gid=315437448"
URL_PREV_FAMMA = "https://docs.google.com/spreadsheets/d/1MptnOuRfyOAr1EgzNJVygTtNziOSdzXJn-PZDX0pNzc/export?format=csv&gid=324842888"
URL_CORR_FAMMA = "https://docs.google.com/spreadsheets/d/1A-0mngZdgvZGbqzWjA_awhrwfvca0K4aGqp5NBAoFAY/export?format=csv&gid=238711679"

VALID_PIEZA_COLS = [
    'PIEZAS RENAULT', 'PIEZAS FAURECIA', 'PIEZAS FIAT', 'PIEZAS DENSO', 
    'PIEZAS PEUGEOT', 'PIEZA FIAT', 'PIEZA NISSAN', 'PIEZA RENAULT', 'NUMERO DE PIEZA'
]

# ==========================================
# 3. FUNCIONES DE LIMPIEZA Y CARGA
# ==========================================
def clean_str(val):
    if pd.isna(val): return ""
    v = str(val).strip().upper()
    if v.endswith('.0'): v = v[:-2]
    return v

def get_match_key(pieza_str):
    pieza_str = str(pieza_str).strip()
    p = pieza_str.split('/')[0].strip()
    if ' - ' in p:
        p = p.split(' - ')[0].strip()
    elif '-' in p:
        p = p.split('-')[0].strip()
    return p

def extract_mantenimientos(url, tipo_mant):
    try:
        df = pd.read_csv(url)
        cols = [str(c).upper().strip() for c in df.columns]
        
        # 1. Buscar la columna de fecha
        col_fecha = next((i for i, c in enumerate(cols) if 'FECHA' in c or 'MARCA TEMPORAL' in c), None)
        if col_fecha is None: 
            return pd.DataFrame()

        # 2. Buscar TODAS las columnas que indiquen si terminó (Aplica a PREV y CORR)
        terminos_clave = ['TERMINO', 'PREVENTIVO?', 'TERMINADO', 'CORRECTIVO']
        cols_terminado_idx = [i for i, c in enumerate(cols) if any(t in c for t in terminos_clave)]
        
        registros = []
        for _, row in df.iterrows():
            fecha = pd.to_datetime(row.iloc[col_fecha], dayfirst=True, errors='coerce')
            if pd.isna(fecha): continue
            
            # 3. Revisar en todas las columnas de 'Terminado'
            estado_terminado = 'NO'
            if not cols_terminado_idx: 
                 estado_terminado = 'SI'
            else:
                for idx in cols_terminado_idx:
                    val = str(row.iloc[idx]).strip().upper()
                    if val == 'SI' or val == 'SÍ':
                        estado_terminado = 'SI'
                        break
                
            # 4. Extraer pieza y OP
            for i, col_name in enumerate(cols):
                base_col = col_name.split('.')[0].strip()
                if base_col in VALID_PIEZA_COLS:
                    pieza_completa = clean_str(row.iloc[i])
                    if pieza_completa and pieza_completa not in ['NAN', 'NONE', '-', '0', 'N/A', 'NO APLICA', '']:
                        pieza_match = get_match_key(pieza_completa)
                        op = ""
                        for j in range(i+1, min(i+4, len(cols))):
                            next_col = cols[j].split('.')[0].strip()
                            if 'OPERACION' in next_col or 'OPERACIÓN' in next_col or 'OP' == next_col:
                                op = clean_str(row.iloc[j])
                                break
                        registros.append({
                            'Fecha': fecha, 
                            'Pieza_Match': pieza_match, 
                            'OP': op, 
                            'Tipo_Mant': tipo_mant, 
                            'Terminado': estado_terminado
                        })
        return pd.DataFrame(registros)
    except Exception as e:
        st.error(f"❌ Error al extraer datos de {tipo_mant}: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_all_data():
    df_cat = pd.read_csv(URL_CATALOGO)
    df_cat.columns = df_cat.columns.astype(str).str.replace('\n', ' ').str.replace('\r', '').str.strip()
    df_cat.columns = df_cat.columns.str.replace(r'\s+', ' ', regex=True)
    col_activo = next((c for c in df_cat.columns if 'ACTIVO' in c.upper()), None)
    if col_activo:
        df_cat = df_cat[df_cat[col_activo].astype(str).str.strip().str.upper() == 'SI']
    
    df_prod = pd.read_csv(URL_PRODUCCION)
    df_prod.columns = df_prod.columns.astype(str).str.strip()
    col_fecha_prod = next((c for c in df_prod.columns if 'fecha' in c.lower() and 'inicio' not in c.lower()), None)
    df_prod['Fecha'] = pd.to_datetime(df_prod[col_fecha_prod], dayfirst=True, errors='coerce') if col_fecha_prod else pd.NaT
    
    col_buenas = next((c for c in df_prod.columns if 'buenas' in c.lower()), None)
    col_retrabajo = next((c for c in df_prod.columns if 'retrabajo' in c.lower()), None)
    df_prod['Buenas_Num'] = pd.to_numeric(df_prod[col_buenas].astype(str).str.replace('.', '').str.replace(',', '.'), errors='coerce').fillna(0) if col_buenas else 0
    df_prod['Retrabajo_Num'] = pd.to_numeric(df_prod[col_retrabajo].astype(str).str.replace('.', '').str.replace(',', '.'), errors='coerce').fillna(0) if col_retrabajo else 0
    df_prod['Golpes_Totales'] = df_prod['Buenas_Num'] + df_prod['Retrabajo_Num']
    
    col_pieza_prod = next((c for c in df_prod.columns if 'código producto' in c.lower() or 'codigo producto' in c.lower() or 'pieza' in c.lower()), None)
    df_prod['Pieza_Match'] = df_prod[col_pieza_prod].apply(lambda x: get_match_key(clean_str(x))) if col_pieza_prod else "" 
    
    df_prev = extract_mantenimientos(URL_PREV_FAMMA, "PREV")
    df_corr = extract_mantenimientos(URL_CORR_FAMMA, "CORR")
    return df_cat, df_prod, pd.concat([df_prev, df_corr], ignore_index=True)

# ==========================================
# 4. MOTOR DE CRUCE Y CÁLCULO
# ==========================================
def procesar_estado_matrices(df_cat, df_prod, df_mant):
    resultados = []
    abiertos = []
    col_pieza = next((c for c in df_cat.columns if c.upper() == 'PIEZA'), 'PIEZA')
    col_op = next((c for c in df_cat.columns if c.upper() == 'OP'), 'OP')
    col_cliente = next((c for c in df_cat.columns if 'CLIENTE' in c.upper()), 'CLIENTE')
    col_tipo = next((c for c in df_cat.columns if 'TIPO' in c.upper()), 'TIPO')
    col_limite = next((c for c in df_cat.columns if 'GOLPES PARA MANTENIMIENTO' in c.upper()), 'GOLPES PARA MANTENIMIENTO')
    col_alerta = next((c for c in df_cat.columns if 'ALERTA' in c.upper()), 'ALERTA')
    col_prev = next((c for c in df_cat.columns if 'ULTIMO PREVENTIVO' in c.upper()), 'ULTIMO PREVENTIVO')
    col_corr = next((c for c in df_cat.columns if 'ULTIMO CORRECTIVO' in c.upper()), 'ULTIMO CORRECTIVO')

    for _, row in df_cat.iterrows():
        pieza_completa = clean_str(row.get(col_pieza, ''))
        op = clean_str(row.get(col_op, ''))
        if not pieza_completa or pieza_completa == 'NAN': continue
        pieza_match = get_match_key(pieza_completa)
        
        limite_mant = pd.to_numeric(row.get(col_limite, 0), errors='coerce') or 20000
        limite_alerta = pd.to_numeric(row.get(col_alerta, 0), errors='coerce') or (limite_mant * 0.8)
        
        fecha_prev, fecha_corr, fecha_abierto = pd.NaT, pd.NaT, pd.NaT
        tiene_abierto, tipo_abierto = False, ""
        
        if col_prev: fecha_prev = pd.to_datetime(row.get(col_prev), dayfirst=True, errors='coerce')
        if col_corr: fecha_corr = pd.to_datetime(row.get(col_corr), dayfirst=True, errors='coerce')

        if not df_mant.empty:
            match = df_mant[(df_mant['Pieza_Match'] == pieza_match) & (df_mant['OP'] == op)]
            
            term = match[match['Terminado'] == 'SI']
            max_fecha_cerrado = pd.NaT
            if not term.empty:
                max_fecha_cerrado = term['Fecha'].max()
                max_p = term[term['Tipo_Mant'] == 'PREV']['Fecha'].max()
                max_c = term[term['Tipo_Mant'] == 'CORR']['Fecha'].max()
                if pd.notna(max_p) and (pd.isna(fecha_prev) or max_p > fecha_prev): fecha_prev = max_p
                if pd.notna(max_c) and (pd.isna(fecha_corr) or max_c > fecha_corr): fecha_corr = max_c
                
            ab = match[match['Terminado'] == 'NO']
            if not ab.empty:
                max_fecha_abierto = ab['Fecha'].max()
                if pd.isna(max_fecha_cerrado) or max_fecha_abierto > max_fecha_cerrado:
                    tiene_abierto = True
                    fecha_abierto = max_fecha_abierto
                    tipo_abierto = ab.loc[ab['Fecha'].idxmax(), 'Tipo_Mant']

        fecha_base = pd.NaT
        if pd.notna(fecha_prev) and pd.notna(fecha_corr): fecha_base = max(fecha_prev, fecha_corr)
        elif pd.notna(fecha_prev): fecha_base = fecha_prev
        elif pd.notna(fecha_corr): fecha_base = fecha_corr

        prod_match = df_prod[df_prod['Pieza_Match'] == pieza_match]
        if pd.notna(fecha_base):
            prod_match = prod_match[prod_match['Fecha'] >= fecha_base]
        
        golpes_totales = int(prod_match['Golpes_Totales'].sum())
        
        color, estado = "VERDE", "OK"
        if golpes_totales >= limite_mant: color, estado = "ROJO", "MANT. REQUERIDO"
        elif golpes_totales >= limite_alerta: color, estado = "AMARILLO", "ALERTA PREVENTIVO"
            
        resultados.append({
            'CLIENTE': clean_str(row.get(col_cliente, '-')), 'PIEZA': pieza_completa, 'OP': op,
            'TIPO': clean_str(row.get(col_tipo, '-')), 'ULT_PREV': fecha_prev.strftime('%d/%m/%y') if pd.notna(fecha_prev) else "-",
            'ULT_CORR': fecha_corr.strftime('%d/%m/%y') if pd.notna(fecha_corr) else "-",
            'GOLPES': golpes_totales, 'LIMITE': int(limite_mant), 'ESTADO': estado, 'COLOR': color
        })
        if tiene_abierto:
            abiertos.append({'CLIENTE': clean_str(row.get(col_cliente, '-')), 'PIEZA': pieza_completa, 'OP': op,
                             'TIPO': clean_str(row.get(col_tipo, '-')), 'TIPO_MANT_ABIERTO': tipo_abierto, 'FECHA_APERTURA': fecha_abierto.strftime('%d/%m/%Y')})
            
    return pd.DataFrame(resultados), pd.DataFrame(abiertos)

# ==========================================
# 5. GENERACIÓN DEL PDF (FPDF)
# ==========================================
class PDFGolpes(FPDF):
    def header(self):
        self.set_font("Arial", 'B', 15)
        self.set_text_color(31, 73, 125)
        self.cell(0, 10, "Control de Golpes de Matrices (Mantenimiento)", border=0, ln=True, align='C')
        self.set_font("Arial", 'I', 9)
        self.set_text_color(100, 100, 100)
        hora_arg = datetime.utcnow() - timedelta(hours=3)
        self.cell(0, 5, f"Calculo generado el: {hora_arg.strftime('%d/%m/%Y %H:%M')}", border=0, ln=True, align='C')
        self.ln(3)
    def footer(self):
        self.set_y(-15)
        self.set_font("Arial", "I", 8)
        self.cell(0, 10, f"Pagina {self.page_no()}", 0, 0, "C")

def build_pdf_golpes(df_resultados, df_abiertos):
    pdf = PDFGolpes(orientation='L', unit='mm', format='A4')
    pdf.add_page()
    pdf.set_auto_page_break(auto=True, margin=15)
    
    pdf.set_font("Arial", 'B', 9)
    pdf.set_fill_color(31, 73, 125)
    pdf.set_text_color(255, 255, 255)
    pdf.cell(15, 8, "Cliente", 1, 0, 'C', fill=True)
    pdf.cell(70, 8, "Codigo Pieza", 1, 0, 'C', fill=True)
    pdf.cell(12, 8, "OP", 1, 0, 'C', fill=True)
    pdf.cell(12, 8, "Tipo", 1, 0, 'C', fill=True)
    pdf.cell(22, 8, "Ult. Prev.", 1, 0, 'C', fill=True)
    pdf.cell(22, 8, "Ult. Corr.", 1, 0, 'C', fill=True)
    pdf.cell(26, 8, "Golpes Ac.", 1, 0, 'C', fill=True)
    pdf.cell(26, 8, "Limite M.", 1, 0, 'C', fill=True)
    pdf.cell(72, 8, "Estado / Accion", 1, 1, 'C', fill=True)
    
    pdf.set_font("Arial", '', 8)
    for _, row in df_resultados.iterrows():
        bg = (255, 180, 180) if row['COLOR'] == "ROJO" else (255, 240, 180) if row['COLOR'] == "AMARILLO" else (198, 239, 206)
        txt = (180, 0, 0) if row['COLOR'] == "ROJO" else (150, 100, 0) if row['COLOR'] == "AMARILLO" else (0, 100, 0)
        pdf.set_text_color(0, 0, 0)
        pdf.cell(15, 7, str(row['CLIENTE']), 1, 0, 'C')
        pdf.cell(70, 7, str(row['PIEZA'])[:45], 1, 0, 'L')
        pdf.cell(12, 7, str(row['OP']), 1, 0, 'C')
        pdf.cell(12, 7, str(row['TIPO']), 1, 0, 'C')
        pdf.cell(22, 7, str(row['ULT_PREV']), 1, 0, 'C')
        pdf.cell(22, 7, str(row['ULT_CORR']), 1, 0, 'C')
        pdf.set_fill_color(*bg); pdf.set_text_color(*txt); pdf.set_font("Arial", 'B', 8)
        pdf.cell(26, 7, f"{row['GOLPES']:,}", 1, 0, 'C', fill=True)
        pdf.set_text_color(0, 0, 0); pdf.set_font("Arial", '', 8)
        pdf.cell(26, 7, f"{row['LIMITE']:,}", 1, 0, 'C')
        pdf.set_fill_color(*bg); pdf.set_text_color(*txt); pdf.set_font("Arial", 'B', 8)
        pdf.cell(72, 7, str(row['ESTADO']), 1, 1, 'C', fill=True)

    if not df_abiertos.empty:
        pdf.add_page()
        pdf.set_font("Arial", 'B', 12); pdf.set_text_color(192, 0, 0)
        pdf.cell(0, 8, "MANTENIMIENTOS ABIERTOS (Pendientes de Cierre)", ln=True)
        pdf.ln(3)
        pdf.set_font("Arial", 'B', 9); pdf.set_fill_color(192, 0, 0); pdf.set_text_color(255, 255, 255)
        pdf.cell(25, 8, "Cliente", 1, 0, 'C', fill=True)
        pdf.cell(90, 8, "Pieza", 1, 0, 'C', fill=True)
        pdf.cell(15, 8, "OP", 1, 0, 'C', fill=True)
        pdf.cell(35, 8, "Tipo Mant.", 1, 0, 'C', fill=True)
        pdf.cell(35, 8, "Fecha Apertura", 1, 1, 'C', fill=True)
        pdf.set_font("Arial", '', 8); pdf.set_text_color(0, 0, 0)
        for _, r in df_abiertos.iterrows():
            pdf.cell(25, 7, r['CLIENTE'], 1, 0, 'C')
            pdf.cell(90, 7, r['PIEZA'], 1, 0, 'L')
            pdf.cell(15, 7, r['OP'], 1, 0, 'C')
            pdf.cell(35, 7, r['TIPO_MANT_ABIERTO'], 1, 0, 'C')
            pdf.cell(35, 7, r['FECHA_APERTURA'], 1, 1, 'C')

    pdf.add_page()
    pdf.set_font("Arial", 'B', 14); pdf.set_text_color(31, 73, 125)
    pdf.cell(0, 10, "ESTADO GENERAL DEL MANTENIMIENTO PREVENTIVO", ln=True, align='C'); pdf.ln(5)
    
    resumen_data = []
    total_gen = len(df_resultados)
    total_ok = len(df_resultados[df_resultados['COLOR'] == 'VERDE'])
    total_nok = total_gen - total_ok
    
    for c in sorted([x for x in df_resultados['CLIENTE'].unique() if x != "-"]):
        df_c = df_resultados[df_resultados['CLIENTE'] == c]
        tot = len(df_c)
        ok = len(df_c[df_c['COLOR'] == 'VERDE'])
        nok = tot - ok
        resumen_data.append({
            'CLIENTE': c, 'TOT': tot, 'OK': ok, 'NOK': nok, 
            'POK': f"{int(round(ok/tot*100))}%" if tot>0 else "0%", 
            'PNOK': f"{int(round(nok/tot*100))}%" if tot>0 else "0%"
        })

    pdf.set_font("Arial", 'B', 10); pdf.set_fill_color(31, 73, 125); pdf.set_text_color(255, 255, 255)
    mx = 48.5; pdf.set_x(mx)
    pdf.cell(40, 8, "CLIENTE", 1, 0, 'C', fill=True)
    pdf.cell(30, 8, "TOTAL OP", 1, 0, 'C', fill=True)
    pdf.cell(35, 8, "CON PREVENTIVO", 1, 0, 'C', fill=True)
    pdf.cell(35, 8, "SIN MANTENIMIENTO", 1, 0, 'C', fill=True)
    pdf.cell(20, 8, "% PREV", 1, 0, 'C', fill=True)
    pdf.cell(30, 8, "% SIN MANT", 1, 1, 'C', fill=True)
    
    pdf.set_font("Arial", '', 10); pdf.set_text_color(0, 0, 0)
    for r in resumen_data:
        pdf.set_x(mx)
        pdf.cell(40, 8, r['CLIENTE'], 1, 0, 'C')
        pdf.cell(30, 8, str(r['TOT']), 1, 0, 'C')
        pdf.cell(35, 8, str(r['OK']), 1, 0, 'C')
        pdf.cell(35, 8, str(r['NOK']), 1, 0, 'C')
        pdf.cell(20, 8, r['POK'], 1, 0, 'C')
        pdf.cell(30, 8, r['PNOK'], 1, 1, 'C')
        
    pdf.set_x(mx); pdf.set_font("Arial", 'B', 10); pdf.set_fill_color(220, 220, 220)
    pdf.cell(40, 8, "TOTAL", 1, 0, 'C', fill=True)
    pdf.cell(30, 8, str(total_gen), 1, 0, 'C', fill=True)
    pdf.cell(35, 8, str(total_ok), 1, 0, 'C', fill=True)
    pdf.cell(35, 8, str(total_nok), 1, 0, 'C', fill=True)
    pdf.cell(20, 8, f"{int(round(total_ok/total_gen*100))}%" if total_gen > 0 else "0%", 1, 0, 'C', fill=True)
    pdf.cell(30, 8, f"{int(round(total_nok/total_gen*100))}%" if total_gen > 0 else "0%", 1, 1, 'C', fill=True)
    
    df_chart = pd.DataFrame(resumen_data)
    if not df_chart.empty:
        fig = go.Figure()
        fig.add_trace(go.Bar(x=df_chart['CLIENTE'], y=df_chart['OK'], name='CON PREVENTIVO (OK)', marker_color='#2ca02c', text=df_chart['POK'], textposition='auto'))
        fig.add_trace(go.Bar(x=df_chart['CLIENTE'], y=df_chart['NOK'], name='SIN MANTENIMIENTO (ALERTA/REQ)', marker_color='#d62728', text=df_chart['PNOK'], textposition='auto'))
        fig.update_layout(title="Estado de Mantenimiento por Cliente", barmode='group', width=750, height=400, plot_bgcolor='rgba(0,0,0,0)', legend=dict(x=0.7, y=1.1))
        
        with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as tmp:
            fig.write_image(tmp.name, engine="kaleido")
            pdf.ln(10); pdf.image(tmp.name, x=61, w=175); os.remove(tmp.name)
    
    buf = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
    pdf.output(buf.name)
    b = open(buf.name, "rb").read()
    os.remove(buf.name)
    return b

# ==========================================
# 6. INTERFAZ DE STREAMLIT
# ==========================================

if st.button("🔄 Forzar Actualización de Datos (Borrar Caché)", use_container_width=True):
    st.cache_data.clear()
    st.rerun()

with st.spinner("Conectando y descargando bases de datos..."):
    try:
        df_cat_raw, df_prod_raw, df_mant_raw = load_all_data()
        datos_listos = True
    except Exception as e:
        st.error(f"Error critico: {e}")
        datos_listos = False

if datos_listos:
    st.success("Bases de datos sincronizadas exitosamente.")
    col1, col2 = st.columns([1, 1])
    with col1:
        st.info("Reporte oficial de control de golpes. Cruza catálogo activo con producción acumulada.")
    with col2:
        if st.button("Procesar y Generar PDF de Golpes", use_container_width=True, type="primary"):
            with st.spinner("Calculando estado de matrices..."):
                df_res, df_abiertos = procesar_estado_matrices(df_cat_raw, df_prod_raw, df_mant_raw)
                if df_res.empty: 
                    st.warning("No hay datos activos en el catálogo.")
                else:
                    rojos = len(df_res[df_res['COLOR']=='ROJO'])
                    amarillos = len(df_res[df_res['COLOR']=='AMARILLO'])
                    verdes = len(df_res[df_res['COLOR']=='VERDE'])
                    st.write(f"**Resumen:** 🔴 {rojos} Críticas | 🟡 {amarillos} Alerta | 🟢 {verdes} OK")
                    
                    pdf_data = build_pdf_golpes(df_res, df_abiertos)
                    h = datetime.utcnow() - timedelta(hours=3)
                    st.download_button(
                        label="📥 Descargar Reporte en PDF", 
                        data=pdf_data, 
                        file_name=f"Reporte_Golpes_{h.strftime('%d%m%Y')}.pdf", 
                        mime="application/pdf", 
                        use_container_width=True
                    )
