import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import tempfile
import os
import matplotlib.pyplot as plt
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
st.write("<p style='text-align: center;'>Cruce automático de Catálogo, Producción (SQL Server) y Mantenimiento.</p>", unsafe_allow_html=True)
st.divider()

# ==========================================
# 2. ENLACES DE GOOGLE SHEETS
# ==========================================
URL_CATALOGO = "https://docs.google.com/spreadsheets/d/1feaeFLl2UslCsO4mzldUVFuhY1bdnUiQPatRM2m0sW0/export?format=csv&gid=1862158700"
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
    v = str(val).replace('\n', ' ').replace('\r', '').strip().upper()
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
        col_fecha = next((i for i, c in enumerate(cols) if 'FECHA' in c or 'MARCA TEMPORAL' in c), None)
        if col_fecha is None: return pd.DataFrame()

        terminos_clave = ['TERMINO', 'PREVENTIVO?', 'TERMINADO', 'CORRECTIVO']
        cols_terminado_idx = [i for i, c in enumerate(cols) if any(t in c for t in terminos_clave)]
        
        registros = []
        for _, row in df.iterrows():
            fecha = pd.to_datetime(row.iloc[col_fecha], dayfirst=True, errors='coerce')
            if pd.isna(fecha): continue
            
            estado_terminado = 'NO'
            if not cols_terminado_idx: 
                 estado_terminado = 'SI'
            else:
                for idx in cols_terminado_idx:
                    val = str(row.iloc[idx]).strip().upper()
                    if val in ['SI', 'SÍ']:
                        estado_terminado = 'SI'
                        break
            
            for i, col_name in enumerate(cols):
                base_col = col_name.split('.')[0].strip()
                if base_col in VALID_PIEZA_COLS:
                    pieza_completa = clean_str(row.iloc[i])
                    if pieza_completa and pieza_completa not in ['NAN', 'NONE', '-', '0', 'N/A', 'NO APLICA', '']:
                        pieza_match = get_match_key(pieza_completa)
                        op = ""
                        for j in range(i+1, min(i+4, len(cols))):
                            next_col = cols[j].split('.')[0].strip()
                            if any(x in next_col for x in ['OPERACION', 'OPERACIÓN', 'OP']):
                                op = clean_str(row.iloc[j])
                                break
                        registros.append({'Fecha': fecha, 'Pieza_Match': pieza_match, 'OP': op, 'Tipo_Mant': tipo_mant, 'Terminado': estado_terminado})
        return pd.DataFrame(registros)
    except Exception as e:
        st.error(f"❌ Error al extraer datos de {tipo_mant}: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_all_data():
    df_cat = pd.read_csv(URL_CATALOGO)
    df_cat.columns = df_cat.columns.astype(str).str.replace('\n', ' ').str.replace('\r', '').str.strip()
    col_activo = next((c for c in df_cat.columns if 'ACTIVO' in c.upper()), None)
    if col_activo:
        df_cat = df_cat[df_cat[col_activo].astype(str).str.strip().str.upper() == 'SI']
    
    QUERY_SQL = "SELECT Date as Fecha_Produccion, Code as Codigo_Pieza, COALESCE(Good, 0) as Buenas, COALESCE(Rework, 0) as Retrabajo FROM PROD_D_01" 
    try:
        conn = st.connection("wii_bi", type="sql")
        df_prod = conn.query(QUERY_SQL)
        df_prod['Fecha'] = pd.to_datetime(df_prod['Fecha_Produccion'], errors='coerce')
        df_prod['Golpes_Totales'] = pd.to_numeric(df_prod['Buenas'], errors='coerce').fillna(0) + pd.to_numeric(df_prod['Retrabajo'], errors='coerce').fillna(0)
        df_prod['Pieza_Match'] = df_prod['Codigo_Pieza'].apply(lambda x: get_match_key(clean_str(x))) 
    except Exception as e:
        st.error(f"❌ Error en SQL Server: {e}")
        df_prod = pd.DataFrame() 

    df_prev = extract_mantenimientos(URL_PREV_FAMMA, "PREV")
    df_corr = extract_mantenimientos(URL_CORR_FAMMA, "CORR")
    return df_cat, df_prod, pd.concat([df_prev, df_corr], ignore_index=True)

# ==========================================
# 4. MOTOR DE CRUCE
# ==========================================
def procesar_estado_matrices(df_cat, df_prod, df_mant):
    resultados, abiertos = [], []
    c_p = next((c for c in df_cat.columns if c.upper() == 'PIEZA'), 'PIEZA')
    c_o = next((c for c in df_cat.columns if c.upper() == 'OP'), 'OP')
    c_cli = next((c for c in df_cat.columns if 'CLIENTE' in c.upper()), 'CLIENTE')
    c_lim = next((c for c in df_cat.columns if 'GOLPES PARA MANTENIMIENTO' in c.upper()), 'GOLPES PARA MANTENIMIENTO')
    c_prv = next((c for c in df_cat.columns if 'ULTIMO PREVENTIVO' in c.upper()), 'ULTIMO PREVENTIVO')

    for _, row in df_cat.iterrows():
        p_comp = clean_str(row.get(c_p, ''))
        op = clean_str(row.get(c_o, ''))
        if not p_comp or p_comp == 'NAN': continue
        p_match = get_match_key(p_comp)
        lim = pd.to_numeric(row.get(c_lim, 0), errors='coerce') or 20000
        
        f_prev = pd.to_datetime(row.get(c_prv), dayfirst=True, errors='coerce')
        f_corr = pd.NaT
        tiene_ab, t_ab, f_ab = False, "", pd.NaT

        if not df_mant.empty:
            m = df_mant[(df_mant['Pieza_Match'] == p_match) & (df_mant['OP'] == op)]
            term = m[m['Terminado'] == 'SI']
            if not term.empty:
                f_p = term[term['Tipo_Mant'] == 'PREV']['Fecha'].max()
                f_c = term[term['Tipo_Mant'] == 'CORR']['Fecha'].max()
                if pd.notna(f_p) and (pd.isna(f_prev) or f_p > f_prev): f_prev = f_p
                if pd.notna(f_c): f_corr = f_c
            
            ab = m[m['Terminado'] == 'NO']
            if not ab.empty:
                f_ab_max = ab['Fecha'].max()
                tiene_ab, f_ab = True, f_ab_max
                t_ab = ab.loc[ab['Fecha'].idxmax(), 'Tipo_Mant']

        f_base = max(f_prev, f_corr) if pd.notna(f_prev) and pd.notna(f_corr) else (f_prev if pd.notna(f_prev) else f_corr)
        p_m = df_prod[df_prod['Pieza_Match'] == p_match]
        if pd.notna(f_base): p_m = p_m[p_m['Fecha'] >= f_base]
        
        golpes = int(p_m['Golpes_Totales'].sum())
        col, est = ("ROJO", "MANT. REQUERIDO") if golpes >= lim else (("AMARILLO", "ALERTA") if golpes >= (lim*0.8) else ("VERDE", "OK"))
        
        resultados.append({
            'CLIENTE': clean_str(row.get(c_cli, '-')), 'PIEZA': p_comp, 'OP': op,
            'ULT_PREV': f_prev.strftime('%d/%m/%y') if pd.notna(f_prev) else "-",
            'GOLPES': golpes, 'LIMITE': int(lim), 'ESTADO': est, 'COLOR': col
        })
        if tiene_ab:
            abiertos.append({'CLIENTE': clean_str(row.get(c_cli, '-')), 'PIEZA': p_comp, 'OP': op, 'TIPO_MANT_ABIERTO': t_ab, 'FECHA_APERTURA': f_ab.strftime('%d/%m/%Y')})
    return pd.DataFrame(resultados), pd.DataFrame(abiertos)

# ==========================================
# 5. GENERACIÓN PDF (FPDF + MATPLOTLIB)
# ==========================================
class BasePDF(FPDF):
    def footer(self):
        self.set_y(-15); self.set_font("Arial", "I", 8)
        self.cell(0, 10, f"Pagina {self.page_no()}", 0, 0, "C")

def build_pdf_main(df_res, df_ab):
    pdf = BasePDF(orientation='L', unit='mm', format='A4')
    pdf.add_page(); pdf.set_font("Arial", 'B', 14)
    pdf.cell(0, 10, "Detalle de Control de Golpes", ln=True, align='C')
    pdf.ln(5)
    
    # Tabla Detalle
    pdf.set_font("Arial", 'B', 8); pdf.set_fill_color(31, 73, 125); pdf.set_text_color(255, 255, 255)
    cols = [("Cliente", 20), ("Pieza", 80), ("OP", 15), ("Ult. Prev", 25), ("Golpes", 25), ("Limite", 25), ("Estado", 60)]
    for c, w in cols: pdf.cell(w, 8, c, 1, 0, 'C', True)
    pdf.ln()
    
    pdf.set_font("Arial", '', 8); pdf.set_text_color(0, 0, 0)
    for _, r in df_res.iterrows():
        bg = (255, 200, 200) if r['COLOR']=="ROJO" else ((255, 255, 200) if r['COLOR']=="AMARILLO" else (200, 255, 200))
        pdf.cell(20, 7, r['CLIENTE'], 1); pdf.cell(80, 7, r['PIEZA'][:45], 1)
        pdf.cell(15, 7, r['OP'], 1, 0, 'C'); pdf.cell(25, 7, r['ULT_PREV'], 1, 0, 'C')
        pdf.set_fill_color(*bg); pdf.cell(25, 7, f"{r['GOLPES']:,}", 1, 0, 'C', True)
        pdf.cell(25, 7, f"{r['LIMITE']:,}", 1, 0, 'C'); pdf.cell(60, 7, r['ESTADO'], 1, 1, 'C', True)

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf").name
    try:
        pdf.output(tmp); return open(tmp, "rb").read()
    finally:
        if os.path.exists(tmp): os.remove(tmp)

def build_pdf_resumen(df_res):
    pdf = BasePDF(orientation='L', unit='mm', format='A4')
    pdf.add_page(); pdf.set_font("Arial", 'B', 14)
    pdf.cell(0, 10, "Estado General del Mantenimiento", ln=True, align='C')
    
    # Procesar Resumen
    total_gen = len(df_res)
    total_nok = len(df_res[df_res['COLOR'] == 'ROJO'])
    total_ok = total_gen - total_nok
    
    resumen_data = []
    for c in sorted(df_res['CLIENTE'].unique()):
        df_c = df_res[df_res['CLIENTE'] == c]
        nok = len(df_c[df_c['COLOR'] == 'ROJO'])
        resumen_data.append({'CLIENTE': c, 'TOT': len(df_c), 'OK': len(df_c)-nok, 'NOK': nok})

    # Gráficos con Matplotlib
    plt.rcParams['figure.max_open_warning'] = 0
    y_pos = pdf.get_y() + 10
    
    # 1. Torta General
    fig1, ax1 = plt.subplots(figsize=(3,3))
    ax1.pie([total_ok, total_nok], labels=['OK/ALERTA', 'CRITICO'], colors=['#2ca02c', '#d62728'], autopct='%1.1f%%')
    ax1.set_title("Total Matrices")
    
    tmp1 = tempfile.NamedTemporaryFile(delete=False, suffix=".png").name
    try:
        fig1.savefig(tmp1, bbox_inches='tight', dpi=100)
        pdf.image(tmp1, x=15, y=y_pos, w=65)
    finally:
        plt.close(fig1)
        if os.path.exists(tmp1): os.remove(tmp1)

    # 2. Torta por Cliente
    n = len(resumen_data)
    fig2, axs = plt.subplots(1, n, figsize=(3*n, 3))
    if n == 1: axs = [axs]
    for i, r in enumerate(resumen_data):
        axs[i].pie([r['OK'], r['NOK']], colors=['#2ca02c', '#d62728'], autopct='%1.1f%%')
        axs[i].set_title(r['CLIENTE'])
    
    tmp2 = tempfile.NamedTemporaryFile(delete=False, suffix=".png").name
    try:
        fig2.savefig(tmp2, bbox_inches='tight', dpi=100)
        pdf.image(tmp2, x=90, y=y_pos, w=190)
    finally:
        plt.close(fig2)
        if os.path.exists(tmp2): os.remove(tmp2)

    tmp_pdf = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf").name
    try:
        pdf.output(tmp_pdf); return open(tmp_pdf, "rb").read()
    finally:
        if os.path.exists(tmp_pdf): os.remove(tmp_pdf)

# ==========================================
# 6. INTERFAZ
# ==========================================
if st.button("🔄 Actualizar Datos"):
    st.cache_data.clear(); st.rerun()

try:
    df_cat, df_prod, df_mant = load_all_data()
    if st.button("⚙️ Procesar Matrices", type="primary"):
        df_res, df_ab = procesar_estado_matrices(df_cat, df_prod, df_mant)
        st.session_state['res'], st.session_state['ab'] = df_res, df_ab

    if 'res' in st.session_state:
        res, ab = st.session_state['res'], st.session_state['ab']
        st.success(f"Analizadas {len(res)} matrices.")
        c1, c2 = st.columns(2)
        with c1: st.download_button("📥 Descargar Detalle", build_pdf_main(res, ab), "Detalle.pdf", "application/pdf", use_container_width=True)
        with c2: st.download_button("📊 Descargar Resumen", build_pdf_resumen(res), "Resumen.pdf", "application/pdf", use_container_width=True)
except Exception as e:
    st.error(f"Error: {e}")
