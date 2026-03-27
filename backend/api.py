from fastapi import FastAPI, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from deep_translator import GoogleTranslator
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
import pandas as pd
import re

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Cache global de traduções
_cache_traducoes: dict = {}


def ler_csv(file):
    """Lê CSV detectando encoding e separador automaticamente."""
    conteudo = file.read()
    for enc in ["utf-8", "latin-1", "cp1252"]:
        try:
            amostra = conteudo[:2048].decode(enc)
            sep = ";" if amostra.count(";") > amostra.count(",") else ","
            df = pd.read_csv(BytesIO(conteudo), sep=sep, encoding=enc)
            # Garante que todos os nomes de colunas são strings
            df.columns = df.columns.astype(str).str.strip()
            return df
        except (UnicodeDecodeError, Exception):
            continue
    raise ValueError("Não foi possível ler o arquivo CSV")


def extrair_vendedor(texto):
    if pd.isna(texto):
        return None
    match = re.search(r"\(([^,]+),", texto)
    if match:
        return match.group(1).strip()
    return None


def classificar_seller(nome):
    if pd.isna(nome):
        return None
    if re.fullmatch(r"[A-Z0-9]{4}", nome.strip()):
        return "Seller Flex"
    return "Easy Ship"


def pivotar_asins(produtos_df):
    """
    Agrupa os produtos por tracking_id.
    Se um TBR tiver múltiplos ASINs, cria colunas asin_1, asin_2, ...
    e title_1, title_2, ... para cada um.
    """
    def agrupar(g):
        dados = {}
        for i, (_, row) in enumerate(g.iterrows()):
            dados[f"asin_{i+1}"] = row["asin"]
            dados[f"title_{i+1}"] = row["title"]
        return pd.Series(dados)

    resultado = produtos_df.groupby("tracking_id", sort=False).apply(agrupar)
    resultado = resultado.reset_index()
    resultado.columns = resultado.columns.astype(str)
    return resultado


def traduzir_titulos(resultado, translator):
    """
    Traduz todas as colunas title_X em paralelo, usando cache global.
    """
    resultado.columns = resultado.columns.astype(str)

    title_cols = [c for c in resultado.columns if re.fullmatch(r"title_\d+", c)]

    todos_titulos = set()
    for col in title_cols:
        resultado[col] = resultado[col].fillna("").astype(str)
        todos_titulos.update(resultado[col].unique())

    novos = [t for t in todos_titulos if t.strip() != "" and t not in _cache_traducoes]

    def traduzir_um(texto):
        try:
            return texto, translator.translate(texto)
        except:
            return texto, texto

    if novos:
        with ThreadPoolExecutor(max_workers=10) as executor:
            for original, traduzido in executor.map(traduzir_um, novos):
                _cache_traducoes[original] = traduzido

    _cache_traducoes[""] = ""

    colunas_finais = []
    for col in resultado.columns:
        colunas_finais.append(col)
        if col in title_cols:
            en_col = col + "_en"
            resultado[en_col] = resultado[col].map(
                lambda t: _cache_traducoes.get(t, t)
            )
            colunas_finais.append(en_col)

    return resultado[colunas_finais]


def montar_resultado(produtos_df, rotas_df, tbrs):
    """
    Lógica compartilhada entre /processar e /preview.
    """
    produtos_df.columns = produtos_df.columns.astype(str).str.strip()
    rotas_df.columns = rotas_df.columns.astype(str).str.strip()

    produtos_df = produtos_df[["tracking_id", "asin", "title"]]
    rotas_df = rotas_df[["trackingId", "enrichedLegInfo"]]

    produtos_pivot = pivotar_asins(produtos_df)

    rotas_df["seller_name"] = rotas_df["enrichedLegInfo"].apply(extrair_vendedor)
    rotas_df["seller_type"] = rotas_df["seller_name"].apply(classificar_seller)
    rotas_df = rotas_df.drop_duplicates(subset="trackingId")

    resultado = produtos_pivot.merge(
        rotas_df[["trackingId", "seller_name", "seller_type"]],
        left_on="tracking_id",
        right_on="trackingId",
        how="left"
    ).drop(columns=["trackingId"])

    resultado.columns = resultado.columns.astype(str)

    tbr_lista = re.findall(r"TBR\d+", tbrs)
    if tbr_lista:
        ordem = pd.DataFrame({
            "tracking_id": tbr_lista,
            "ordem": range(len(tbr_lista))
        })
        resultado = resultado.merge(ordem, on="tracking_id", how="left")
        resultado = resultado.sort_values("ordem").drop(columns=["ordem"])

    return resultado


def renomear(df):
    """Renomeia colunas para exibição final."""
    df.columns = df.columns.astype(str)
    rename_map = {"tracking_id": "TBR", "seller_name": "Seller"}
    for col in df.columns:
        if re.fullmatch(r"asin_\d+", col):
            rename_map[col] = col.upper().replace("_", " ")
        elif re.fullmatch(r"title_\d+", col):
            n = col.split("_")[1]
            rename_map[col] = f"Título {n}"
        elif re.fullmatch(r"title_\d+_en", col):
            n = col.split("_")[1]
            rename_map[col] = f"Title {n} (EN)"
    return df.rename(columns=rename_map)


@app.post("/processar")
async def processar(
    produtos: UploadFile = File(...),
    rotas: UploadFile = File(...),
    tbrs: str = Form(...)
):
    produtos_df = ler_csv(produtos.file)
    rotas_df = ler_csv(rotas.file)

    resultado = montar_resultado(produtos_df, rotas_df, tbrs)

    translator = GoogleTranslator(source="pt", target="en")
    resultado = traduzir_titulos(resultado, translator)

    easy_ship = resultado[resultado["seller_type"] == "Easy Ship"].drop(columns=["seller_type"])
    seller_flex = resultado[resultado["seller_type"] == "Seller Flex"].drop(columns=["seller_type"])
    
    # Preenche colunas vazias com —
    easy_ship = easy_ship.fillna("—")
    seller_flex = seller_flex.fillna("—")
    
    easy_ship = renomear(easy_ship)
    seller_flex = renomear(seller_flex)

    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        easy_ship.to_excel(writer, index=False, sheet_name="Easy Ship")
        seller_flex.to_excel(writer, index=False, sheet_name="Seller Flex")
    output.seek(0)

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=resultado.xlsx"}
    )


@app.post("/preview")
async def preview(
    produtos: UploadFile = File(...),
    rotas: UploadFile = File(...),
    tbrs: str = Form(...)
):
    produtos_df = ler_csv(produtos.file)
    rotas_df = ler_csv(rotas.file)

    resultado = montar_resultado(produtos_df, rotas_df, tbrs)

    easy_ship = resultado[resultado["seller_type"] == "Easy Ship"].drop(columns=["seller_type"]).head(10)
    seller_flex = resultado[resultado["seller_type"] == "Seller Flex"].drop(columns=["seller_type"]).head(10)

    headers = list(resultado.drop(columns=["seller_type"]).columns.astype(str))

    return {
        "headers": headers,
        "easy_ship": easy_ship.fillna("—").values.tolist(),
        "seller_flex": seller_flex.fillna("—").values.tolist(),
        "total_easy_ship": int((resultado["seller_type"] == "Easy Ship").sum()),
        "total_seller_flex": int((resultado["seller_type"] == "Seller Flex").sum()),
    }
