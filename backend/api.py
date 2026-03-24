from fastapi import FastAPI, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from deep_translator import GoogleTranslator
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
        for i, row in enumerate(g.itertuples(index=False)):
            dados[f"asin_{i+1}"] = row.asin
            dados[f"title_{i+1}"] = row.title
        return pd.Series(dados)

    return produtos_df.groupby("tracking_id").apply(agrupar).reset_index()


def traduzir_titulos(resultado, translator):
    """
    Traduz todas as colunas que começam com 'title_'
    e cria colunas 'title_X_en' ao lado de cada uma.
    """
    title_cols = [c for c in resultado.columns if re.fullmatch(r"title_\d+", c)]

    # Coleta todos os títulos únicos de todas as colunas
    todos_titulos = set()
    for col in title_cols:
        resultado[col] = resultado[col].fillna("").astype(str)
        todos_titulos.update(resultado[col].unique())

    # Traduz de uma vez
    traduzidos = {}
    for t in todos_titulos:
        if t.strip() == "":
            traduzidos[t] = ""
            continue
        try:
            traduzidos[t] = translator.translate(t)
        except:
            traduzidos[t] = t

    # Insere a coluna _en logo após cada coluna de título original
    colunas_finais = []
    for col in resultado.columns:
        colunas_finais.append(col)
        if col in title_cols:
            en_col = col + "_en"
            resultado[en_col] = resultado[col].map(traduzidos)
            colunas_finais.append(en_col)

    return resultado[colunas_finais]


def montar_resultado(produtos_df, rotas_df, tbrs):
    """
    Lógica compartilhada entre /processar e /preview.
    Retorna o DataFrame final já ordenado.
    """
    # Pivota ASINs múltiplos em colunas separadas
    produtos_pivot = pivotar_asins(produtos_df)

    # Prepara rotas — seller é o mesmo para o TBR inteiro
    rotas_df["seller_name"] = rotas_df["enrichedLegInfo"].apply(extrair_vendedor)
    rotas_df["seller_type"] = rotas_df["seller_name"].apply(classificar_seller)
    rotas_df = rotas_df.drop_duplicates(subset="trackingId")

    # Merge principal
    resultado = produtos_pivot.merge(
        rotas_df[["trackingId", "seller_name", "seller_type"]],
        left_on="tracking_id",
        right_on="trackingId",
        how="left"
    ).drop(columns=["trackingId"])

    # Ordena pela lista de TBRs fornecida
    tbr_lista = re.findall(r"TBR\d+", tbrs)
    if tbr_lista:
        ordem = pd.DataFrame({
            "tracking_id": tbr_lista,
            "ordem": range(len(tbr_lista))
        })
        resultado = resultado.merge(ordem, on="tracking_id", how="left")
        resultado = resultado.sort_values("ordem").drop(columns=["ordem"])

    return resultado


@app.post("/processar")
async def processar(
    produtos: UploadFile = File(...),
    rotas: UploadFile = File(...),
    tbrs: str = Form(...)
):
    def ler_csv(file):
        conteudo = file.read()
        for enc in ["utf-8", "latin-1", "cp1252"]:
            try:
                return pd.read_csv(BytesIO(conteudo), sep=";", encoding=enc)
            except (UnicodeDecodeError, Exception):
                continue
        raise ValueError("Não foi possível ler o arquivo CSV")

    produtos_df = ler_csv(produtos.file)
    rotas_df = ler_csv(rotas.file)

    produtos_df = produtos_df[["tracking_id", "asin", "title"]]
    rotas_df = rotas_df[["trackingId", "enrichedLegInfo"]]

    resultado = montar_resultado(produtos_df, rotas_df, tbrs)

    # Traduz todos os títulos
    translator = GoogleTranslator(source="pt", target="en")
    resultado = traduzir_titulos(resultado, translator)

    # Separa por tipo de seller
    easy_ship = resultado[resultado["seller_type"] == "Easy Ship"].drop(columns=["seller_type"])
    seller_flex = resultado[resultado["seller_type"] == "Seller Flex"].drop(columns=["seller_type"])

    # Renomeia colunas para exibição
    def renomear(df):
        rename_map = {"tracking_id": "TBR", "seller_name": "Seller"}
        for col in df.columns:
            if re.fullmatch(r"asin_\d+", col):
                rename_map[col] = col.upper().replace("_", " ")        # ASIN 1, ASIN 2...
            elif re.fullmatch(r"title_\d+", col):
                n = col.split("_")[1]
                rename_map[col] = f"Título {n}"
            elif re.fullmatch(r"title_\d+_en", col):
                n = col.split("_")[1]
                rename_map[col] = f"Title {n} (EN)"
        return df.rename(columns=rename_map)

    easy_ship = renomear(easy_ship)
    seller_flex = renomear(seller_flex)

    # Gera Excel com duas abas
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
        def ler_csv(file):
            conteudo = file.read()
            for enc in ["utf-8", "latin-1", "cp1252"]:
                try:
                    return pd.read_csv(BytesIO(conteudo), sep=";", encoding=enc)
                except (UnicodeDecodeError, Exception):
                    continue
            raise ValueError("Não foi possível ler o arquivo CSV")
    
    produtos_df = ler_csv(produtos.file)
    rotas_df = ler_csv(rotas.file)

    produtos_df = produtos_df[["tracking_id", "asin", "title"]]
    rotas_df = rotas_df[["trackingId", "enrichedLegInfo"]]

    resultado = montar_resultado(produtos_df, rotas_df, tbrs)

    # Sem tradução no preview — mais rápido
    # Separa as duas tabelas
    easy_ship = resultado[resultado["seller_type"] == "Easy Ship"].drop(columns=["seller_type"]).head(10)
    seller_flex = resultado[resultado["seller_type"] == "Seller Flex"].drop(columns=["seller_type"]).head(10)

    headers = list(resultado.drop(columns=["seller_type"]).columns)

    return {
        "headers": headers,
        "easy_ship": easy_ship.fillna("—").values.tolist(),
        "seller_flex": seller_flex.fillna("—").values.tolist(),
        "total_easy_ship": int((resultado["seller_type"] == "Easy Ship").sum()),
        "total_seller_flex": int((resultado["seller_type"] == "Seller Flex").sum()),
    }
