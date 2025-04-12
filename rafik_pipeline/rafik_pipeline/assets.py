# les bibliothèques que je vais utiliser 
import pytz
import yfinance as yf
import pandas as pd
from dagster import asset, AssetIn 
from datetime import datetime, timedelta
from fpdf import FPDF
import matplotlib.pyplot as plt
import os
import requests 
from dotenv import load_dotenv
import tempfile

load_dotenv()  # celui là je l'utilise pour récupérer ma clé API, je me méfie trop des autres je ne le mets pas sur git en public


# en ce qui concerne l'API
os.getenv("NEWS_API_KEY")
API_KEY = os.getenv("NEWS_API_KEY")

URL_API = "https://newsapi.org/v2/everything"



# la liste des assets à traiter, cette ligne je la ramène de chatgpt directement, le but c'est de l'utiliser pour faire le pipeline 
# le but ce n'est pas de chercher ou de connaître les 50 actifs eux mêmes 
ASSETS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA",
    "META", "NFLX", "NVDA", "INTC", "AMD",
    "BA", "JPM", "GS", "BAC", "WFC",
    "V", "MA", "PYPL", "AXP", "DIS",
    "KO", "PEP", "NKE", "MCD", "SBUX",
    "XOM", "CVX", "COP", "BP", "SLB",
    "T", "VZ", "TMUS", "ADBE", "CRM",
    "ORCL", "IBM", "QCOM", "TXN", "CSCO",
    "PFE", "MRK", "JNJ", "UNH", "ABT",
    "SPY", "QQQ", "DIA", "IWM", "GLD"
]

# je défini la première étape qui sert à récupérer le prix 
@asset
def getPrice() -> pd.DataFrame:
    try:
        # Télécharger toutes les colonnes
        data = yf.download(ASSETS, period="2d", group_by='ticker')
        
        # Liste des tickers valides (ceux qui ont des données)
        tickers_valides = [ticker for ticker in ASSETS if ticker in data]
        
        # Création du DataFrame de résultats
        resultat = []
        
        for ticker in tickers_valides:
            try:
                prix_fermeture = data[ticker]['Close'].iloc[-1]
                prix_fermeture_avant = data[ticker]['Close'].iloc[-2]
                resultat.append({
                    'ticker': ticker,
                    'price': prix_fermeture,
                    'previous_price': prix_fermeture_avant
                })
            except:
                continue
        
        return pd.DataFrame(resultat)
    
    except Exception as e:
        raise Exception(f"Erreur dans getPrice: {str(e)}")

@asset
def calculRJ(getPrice: pd.DataFrame) -> pd.DataFrame:
    try:
        if getPrice.empty:
            raise ValueError("Aucune donnée reçue de getPrice")
        
        df = getPrice.copy()
        df['return'] = (df['price'] - df['previous_price']) / df['previous_price'].replace(0, float('nan'))
        return df.dropna()
    except Exception as e:
        raise Exception(f"Erreur dans calculRJ: {str(e)}")


@asset
def getNews(context) -> dict:
    """Récupère les actualités pour chaque ticker dans ASSETS"""
    news_dict = {}
    today = datetime.now(pytz.UTC).date()
    yesterday = today - timedelta(days=1)
    
    for ticker in ASSETS:
        try:
            params = {
                "q": ticker,
                "from": yesterday.isoformat(),
                "to": today.isoformat(),
                "language": "en",
                "sortBy": "publishedAt",
                "pageSize": 3,  # On prend seulement 3 articles par ticker
                "apiKey": API_KEY,
            }
            
            response = requests.get(URL_API, params=params)
            data = response.json()
            
            if response.status_code != 200:
                context.log.warning(f"Erreur API pour {ticker}: {data.get('message')}")
                continue
                
            articles = data.get("articles", [])
            news_dict[ticker] = [
                {
                    "title": article["title"],
                    "publishedAt": datetime.strptime(
                        article["publishedAt"], "%Y-%m-%dT%H:%M:%SZ"
                    ).strftime("%d/%m %H:%M"),
                    "source": article["source"]["name"]
                }
                for article in articles
            ]
            
        except Exception as e:
            context.log.error(f"Erreur pour {ticker}: {str(e)}")
            news_dict[ticker] = []
    
    return news_dict
    

@asset
def generate_pdf(context, calculRJ: pd.DataFrame, getNews: dict):
    """Génère un PDF de rapport journalier en utilisant les polices standard"""
    try:
        pdf = FPDF()
        pdf.add_page()
        pdf.set_font("Arial", size=12)

        # Titre du rapport
        date_str = datetime.now().strftime("%d/%m/%Y")
        pdf.cell(200, 10, txt=f"Rapport Journalier - {date_str}", ln=True, align="C")
        pdf.ln(15)

        # Tableau des performances
        pdf.set_font("Arial", "B", 14)
        pdf.cell(200, 10, txt="Performances des Actifs", ln=True)
        pdf.ln(5)

        headers = ["Ticker", "Prix", "Précédent", "Rendement"]
        col_widths = [40, 40, 40, 40]

        pdf.set_font("Arial", "B", 12)
        for i, header in enumerate(headers):
            pdf.cell(col_widths[i], 10, header, border=1)
        pdf.ln()

        pdf.set_font("Arial", size=10)
        for _, row in calculRJ.iterrows():
            pdf.cell(col_widths[0], 10, row["ticker"], border=1)
            pdf.cell(col_widths[1], 10, f"{row['price']:.2f}", border=1)
            pdf.cell(col_widths[2], 10, f"{row['previous_price']:.2f}", border=1)
            pdf.cell(col_widths[3], 10, f"{row['return']:.2%}", border=1)
            pdf.ln()

        pdf.ln(15)

        # Graphique Top 5
        pdf.set_font("Arial", "B", 14)
        pdf.cell(200, 10, txt="Top 5 Performances", ln=True)
        pdf.ln(5)

        top5 = calculRJ.nlargest(5, "return")
        plt.figure(figsize=(8, 4))
        bars = plt.bar(top5["ticker"], top5["return"] * 100, color="#4CAF50")

        for bar in bars:
            height = bar.get_height()
            plt.text(
                bar.get_x() + bar.get_width() / 2.,
                height,
                f"{height:.2f}%",
                ha="center",
                va="bottom"
            )

        plt.title("Top 5 Rendements Journaliers")
        plt.ylabel("Rendement (%)")

        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmpfile:
            chart_path = tmpfile.name
            plt.savefig(chart_path, bbox_inches="tight", dpi=100)
            plt.close()

            pdf.image(chart_path, x=10, w=180)
            os.unlink(chart_path)

        pdf.ln(80)

        # Section actualités
        pdf.set_font("Arial", "B", 14)
        pdf.cell(200, 10, txt="Actualités Clés", ln=True)
        pdf.ln(5)

        pdf.set_font("Arial", size=10)
        for ticker in calculRJ["ticker"]:
            if ticker in getNews and getNews[ticker]:
                pdf.set_font("Arial", "B", 12)
                pdf.cell(200, 10, txt=f"{ticker}:", ln=True)
                pdf.set_font("Arial", size=10)

                for news in getNews[ticker][:3]:  # Max 3 actualités
                    raw_text = f"{news['publishedAt']} - {news['source']} - {news['title']}"
                    clean_news = (raw_text.replace("•", "-")
                                           .replace("'", "'")
                                           .replace('"', '"')
                                           .encode('ascii', 'ignore').decode('ascii'))
                    pdf.multi_cell(0, 8, clean_news)
                    pdf.ln(3)

        # Génération du fichier
        os.makedirs("pdf_reports", exist_ok=True)
        output_path = f"pdf_reports/market_recap_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
        pdf.output(output_path)

        context.log.info(f"PDF généré avec succès: {output_path}")

    except Exception as e:
        context.log.error(f"Erreur: {str(e)}")
        raise
