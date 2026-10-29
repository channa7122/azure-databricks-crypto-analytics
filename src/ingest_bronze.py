import os
import time
import requests
from dotenv import load_dotenv

# 1. Charger la clé API depuis le fichier .env
load_dotenv()
API_KEY = os.getenv("COINGECKO_API_KEY")

if not API_KEY:
    raise ValueError("Clé API introuvable ! Assurez-vous d'avoir créé le fichier .env avec COINGECKO_API_KEY=...")

# 2. Liste des 15 cryptos du projet
COINS = [
    "bitcoin", "ethereum", "solana", "cardano", "dogecoin",
    "polkadot", "ripple", "uniswap", "litecoin", "bitcoin-cash",
    "chainlink", "monero", "stellar", "cosmos", "near"
]

OUTPUT_DIR = os.path.join("data", "bronze")
os.makedirs(OUTPUT_DIR, exist_ok=True)

headers = {
    "accept": "application/json",
    "x-cg-demo-api-key": API_KEY
}

print("=" * 60)
print(f"🚀 Démarrage de l'ingestion Bronze pour {len(COINS)} cryptomonnaies...")
print("=" * 60)

for coin in COINS:
    url = f"https://api.coingecko.com/api/v3/coins/{coin}/market_chart?vs_currency=usd&days=365"
    print(f"📥 Téléchargement de l'historique 365j pour : {coin}...")
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        file_path = os.path.join(OUTPUT_DIR, f"{coin}.json")
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(response.text)
        print(f"✅ Sauvegardé avec succès dans {file_path}")
    else:
        print(f"❌ Erreur {response.status_code} pour {coin} : {response.text}")
    
    # Pause de 2 secondes pour respecter la limite de l'API Demo (30 requêtes/min)
    time.sleep(2.5)

print("\n🎉 Ingestion Bronze terminée avec succès dans data/bronze/ !")