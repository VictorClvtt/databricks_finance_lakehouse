# Databricks notebook source
!pip install faker

# COMMAND ----------

from faker import Faker
import pandas as pd
import random
import numpy as np
from datetime import datetime, timedelta
import os

fake = Faker()

# COMMAND ----------

def generate_fake_sales(n: int, inject_defects: bool = True) -> pd.DataFrame:
    segments = ['Government', 'Midmarket', 'Channel Partners', 'Enterprise', 'Small Business']
    countries = ['USA', 'Canada', 'Germany', 'France', 'Brazil', 'Australia',
                 'Spain', 'Italy', 'United Kingdom', 'Mexico', 'Japan', 'India', 'South Africa']
    products = ['Carretera', 'VTT', 'Amarilla', 'Montana', 'Paseo', 'Rosa', 'Negra',
                'Ciudad', 'Elite', 'Rapida', 'Velocidad', 'Turbo', 'Aventura']
    discount_bands = ['None', 'Low', 'Medium', 'High']

    today = datetime.today()

    data = []
    for _ in range(n):
        segment = random.choice(segments)
        country = random.choice(countries)
        product = random.choice(products)
        discount_band = random.choice(discount_bands)

        units_sold = random.randint(500, 3000)
        manufacturing_price = random.choice([3, 5, 7])
        sale_price = random.choice([10, 12, 15, 18, 20, 22, 25, 28, 30, 35, 55, 75, 100, 125])
        gross_sales = units_sold * sale_price
        discount = 0 if discount_band == 'None' else gross_sales * random.uniform(0.05, 0.20)
        sales = gross_sales - discount
        cogs = units_sold * manufacturing_price
        profit = sales - cogs

        date = today.strftime('%Y-%m-%d')
        month_number = today.month
        month_name = today.strftime('%B')
        year = today.year

        data.append({
            'Segment': segment,
            'Country': country,
            'Product': product,
            'Discount Band': discount_band,
            'Units Sold': units_sold,
            'Manufacturing Price': manufacturing_price,
            'Sale Price': sale_price,
            'Gross Sales': round(gross_sales, 2),
            'Discounts': round(discount, 2),
            'Sales': round(sales, 2),
            'COGS': round(cogs, 2),
            'Profit': round(profit, 2),
            'Date': date,
            'Month Number': month_number,
            'Month Name': month_name,
            'Year': year,
        })

    df = pd.DataFrame(data)

    if inject_defects:
        # 1️⃣ Valores ausentes aleatórios
        for col in ['Country', 'Product', 'Units Sold']:
            df.loc[df.sample(frac=0.05).index, col] = np.nan

        # 2️⃣ Pequenos typos em Country
        country_typos = {
            'Brazil': ['Brasil', 'BRAZIL'],
            'USA': ['U.S.A', 'United States'],
            'Germany': ['Deutschland'],
            'United Kingdom': ['UK'],
        }
        for idx in df.sample(frac=0.05).index:
            c = df.at[idx, 'Country']
            if pd.notna(c) and c in country_typos:
                df.at[idx, 'Country'] = random.choice(country_typos[c])

        # 3️⃣ Datas em formatos diferentes
        for idx in df.sample(frac=0.05).index:
            d = df.at[idx, 'Date']
            if pd.notna(d):
                df.at[idx, 'Date'] = datetime.strptime(d, '%Y-%m-%d').strftime('%d/%m/%Y')

    return df


df_sales = generate_fake_sales(random.randint(75, 500), inject_defects=True)
display(df_sales)

os.makedirs("/Volumes/main/financial/lakehouse/bronze/csv/sales", exist_ok=True)
df_sales.to_csv(f"/Volumes/main/financial/lakehouse/bronze/csv/sales/sales-data-{datetime.today().strftime('%Y-%m-%d')}.csv", index=False)


# COMMAND ----------

def generate_countries() -> pd.DataFrame:
    countries = [
        'USA', 'Canada', 'Germany', 'France', 'Brazil', 'Australia', 
        'Spain', 'Italy', 'United Kingdom', 'Mexico', 'Japan', 'India', 'South Africa'
    ]
    
    regions = {
        'USA': 'North America',
        'Canada': 'North America',
        'Mexico': 'North America',
        'Brazil': 'South America',
        'Germany': 'Europe',
        'France': 'Europe',
        'Spain': 'Europe',
        'Italy': 'Europe',
        'United Kingdom': 'Europe',
        'Japan': 'Asia',
        'India': 'Asia',
        'Australia': 'Oceania',
        'South Africa': 'Africa'
    }

    currencies = {
        'USA': 'USD', 'Canada': 'CAD', 'Mexico': 'MXN', 'Brazil': 'BRL',
        'Germany': 'EUR', 'France': 'EUR', 'Spain': 'EUR', 'Italy': 'EUR', 'United Kingdom': 'GBP',
        'Japan': 'JPY', 'India': 'INR', 'Australia': 'AUD', 'South Africa': 'ZAR'
    }

    languages = {
        'USA': 'English', 'Canada': 'English/French', 'Mexico': 'Spanish', 'Brazil': 'Portuguese',
        'Germany': 'German', 'France': 'French', 'Spain': 'Spanish', 'Italy': 'Italian', 'United Kingdom': 'English',
        'Japan': 'Japanese', 'India': 'Hindi/English', 'Australia': 'English', 'South Africa': 'English/Zulu/Afrikaans'
    }

    today = datetime.today()

    date = today.strftime('%Y-%m-%d')
    month_number = today.month
    month_name = today.strftime('%B')
    year = today.year

    data = []
    for c in countries:
        population = random.randint(5_000_000, 300_000_000)
        gdp = round(random.uniform(200, 20000), 2)
        region = regions[c]
        currency = currencies[c]
        language = languages[c]

        data.append({
            "Country": c,
            "Region": region,
            "Official Language": language,
            "Currency": currency,
            "Population": population,
            "GDP (Billion USD)": gdp,
            'Date': date,
            'Month Number': month_number,
            'Month Name': month_name,
            'Year': year,
        })

    return pd.DataFrame(data)

path_dir = "/Volumes/main/financial/lakehouse/bronze/csv/countries/"

def update_countries_if_needed():
    os.makedirs(path_dir, exist_ok=True)

    # Pega o arquivo mais recente (se existir)
    existing_files = sorted(
        [f for f in os.listdir(path_dir) if f.startswith("countries-data-") and f.endswith(".csv")]
    )

    if not existing_files:
        print("🔹 Nenhum arquivo encontrado — gerando dados de países pela primeira vez.")
        df = generate_countries()
        df["Last Updated"] = datetime.today().strftime("%Y-%m-%d")
        new_file = f"{path_dir}countries-data-{datetime.today().strftime('%Y-%m-%d')}.csv"
        df.to_csv(new_file, index=False)
        return df

    # Último arquivo salvo
    latest_file = os.path.join(path_dir, existing_files[-1])
    modified = datetime.fromtimestamp(os.path.getmtime(latest_file))

    if datetime.today() - modified > timedelta(days=180):
        print("🔄 Mais de 6 meses desde a última atualização — gerando novos dados.")
        df = generate_countries()
        df["Last Updated"] = datetime.today().strftime("%Y-%m-%d")
        new_file = f"{path_dir}countries-data-{datetime.today().strftime('%Y-%m-%d')}.csv"
        df.to_csv(new_file, index=False)
        return df
    else:
        print("✅ Dados de países ainda válidos — carregando existentes.")
        return pd.read_csv(latest_file)


df_countries = update_countries_if_needed()
display(df_countries)