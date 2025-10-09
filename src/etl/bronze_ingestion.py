# Databricks notebook source
!pip install faker

# COMMAND ----------

from faker import Faker
import pandas as pd
import random
import numpy as np
from datetime import datetime

fake = Faker()

def generate_fake_sales(n=50, inject_defects=True):
    segments = ['Government', 'Midmarket', 'Channel Partners', 'Enterprise', 'Small Business']
    countries = ['USA', 'Canada', 'Germany', 'France', 'Brazil', 'Australia',
                 'Spain', 'Italy', 'United Kingdom', 'Mexico', 'Japan', 'India', 'South Africa']
    products = ['Carretera', 'VTT', 'Amarilla', 'Montana', 'Paseo', 'Rosa', 'Negra',
                'Ciudad', 'Elite', 'Rapida', 'Velocidad', 'Turbo', 'Aventura']
    discount_bands = ['None', 'Low', 'Medium', 'High']

    data = []

    today = datetime.today()
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


df = generate_fake_sales(random.randint(75, 500), inject_defects=True)
display(df)


# COMMAND ----------

df.to_csv(f"/Volumes/main/financial/lakehouse/csv/sales-data-{datetime.today().strftime('%Y-%m-%d')}.csv", index=False)