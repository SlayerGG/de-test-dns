import time
import threading
import psycopg2

# csv файлы находятся в каталоге test_data 
# Результаты запросов заносятся в таблицы queryone-querysix

def execute_query(query, query_name):
	start_time = time.perf_counter()

	# Подключение к локальной базе данных PostgreSQL detest пользователя detest и паролем admin
	conn = psycopg2.connect("host=localhost dbname=detest user=detest password=admin")
	cur = conn.cursor()

	cur.execute(query)

	conn.commit()
	cur.close()
	conn.close()

	end_time = time.perf_counter()
	total_time = end_time - start_time

	print(f"Время выполнения запроса {query_name}: {total_time}")


start_time = time.perf_counter()

# Подключение к локальной базе данных PostgreSQL detest пользователя detest и паролем admin
conn = psycopg2.connect("host=localhost dbname=detest user=detest password=admin")
cur = conn.cursor()
# Создание и загрузка cities
cur.execute("""
	CREATE TABLE IF NOT EXISTS Cities(
		cities_id integer PRIMARY KEY,
		Ссылка uuid UNIQUE,
		Наименование text
		)""")
with open('test_data/t_cities.csv', 'r', encoding='utf-8') as file:
	cur.copy_expert("COPY Cities FROM stdin WITH csv HEADER", file)
cur.execute("""
	CREATE INDEX cities_ref_index ON cities (Ссылка)
	""")
conn.commit()

# Создание и загрузка branches
cur.execute("""
	CREATE TABLE IF NOT EXISTS Branches(
		branches_id integer PRIMARY KEY,
		Ссылка uuid UNIQUE,
		Наименование text,
		Город uuid REFERENCES Cities (Ссылка),
		КраткоеНаименование text,
		Регион text
		)""")
with open('test_data/t_branches.csv', 'r', encoding='utf-8') as file:
	cur.copy_expert("COPY Branches FROM stdin WITH csv HEADER", file)
cur.execute("""
	CREATE INDEX branches_ref_index ON branches (Ссылка)
	""")
cur.execute("""
	CREATE INDEX branches_cities_index on branches (Город)
	""")
conn.commit()

# Создание и загрузка products
cur.execute("""
	CREATE TABLE IF NOT EXISTS Products(
		products_id integer PRIMARY KEY,
		Ссылка uuid UNIQUE,
		Наименование text
		)""")
with open('test_data/t_products.csv', 'r', encoding='utf-8') as file:
	cur.copy_expert("COPY Products FROM stdin WITH csv HEADER", file)
cur.execute("""
	CREATE INDEX products_ref_index ON products (Ссылка)
	""")
conn.commit()

# Создание и загрузка sales
cur.execute("""
	CREATE TABLE IF NOT EXISTS Sales(
		sales_id integer PRIMARY KEY,
		Период timestamptz,
		Филиал uuid REFERENCES Branches (Ссылка),
		Номенклатура uuid REFERENCES Products (Ссылка),
		Количество numeric(1000, 1),
		Продажа numeric(1000, 2)
		)""")
with open('test_data/t_sales.csv', 'r', encoding='utf-8') as file:
	cur.copy_expert("COPY Sales FROM stdin WITH csv HEADER", file)
cur.execute("""
	CREATE INDEX sales_branches_index ON sales (Филиал)
	""")
cur.execute("""
	CREATE INDEX sales_products_index ON sales (Номенклатура)
	""")
conn.commit()

cur.close()
conn.close()

queries = [(""" 
	CREATE TABLE IF NOT EXISTS QueryOne AS
	WITH RankedProducts AS (
	    SELECT
	        branches.Наименование AS Наименование_магазина,
	        products.Наименование AS Наименование_продукта,
	        SUM(Количество) AS Сумма_товара,
	        ROW_NUMBER() OVER (PARTITION BY branches.Наименование ORDER BY SUM(Количество) DESC) AS rank
	    FROM
	        sales
	    INNER JOIN branches ON Филиал = branches.Ссылка
	    INNER JOIN products ON Номенклатура = products.Ссылка
	    GROUP BY
	        branches.Наименование,
	        products.Наименование
	)

	SELECT
	    Наименование_магазина,
	    Наименование_продукта,
	    Сумма_товара
	FROM
	    RankedProducts
	WHERE
	    rank <= 10
	ORDER BY
	    Наименование_магазина,
	    rank
	""", "1.1"),
 (("""
	CREATE TABLE IF NOT EXISTS QueryTwo AS
	WITH RankedProducts AS (
		SELECT 
			cities.Наименование AS Наименование_города,
			products.Наименование AS Наименование_продукта,
			SUM(Количество) AS Сумма_товара,
			ROW_NUMBER() OVER (PARTITION BY cities.Наименование ORDER BY SUM(Количество) DESC) as rank
		FROM
			sales
		INNER JOIN branches ON Филиал = branches.Ссылка
		INNER JOIN cities ON branches.Город = cities.Ссылка
		INNER JOIN products ON Номенклатура = products.Ссылка
		GROUP BY
			cities.Наименование,
			products.Наименование
	)

	SELECT 
		Наименование_города,
		Наименование_продукта,
		Сумма_товара
	FROM
		RankedProducts
	WHERE
		rank <= 10
	ORDER BY
		Наименование_города,
		rank
	"""), "1.2"),
(("""
	CREATE TABLE IF NOT EXISTS QueryThree AS
	SELECT 
		branches.Наименование,
		SUM(Количество) AS Сумма_товара,
		SUM(Продажа) AS Сумма_продаж
	FROM
		sales
	INNER JOIN branches ON Филиал = branches.Ссылка
	GROUP BY
		branches.Наименование
	ORDER BY 
		Сумма_продаж DESC
	LIMIT 10
	"""), "1.3"),
(("""
	CREATE TABLE IF NOT EXISTS QueryFour AS
	WITH SalesSummary AS(
	SELECT 
		products.Наименование AS Наименование_продукта,
		SUM(Количество) AS Сумма_товара,
		COUNT(DATE(Период)) AS sales_days
	FROM
		sales
	INNER JOIN products ON Номенклатура = products.Ссылка
	GROUP BY
		products.Наименование
	)

	SELECT 
		Наименование_продукта,
		Сумма_товара,
		AVG(Сумма_товара / sales_days) AS Средние_продажи_в_день
	FROM
		SalesSummary
	GROUP BY
		Наименование_продукта,
		Сумма_товара
	ORDER BY
		Средние_продажи_в_день DESC
	"""), "1.4"),
(("""
	CREATE TABLE IF NOT EXISTS QueryFive AS
	SELECT 
		branches.Наименование,
		SUM(Количество) AS Сумма_товара
	FROM
		sales
	INNER JOIN branches ON Филиал = branches.Ссылка
	INNER JOIN cities ON branches.Город = cities.Ссылка
	WHERE
		branches.Регион = 'Урал' AND
		cities.cities_id = 51 AND
		EXTRACT(MONTH FROM Период) = 1
	GROUP BY
		branches.Наименование
	ORDER BY
		Сумма_товара DESC
	LIMIT 2
	"""), "1.5"),
(("""
	CREATE TABLE IF NOT EXISTS QuerySix AS
	SELECT
		EXTRACT(DOW FROM Период) AS День_недели,
		EXTRACT(HOUR FROM Период) AS Час,
		SUM(Количество) AS Сумма_товара
	FROM
		sales
	GROUP BY
		Период,
		Час
	ORDER BY
		Сумма_товара DESC
	LIMIT 1
	"""), "1.6")]

threads = []
for query, name in queries:
	thread = threading.Thread(target=execute_query, args=(query, name))
	threads.append(thread)
	thread.start()

for thread in threads:
	thread.join()

for query, name in queries:
	execute_query(query, name)

end_time = time.perf_counter()
total_time = end_time - start_time

print(f"Время выполнения скрипта: {total_time}")
