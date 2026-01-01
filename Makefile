.PHONY: sql
sql:
	python -m streamlit run apps/sql_app.py

.PHONY: pandas
pandas:
	python -m streamlit run apps/pandas_app.py