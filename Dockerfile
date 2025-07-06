FROM python:3.12-slim
WORKDIR /app
COPY dashboard_app.py .
RUN pip install streamlit pandas matplotlib
EXPOSE 8501
CMD ["streamlit", "run", "dashboard_app.py", "--server.port=8501", "--server.address=0.0.0.0"]
