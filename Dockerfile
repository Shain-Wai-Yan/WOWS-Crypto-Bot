FROM freqtradeorg/freqtrade:2024.12

WORKDIR /freqtrade

# Copy requirements & install dependencies
COPY requirements.txt /freqtrade/
RUN pip install --no-cache-dir -r requirements.txt

# Copy strategy and configuration
COPY config.json /freqtrade/config.json
COPY user_data/ /freqtrade/user_data/

# Mountable volume for outbox.db and trades.sqlite
VOLUME ["/freqtrade/user_data"]

CMD ["trade", "--config", "/freqtrade/config.json", "--strategy", "WowsStrategy"]
