FROM freqtradeorg/freqtrade:2024.12

WORKDIR /freqtrade

# Copy requirements & install dependencies
COPY requirements.txt /freqtrade/
RUN pip install --no-cache-dir -r requirements.txt

# Copy configuration and strategy files
COPY config.json /freqtrade/config.json
COPY user_data/strategies/ /freqtrade/strategies/
COPY user_data/ /freqtrade/user_data/

# Mountable volume for outbox.db, tradesv3.dryrun.sqlite, and downloaded data
EXPOSE 8080

VOLUME ["/freqtrade/user_data"]

CMD ["trade", "--config", "/freqtrade/config.json", "--strategy-path", "/freqtrade/strategies", "--strategy", "WowsStrategy"]
