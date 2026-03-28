import logging

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask

from app_config import AppConfig
from cdm_loader.cdm_message_processor_job import CdmMessageProcessor
from cdm_loader.repository import CdmRepository


app = Flask(__name__)


@app.get('/health')
def hello_world():
    return 'healthy'


if __name__ == '__main__':
    app.logger.setLevel(logging.DEBUG)

    config = AppConfig()

    consumer = config.kafka_consumer()
    pg = config.pg_warehouse_db()
    cdm_repository = CdmRepository(pg)

    proc = CdmMessageProcessor(
        logger=app.logger,
        consumer=consumer,
        cdm_repository=cdm_repository,
        batch_size=100
    )

    scheduler = BackgroundScheduler()
    scheduler.add_job(func=proc.run, trigger="interval", seconds=25)
    scheduler.start()

    app.run(debug=True, host='0.0.0.0', use_reloader=False)
