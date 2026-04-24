# ETL-пайплайн в Kubernetes для обработки потоковых данных

### Описание
Проект курса "Инженер данных" от Яндекс.Практикум.

### Задача
Развернуть в Kubernetes сервисы для обработки потоковых данных из Kafka, заполнения DDS-слоя хранилища по модели Data Vault и расчета витрин на CDM-слое.

### Архитектура пайплайна:
Схема пайплайна:
`\solution\схема-ETL.png`
Прим.: в репозитории представлен код для поднятия в Kubernetes двух сервисов, `DDS-Service` и `CDM-Service`.

1) **DDS-Service**: 
- `\solution\service_dds\src\app.py` запускает по расписанию обработку микробатча сообщений `dds_message_processor_job.py` из входящего топика Kafka, который наполняется данными из `STG-Service`:
	- `dds_repository.py` данные раскладываются в хранилище по сущностям Data Vault по логике upsert. 
	- `dds_output_message.py` формируется и отправляется выходное сообщение в исходящий топик для обработки следующим сервисом.

2) **CDM-Service**:
- `\solution\service_cdm\src\app.py` запускает по расписанию обработку микробатча сообщений `cdm_message_processor_job.py` из входящего топика Kafka, который наполняется `DDS-Service`:
	- `cdm_repository.py` обновляет витрины `cdm.user_product_counters` и `cdm.user_category_counters`. При этом идемпотентность расчета витрин обеспечивается проверкой айди заказов в сервисной таблице `cdm.processed_orders`.

3) **Релиз в Kubernetes**:
- собранные из докерфайлов образы размещаются в Container Registry в облаке Yandex Cloud.
- при помощи Helm-чартов сервисы запускаются в Kubernetes.


### Структура репозитория

Внутри `solution` расположены папки:
- `/solution/service_dds` необходимые файлы для запуска сервиса `DDS-Service`
- `/solution/service_cdm` необходимые файлы для запуска сервиса `CDM-Service`