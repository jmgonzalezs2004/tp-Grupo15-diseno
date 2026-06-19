# Trabajo Práctico - Escalabilidad: Money Laundering Analysis

El presente trabajo simula un sistema que analiza el extracto de transacciones realizadas entre
cuentas bancarias en busca de anomalías. 

## Integrantes

| Nombre                           | GitHub                                                |
| -------------------------------- | ----------------------------------------------------- |
| Lucas Gustavo Soro               | [Blaz77](https://github.com/Blaz77)                   |
| Juan Manuel Gonzalez Segura      | [jmgonzalezs2004](https://github.com/jmgonzalezs2004) |
| Jesabel Melissa Soledad Pugliese | [jesapugliese](https://github.com/jesapugliese)       |

## Dataset

Se trabajó con datasets de IBM Transactions for Anti Money Laundering (AML), que se pueden encontrar en este [link](https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml/data).  
En particular, se extrajeron de forma aleatoria el 20% de los datos de los siguientes datasets:  
- `HI-Medium_Trans.csv`  
- `HI-Medium_accounts.csv`  

Los datasets a utilizar se guardaron en la carpeta `./datasets`, y se utilizaron los nombres, respectivamente:  
- `input_<client_id>.csv`  
- `accounts_<client_id>.csv`  

## Ejecución

`make up` : Inicia los contenedores del sistema y comienza a seguir los logs de todos ellos en un solo flujo de salida.

`make down`: Detiene los contenedores y libera los recursos asociados.

`make logs`: Sigue los logs de todos los contenedores en un solo flujo de salida.

`make generate-expected`: Genera los archivos de resultados esperados a partir de una ejecución serial de las queries.

`make test`: Inicia los contenedores del sistema, espera a que los clientes finalicen la ejecución de las queries, compara los resultados obtenidos con los resultados esperados y detiene los contenedores.

`make switch`: Permite alternar rápidamente entre los archivos de docker compose de distintos escenarios. Estos son:  
1. Un cliente, una sola réplica de cada elemento
2. Un cliente, con réplica escaladas
3. Múltiples clientes, con réplica escaladas

## Queries

Las queries que fueron resueltas son:  
1. Cuenta de origen, cuenta de destino y monto para transacciones USD menores a 50.
2. Nombre de banco, cuenta de origen y monto de la max. transacción USD de cada banco.
3. Cuenta de origen y monto de transacciones USD en el período [2022-09-06, 2022-09-15] con monto menor a 1 centésimo del promedio encontrado para el mismo formato de pago en el período [2022-09-01, 2022-09-05]
4. Cuentas que cumplan con el patrón scatter-gather con una sola cuenta de separación, para cuentas que hayan realizado transferencias en USD hacia 5 cuentas distintas dentro del período [2022-09-01, 2022-09-05]
5. Cantidad de transacciones del período [2022-09-01, 2022-09-05] con formato de pago "Wire" o "ACH" cuyo monto convertido a USD sea menor a 1

## Resultados

La salida correspondiente a los resultados de la resolución de las queries se encuentra en el directorio `./output`, siguiendo el formato `output_<client_id>_<query_number>.csv`.
