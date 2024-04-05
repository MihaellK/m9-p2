# m9-p2
REpositório da segunda prova do modulo 09 de EC

## Rodar o kafka e o zookeeper
Execute o .yml com o docker compose
```
docker-compose up
```

em outros terminal execute o código em python que ira gerar os dados simulados e a conexão com o kafka
```
python3 kafka-python.py
```

Abaixo está um video do projeto sendo executado e as mensagens chegando. No video também é possivel ver que as mensagens anteriores após o desligamento do consumer permanecem.
Após rodar pela segunda o vez as mensagens param de chegar porque o limite de 50 mensagens foi atingido como é possivel ver no quanto inferior direito do consumero

Link do video
[Link do VIdeo](https://youtu.be/p54NHhLxx3g)

