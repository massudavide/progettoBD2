# bitcoin-analyzer

# installazione e primo avvio
1) "add-dependencies-for-IDEA" clean install
2) avviare docker-compose (tramite comando "docker-compose up" da terminale)
3) avviare "run-docker-images.sh"

# avviare applicazione 
1) eseguire docker-compose (tramite comando "docker-compose up" da terminale)
2) eseguire "start-all.sh"

# inviare dati da Flink ad elasticsearch
avviare classe "FlinkBitcoinMain"

# inviare dati da mongoDB ad elasticsearch
avviare classe "mongo2elastic"

# terminare applicazione
eseguire "stop-all.sh"
