## Both Front and Back End Machines
- Set up GitHub SSH keys
- Clone `scripts` and `twitch-chat-ingestor` repositories:
    - `git clone git@github.com:janovak/scripts.git`
    - `git clone git@github.com:janovak/twitch-chat-ingestor.git`

## Front End
- Restart services:
    ```bash
    sudo restart_service.py streamer-summaries
    sudo restart_daemon database_facade
    sudo systemctl reload nginx
    sudo systemctl daemon-reload
    ```

## Back End
- Install Python 3.10 (dependency for Bloom Filter fails on 3.12):
    ```bash
    sudo apt install software-properties-common -y
    sudo add-apt-repository ppa:deadsnakes/ppa
    sudo apt update
    sudo apt install python3.10 python3.10-venv python3.10-dev
    pip3.10 install -r requirements.txt
    python3.10 -m venv env
    ```

- Update sudoers file via `vimudo` to include `scripts` repo:
    - Update `.profile` to include `scripts` repo in PATH:
    ```bash
    # set PATH so it includes scripts repo if it exists
    if [ -d "$HOME/repos/scripts" ] ; then
        PATH="$HOME/repos/scripts:$PATH"
    fi
    source ~/.profile
    ```

- Copy secrets:
    ```bash
    scp -r -i ~/Downloads/ssh-key.key ~/Downloads/secrets ubuntu@<destination_ip>:/home/ubuntu/repos/twitch-chat-ingestor
    ```

- Set up Kafka server:
  - Download Kafka:
    ```bash
    wget https://downloads.apache.org/kafka/3.8.0/kafka_2.13-3.8.0.tgz
    ```
  - Configure the server:
    ```bash
    KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
    bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties
    bin/kafka-server-start.sh config/kraft/server.properties
    ```
  - **Note:** Consider making changes for underpowered machines:
    - Lower the memory usage by modifying `KAFKA_HEAP_OPTS` in `kafka-server-start.sh`.
    - Decrease disk space usage by modifying `log.retention.bytes` in `server.properties`.

- Enable and start services services:
     ```bash
    sudo systemctl start database_facade
    sudo systemctl enable database_facade
    sudo systemctl start prometheus
    sudo systemctl enable prometheus
     ```

- Configure Redis to publish expiration notifications:
    Run
    ```bash
    CONFIG SET notify-keyspace-events Ex
    ```
    in Redis CLI (can be found in Redis Insight).

## gRPC Command for Python Code Generation
    ```bash
    python3 -m grpc_tools.protoc -Igen/grpc/chat_database=protos --python_out=. --pyi_out=. --grpc_python_out=. protos/chat_database.proto
    ```
