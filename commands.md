1. Build base image:
    ```
    docker build -t dataintensive-base ./docker/base
    ```

1. Run everything with docker-compose:
    ```
    docker-compose up --build --force-recreate --detach --remove-orphans
    ```

1. Stop and remove everything:
    ```
    docker-compose down
    ```