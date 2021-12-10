FROM postgres:13

RUN apt-get update && \
    apt-get install -y libbz2-dev libc6-dev libffi-dev libgdbm-dev libncursesw5-dev \
    libsqlite3-dev libssl-dev tk-dev zlib1g-dev build-essential curl libncurses5-dev \
    libnss3-dev libreadline-dev python3-pip

RUN apt-get install -y postgresql-contrib postgresql-plpython3-13
