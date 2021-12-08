FROM postgres:13

RUN apt-get update && \
    apt-get install -y libbz2-dev libc6-dev libffi-dev libgdbm-dev libncursesw5-dev \
    libsqlite3-dev libssl-dev tk-dev zlib1g-dev build-essential curl libncurses5-dev \
    libnss3-dev libreadline-dev python3-pip

# RUN curl -O https://www.python.org/ftp/python/3.8.12/Python-3.8.12.tar.xz
# RUN tar -xf Python-3.8.12.tar.xz
# RUN /Python-3.8.12/configure --enable-optimizations
# RUN make -j 6
# RUN make altinstall
RUN apt-get install -y postgresql-contrib postgresql-plpython3-13
