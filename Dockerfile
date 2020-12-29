FROM ubuntu:18.04

ENV GOPATH /go
ENV GO111MODULE on
ENV PATH=/go/bin/:${PATH}

RUN apt-get -q update && \
    DEBIAN_FRONTEND=noninteractive apt-get -yq install \
        automake make \
        autoconf pkgconf \
        libtool \
        software-properties-common \
        git wget gawk vim jq \
        postgresql && \
    add-apt-repository ppa:longsleep/golang-backports && \
    apt-get -q update && \
    apt-get install -yq golang-go libzmq3-dev libargon2-0-dev && \
    apt-get -yqq install


COPY go.mod /tmp/updaterd/go.mod
WORKDIR /tmp/updaterd
RUN go get ./...

COPY . /tmp/updaterd
RUN go install ./...

ENV DATA_DIR=/.config/updaterd
ENV CONFIG_FILE=${DATA_DIR}/updaterd.conf
ENV PROJ_HOME=/go/src/github.com/bitmark-inc/go-programs/updaterd

COPY updaterd.conf.sample ${CONFIG_FILE}
COPY share/docker/run /run.sh
COPY share/docker/init /init
RUN sed -i -e "s#/var/lib/updaterd/#$DATA_DIR#" ${CONFIG_FILE} && updaterd -c ${CONFIG_FILE} generate-identity

CMD ["/run.sh"]
