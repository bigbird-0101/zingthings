FROM alpine:3.16
RUN apk add --update ca-certificates
EXPOSE 8775
COPY deploy /
RUN mkdir -p /etc/etcd/ssl
COPY ssl /etc/etcd/ssl
ENTRYPOINT ["/deploy"]