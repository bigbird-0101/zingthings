FROM alpine:3.16
RUN apk add --update ca-certificates
EXPOSE 8776
EXPOSE 8887
EXPOSE 8787
RUN mkdir -p /etc/etcd/ssl
COPY protocol-container /
COPY ssl /etc/etcd/ssl
ENTRYPOINT ["/protocol-container"]